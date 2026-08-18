package k8s

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// PodView is a snapshot used by the active investigator. It is not a full Pod.
type PodView struct {
	Name       string
	Namespace  string
	IP         string
	Node       string
	Phase      string
	Ready      bool
	Restarts   int
	Workload   string
	Container  string
	Containers []string
	Deleting   bool
}

// ServiceView is a ClusterIP service snapshot.
type ServiceView struct {
	Name      string
	Namespace string
	ClusterIP string
	Ports     []int32
}

// EndpointsView lists ready backend addresses for a Service.
type EndpointsView struct {
	Service  string
	Ready    []string
	NotReady []string
}

// ExecResult is stdout/stderr from a policy-controlled pod exec.
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Access is a Kubernetes API + optional exec handle for one cluster.
type Access struct {
	client kubernetes.Interface
	config *rest.Config
}

// NewAccess builds an Access from an existing client and rest config.
func NewAccess(client kubernetes.Interface, config *rest.Config) *Access {
	if client == nil {
		return nil
	}
	return &Access{client: client, config: config}
}

func (a *Access) GetPod(ctx context.Context, namespace, name string) (*PodView, error) {
	if a == nil || a.client == nil {
		return nil, fmt.Errorf("no kubernetes access")
	}
	p, err := a.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return podView(p), nil
}

func (a *Access) FindPodByIP(ctx context.Context, namespace, ip string) (*PodView, error) {
	if ip == "" {
		return nil, nil
	}
	pods, err := a.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP == ip {
			return podView(p), nil
		}
	}
	return nil, nil
}

func (a *Access) FindRunningPods(ctx context.Context, namespace, workload string) ([]*PodView, error) {
	pods, err := a.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []*PodView
	wl := strings.ToLower(strings.TrimSpace(workload))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
			continue
		}
		view := podView(p)
		if wl == "" || strings.EqualFold(view.Workload, workload) || strings.Contains(strings.ToLower(p.Name), wl) {
			out = append(out, view)
		}
	}
	if len(out) == 0 && wl != "" {
		// Fallback: any running pod in the namespace whose labels mention the workload.
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, v := range p.Labels {
				if strings.EqualFold(v, workload) {
					out = append(out, podView(p))
					break
				}
			}
		}
	}
	return out, nil
}

func (a *Access) FindServiceByIP(ctx context.Context, namespace, ip string) (*ServiceView, error) {
	if ip == "" {
		return nil, nil
	}
	svcs, err := a.client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range svcs.Items {
		s := &svcs.Items[i]
		if s.Spec.ClusterIP == ip {
			ports := make([]int32, 0, len(s.Spec.Ports))
			for _, p := range s.Spec.Ports {
				ports = append(ports, p.Port)
			}
			return &ServiceView{Name: s.Name, Namespace: s.Namespace, ClusterIP: s.Spec.ClusterIP, Ports: ports}, nil
		}
	}
	return nil, nil
}

func (a *Access) GetEndpoints(ctx context.Context, namespace, service string) (*EndpointsView, error) {
	ep, err := a.client.CoreV1().Endpoints(namespace).Get(ctx, service, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	view := &EndpointsView{Service: service}
	for _, sub := range ep.Subsets {
		for _, addr := range sub.Addresses {
			view.Ready = append(view.Ready, addr.IP)
		}
		for _, addr := range sub.NotReadyAddresses {
			view.NotReady = append(view.NotReady, addr.IP)
		}
	}
	return view, nil
}

func (a *Access) ListWarningEvents(ctx context.Context, namespace, pod string) ([]string, error) {
	ev, err := a.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + pod + ",type=Warning",
		Limit:         8,
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ev.Items))
	for _, item := range ev.Items {
		msg := strings.TrimSpace(item.Reason + ": " + item.Message)
		if msg != "" {
			out = append(out, msg)
		}
	}
	return out, nil
}

func (a *Access) FindDiagnosticPod(ctx context.Context, namespace string) (*PodView, error) {
	pods, err := a.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=crnet-diagnostics",
	})
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
			return podView(p), nil
		}
	}
	return nil, nil
}

// Exec runs argv in the named container. Command templates must be policy-controlled
// by the caller. This method does not create pods.
func (a *Access) Exec(ctx context.Context, namespace, pod, container string, argv []string) (*ExecResult, error) {
	if a == nil || a.client == nil || a.config == nil {
		return nil, fmt.Errorf("pod exec is not configured")
	}
	if len(argv) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	req := a.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   argv,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(a.config, http.MethodPost, req.URL())
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	streamCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		streamCtx, cancel = context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
	}
	err = executor.StreamWithContext(streamCtx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	res := &ExecResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		res.ExitCode = 1
		if strings.Contains(err.Error(), "exit code") {
			fmt.Sscanf(err.Error(), "%*s exit code %d", &res.ExitCode)
		}
		return res, err
	}
	return res, nil
}

func podView(p *corev1.Pod) *PodView {
	restarts := 0
	ready := false
	containers := make([]string, 0, len(p.Spec.Containers))
	appContainer := ""
	for _, c := range p.Spec.Containers {
		containers = append(containers, c.Name)
		if appContainer == "" && !isSidecarName(c.Name) {
			appContainer = c.Name
		}
	}
	if appContainer == "" && len(containers) > 0 {
		appContainer = containers[0]
	}
	for _, st := range p.Status.ContainerStatuses {
		restarts += int(st.RestartCount)
		if st.Name == appContainer {
			ready = st.Ready
		}
	}
	if !ready {
		for _, st := range p.Status.ContainerStatuses {
			if st.Ready && !isSidecarName(st.Name) {
				ready = true
				break
			}
		}
	}
	return &PodView{
		Name:       p.Name,
		Namespace:  p.Namespace,
		IP:         p.Status.PodIP,
		Node:       p.Spec.NodeName,
		Phase:      string(p.Status.Phase),
		Ready:      ready && p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil,
		Restarts:   restarts,
		Workload:   getPodApp(p),
		Container:  appContainer,
		Containers: containers,
		Deleting:   p.DeletionTimestamp != nil,
	}
}

func isSidecarName(name string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, "opentelemetry") ||
		strings.Contains(n, "istio-proxy") ||
		strings.Contains(n, "instrumentation") ||
		n == "dummy-backend"
}
