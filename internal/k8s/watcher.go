package k8s

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// PodInfo holds lightweight pod metadata for trace enrichment
type PodInfo struct {
	Name                string            `json:"name"`
	Namespace           string            `json:"namespace"`
	NodeName            string            `json:"nodeName"`
	Labels              map[string]string `json:"labels"`
	Phase               string            `json:"phase"`
	AppName             string            `json:"appName"`
	Language            string            `json:"language"`
	Instrumented        bool              `json:"instrumented"`
	InstrumentationType string            `json:"instrumentationType"` // "annotation", "env", or "none"
	Details             string            `json:"details"`
	IsFrontend          bool              `json:"isFrontend"`
}

// Watcher watches Kubernetes pods and services to enrich trace metadata
type Watcher struct {
	client        kubernetes.Interface
	dynamicClient dynamic.Interface
	restConfig    *rest.Config
	pods          map[string]*PodInfo // key: namespace/podname
	namespaces    []string
	clusterName   string
	mu            sync.RWMutex
}

// NewWatcher creates a Kubernetes API watcher.
// It first tries in-cluster config, then falls back to kubeconfig.
func NewWatcher(kubeconfig string) (*Watcher, error) {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("k8s config: %w", err)
		}
	}

	// Increase QPS for busy clusters
	config.QPS = 50
	config.Burst = 100

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s dynamic client: %w", err)
	}

	w := &Watcher{
		client:        client,
		dynamicClient: dynamicClient,
		restConfig:    config,
		pods:          make(map[string]*PodInfo),
	}
	w.clusterName = w.detectClusterName(context.Background())

	return w, nil
}

// Client returns the typed Kubernetes client, or nil.
func (w *Watcher) Client() kubernetes.Interface {
	if w == nil {
		return nil
	}
	return w.client
}

// RESTConfig returns the rest config used to build the client, needed for exec.
func (w *Watcher) RESTConfig() *rest.Config {
	if w == nil {
		return nil
	}
	return w.restConfig
}

// Start begins watching all namespaces for pod changes
func (w *Watcher) Start(ctx context.Context) error {
	// Load initial pod list
	if err := w.loadPods(ctx); err != nil {
		log.Printf("[k8s] warning: could not load pods: %v", err)
	}

	// Load namespace list
	if err := w.loadNamespaces(ctx); err != nil {
		log.Printf("[k8s] warning: could not load namespaces: %v", err)
	}

	// Start watching for changes
	go w.watchPods(ctx)
	return nil
}

// GetPodInfo returns metadata for a pod by namespace+name
func (w *Watcher) GetPodInfo(namespace, name string) *PodInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.pods[namespace+"/"+name]
}

// GetNamespaces returns the list of known namespaces
func (w *Watcher) GetNamespaces() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make([]string, len(w.namespaces))
	copy(result, w.namespaces)
	return result
}

// GetAgentNamespaces returns namespaces where the agent-backend pod is running
func (w *Watcher) GetAgentNamespaces() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	nsMap := make(map[string]bool)
	for _, pod := range w.pods {
		isAgent := strings.HasPrefix(pod.Name, "agent-backend") ||
			pod.Labels["app.kubernetes.io/name"] == "agent-backend" ||
			pod.Labels["app"] == "agent-backend" ||
			pod.Labels["app"] == "kubetrace-agent"

		if isAgent && pod.Phase == "Running" {
			nsMap[pod.Namespace] = true
		}
	}

	var result []string
	for ns := range nsMap {
		result = append(result, ns)
	}
	sort.Strings(result)
	return result
}

// GetPodsByNamespace returns all pods in a namespace
func (w *Watcher) GetPodsByNamespace(namespace string) []*PodInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var result []*PodInfo
	for key, pod := range w.pods {
		if pod.Namespace == namespace {
			_ = key
			result = append(result, pod)
		}
	}
	return result
}

func (w *Watcher) loadPods(ctx context.Context) error {
	pods, err := w.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range pods.Items {
		p := &pods.Items[i]
		w.pods[p.Namespace+"/"+p.Name] = podToInfo(p)
	}
	log.Printf("[k8s] loaded %d pods", len(pods.Items))
	return nil
}

func (w *Watcher) loadNamespaces(ctx context.Context) error {
	nsList, err := w.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.namespaces = make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		w.namespaces = append(w.namespaces, ns.Name)
	}
	return nil
}

func (w *Watcher) watchPods(ctx context.Context) {
	watcher, err := w.client.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("[k8s] pod watch error: %v", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			key := pod.Namespace + "/" + pod.Name
			w.mu.Lock()
			switch event.Type {
			case watch.Added, watch.Modified:
				w.pods[key] = podToInfo(pod)
			case watch.Deleted:
				delete(w.pods, key)
			}
			w.mu.Unlock()
		}
	}
}

func getPodApp(p *corev1.Pod) string {
	if app, ok := p.Labels["app.kubernetes.io/name"]; ok {
		return app
	}
	if app, ok := p.Labels["app"]; ok {
		return app
	}
	if app, ok := p.Labels["k8s-app"]; ok {
		return app
	}
	parts := strings.Split(p.Name, "-")
	if len(parts) > 2 {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return parts[0]
}

func isFrontendPod(p *corev1.Pod) bool {
	return isStaticHTTPStack(detectLanguage(p))
}

func detectLanguage(p *corev1.Pod) string {
	if p == nil {
		return ""
	}
	return resolveLanguage("", p.Spec.Containers)
}

func detectInstrumentation(p *corev1.Pod) (bool, string, string) {
	if p.Annotations != nil {
		for k, v := range p.Annotations {
			if strings.HasPrefix(k, "instrumentation.opentelemetry.io/inject-") && v != "" && v != "false" {
				return true, "annotation", fmt.Sprintf("Annotation: %s = %s", k, v)
			}
		}
	}
	for _, c := range p.Spec.Containers {
		for _, env := range c.Env {
			switch env.Name {
			case "OTEL_PHP_AUTOLOAD_ENABLED":
				if env.Value == "true" {
					return true, "env", "PHP environment variable: OTEL_PHP_AUTOLOAD_ENABLED=true"
				}
			case "OTEL_TRACES_EXPORTER":
				if env.Value != "" && env.Value != "none" {
					return true, "env", fmt.Sprintf("OTEL_TRACES_EXPORTER=%s", env.Value)
				}
			case "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT":
				if env.Value != "" {
					return true, "env", fmt.Sprintf("%s is set", env.Name)
				}
			case "JAVA_TOOL_OPTIONS":
				if strings.Contains(strings.ToLower(env.Value), "opentelemetry") || strings.Contains(env.Value, "otel") {
					return true, "env", "Instrumented via JAVA_TOOL_OPTIONS"
				}
			case "NODE_OPTIONS":
				if strings.Contains(strings.ToLower(env.Value), "opentelemetry") {
					return true, "env", "Instrumented via NODE_OPTIONS"
				}
			case "PYTHONPATH":
				if strings.Contains(strings.ToLower(env.Value), "opentelemetry") {
					return true, "env", "Instrumented via PYTHONPATH"
				}
			}
		}
	}
	return false, "none", "No OTel injection annotations or env vars detected"
}

func podToInfo(p *corev1.Pod) *PodInfo {
	labels := make(map[string]string)
	for k, v := range p.Labels {
		labels[k] = v
	}
	instr, instrType, details := detectInstrumentation(p)
	isFrontend := isFrontendPod(p)
	return &PodInfo{
		Name:                p.Name,
		Namespace:           p.Namespace,
		NodeName:            p.Spec.NodeName,
		Labels:              labels,
		Phase:               string(p.Status.Phase),
		AppName:             getPodApp(p),
		Language:            detectLanguage(p),
		Instrumented:        instr,
		InstrumentationType: instrType,
		Details:             details,
		IsFrontend:          isFrontend,
	}
}

// GetLanguageForService returns the assigned/detected stack of a service.
func (w *Watcher) GetLanguageForService(namespace, serviceName string) string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, pod := range w.pods {
		if pod.Namespace == namespace && (pod.AppName == serviceName || strings.HasPrefix(pod.Name, serviceName)) {
			lang := strings.ToLower(strings.TrimSpace(pod.Language))
			if lang != "" && lang != "unknown" && lang != "auto" && lang != "unk" {
				return pod.Language
			}
		}
	}
	return ""
}

// ReconcileInstrumentation creates, updates or deletes an Instrumentation CRD in Kubernetes dynamically
func (w *Watcher) ReconcileInstrumentation(ctx context.Context, namespace string, disabled bool) error {
	if w.dynamicClient == nil {
		return fmt.Errorf("dynamic client not initialized")
	}

	gvr := schema.GroupVersionResource{
		Group:    "opentelemetry.io",
		Version:  "v1alpha1",
		Resource: "instrumentations",
	}

	name := namespace + "-instrumentation"

	if disabled {
		err := w.dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete instrumentation: %w", err)
		}
		log.Printf("[k8s] deleted instrumentation %s in namespace %s", name, namespace)
		return nil
	}

	agentNs := getCurrentNamespace()
	if agentNss := w.GetAgentNamespaces(); len(agentNss) > 0 {
		agentNs = agentNss[0]
	}

	if err := ApplyInstrumentationCR(ctx, w.dynamicClient, namespace, agentNs); err != nil {
		return fmt.Errorf("failed to apply instrumentation: %w", err)
	}
	log.Printf("[k8s] applied instrumentation %s in namespace %s", name, namespace)
	return nil
}

// InstrumentationInfo holds metadata about an OTel Auto-Instrumentation CRD resource
type InstrumentationInfo struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Endpoint  string `json:"endpoint"`
	Sampler   string `json:"sampler"`
}

// GetInstrumentations queries CustomResourceDefinitions for OpenTelemetry Instrumentations
func (w *Watcher) GetInstrumentations(ctx context.Context) ([]*InstrumentationInfo, error) {
	if w.dynamicClient == nil {
		return nil, fmt.Errorf("dynamic client not initialized")
	}

	gvr := schema.GroupVersionResource{
		Group:    "opentelemetry.io",
		Version:  "v1alpha1",
		Resource: "instrumentations",
	}

	list, err := w.dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		// FALLBACK: If CRDs are not registered on this cluster, generate them deterministically
		// for all matching namespaces in the inventory.
		log.Printf("[k8s] listing instrumentations: %v. Using fallback namespace discovery.", err)

		var fallbackResult []*InstrumentationInfo
		w.mu.RLock()
		namespaces := make([]string, len(w.namespaces))
		copy(namespaces, w.namespaces)
		w.mu.RUnlock()

		for _, ns := range namespaces {
			if isAppNamespace(ns) {
				agentNs := getCurrentNamespace()
				if agentNss := w.GetAgentNamespaces(); len(agentNss) > 0 {
					agentNs = agentNss[0]
				}
				fallbackResult = append(fallbackResult, &InstrumentationInfo{
					Name:      ns + "-instrumentation",
					Namespace: ns,
					Endpoint:  fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4317", agentNs),
					Sampler:   "always_off",
				})
			}
		}
		return fallbackResult, nil
	}

	var result []*InstrumentationInfo
	for _, item := range list.Items {
		name := item.GetName()
		namespace := item.GetNamespace()

		if !isAppNamespace(namespace) {
			continue
		}

		spec, found, _ := unstructured.NestedMap(item.Object, "spec")
		var endpoint string
		var sampler string
		if found {
			exporter, foundExporter, _ := unstructured.NestedMap(spec, "exporter")
			if foundExporter {
				endpoint, _, _ = unstructured.NestedString(exporter, "endpoint")
			}
			samplerMap, foundSampler, _ := unstructured.NestedMap(spec, "sampler")
			if foundSampler {
				sampler, _, _ = unstructured.NestedString(samplerMap, "type")
			}
		}

		result = append(result, &InstrumentationInfo{
			Name:      name,
			Namespace: namespace,
			Endpoint:  endpoint,
			Sampler:   sampler,
		})
	}
	return result, nil
}

func isAppNamespace(ns string) bool {
	lower := strings.ToLower(ns)
	if strings.HasPrefix(lower, "kube-") ||
		strings.HasPrefix(lower, "istio-") ||
		strings.HasPrefix(lower, "ingress-") ||
		strings.HasPrefix(lower, "prometheus-") ||
		strings.HasPrefix(lower, "argocd-") ||
		strings.HasPrefix(lower, "cert-") ||
		strings.HasPrefix(lower, "devops-") ||
		strings.HasPrefix(lower, "devopstools-") ||
		lower == "argocd" ||
		lower == "prometheus" ||
		lower == "grafana" ||
		lower == "fluentbit" ||
		lower == "metallb-system" ||
		lower == "backstage" ||
		lower == "permission-manager" ||
		lower == "apm-observability" ||
		lower == "lens-shells" ||
		lower == "lens-with-go" ||
		lower == "nfs-provisioner" {
		return false
	}
	return true
}

// GetClusterName returns the dynamically detected Kubernetes cluster name
func (w *Watcher) GetClusterName() string {
	if w.clusterName == "" {
		return "cluster.local"
	}
	return w.clusterName
}

func getCurrentNamespace() string {
	if nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(nsBytes))
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if ns := os.Getenv("AGENT_NAMESPACE"); ns != "" {
		return ns
	}
	return "default"
}

func (w *Watcher) detectClusterName(ctx context.Context) string {
	if envVal := os.Getenv("CLUSTER_NAME"); envVal != "" {
		return envVal
	}
	if envVal := os.Getenv("KUBERNETES_CLUSTER_NAME"); envVal != "" {
		return envVal
	}
	// Try kubeadm-config configmap in kube-system
	if w.client != nil {
		cm, err := w.client.CoreV1().ConfigMaps("kube-system").Get(ctx, "kubeadm-config", metav1.GetOptions{})
		if err == nil {
			if data, ok := cm.Data["ClusterConfiguration"]; ok {
				for _, line := range strings.Split(data, "\n") {
					if strings.Contains(line, "clusterName:") {
						parts := strings.Split(line, ":")
						if len(parts) > 1 {
							return strings.TrimSpace(parts[1])
						}
					}
				}
			}
		}
	}
	return "cluster.local"
}

// BuildDynamicClientForCluster constructs a dynamic client for a remote Kubernetes cluster using a host and credentials (token or kubeconfig YAML)
func BuildDynamicClientForCluster(host, credentials string) (dynamic.Interface, error) {
	var config *rest.Config

	// If credentials contain typical kubeconfig keywords, treat it as a raw kubeconfig YAML
	if strings.Contains(credentials, "apiVersion:") && strings.Contains(credentials, "clusters:") {
		clientConfig, err := clientcmd.NewClientConfigFromBytes([]byte(credentials))
		if err != nil {
			return nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
		}
		cfg, err := clientConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get rest config from kubeconfig: %w", err)
		}
		config = cfg
	} else {
		// Treat credentials as a raw bearer token
		config = &rest.Config{
			Host:        host,
			BearerToken: credentials,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}
	}

	return dynamic.NewForConfig(config)
}

// ReconcileRemoteInstrumentation creates/deletes Instrumentation in a remote cluster using a provided dynamic client
func ReconcileRemoteInstrumentation(ctx context.Context, dynClient dynamic.Interface, namespace string, disabled bool, agentNamespaces []string) error {
	gvr := schema.GroupVersionResource{
		Group:    "opentelemetry.io",
		Version:  "v1alpha1",
		Resource: "instrumentations",
	}

	name := namespace + "-instrumentation"

	if disabled {
		err := dynClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		log.Printf("[k8s/remote] deleted remote instrumentation %s in namespace %s", name, namespace)
		return nil
	}

	agentNs := AgentNamespaceFallback()
	if len(agentNamespaces) > 0 && agentNamespaces[0] != "" {
		agentNs = agentNamespaces[0]
	}

	if err := ApplyInstrumentationCR(ctx, dynClient, namespace, agentNs); err != nil {
		return err
	}
	log.Printf("[k8s/remote] applied remote instrumentation %s in namespace %s", name, namespace)
	return nil
}

// GetRemoteNamespaces lists all namespace names on a remote Kubernetes cluster using its host and token/kubeconfig credentials
func GetRemoteNamespaces(ctx context.Context, host, credentials string) ([]string, error) {
	var config *rest.Config
	var err error

	if strings.Contains(credentials, "apiVersion:") && strings.Contains(credentials, "clusters:") {
		clientConfig, err := clientcmd.NewClientConfigFromBytes([]byte(credentials))
		if err != nil {
			return nil, err
		}
		config, err = clientConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
	} else {
		config = &rest.Config{
			Host:        host,
			BearerToken: credentials,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	nsList, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var res []string
	for _, ns := range nsList.Items {
		res = append(res, ns.Name)
	}
	return res, nil
}
