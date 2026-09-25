package k8s

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const nginxProbeTimeout = 8 * time.Second

// ResolveNginxVersion returns the best-known nginx version from container spec fields,
// optionally probing a Ready pod with nginx -v when the version is still unknown.
func ResolveNginxVersion(ctx context.Context, kube kubernetes.Interface, config *rest.Config, namespace string, template *corev1.PodTemplateSpec) string {
	if template == nil {
		return ""
	}
	if v := resolveNginxVersionFromContainers(template.Spec.Containers); v != "" {
		return v
	}
	if config == nil || kube == nil || namespace == "" {
		return ""
	}
	return probeNginxVersion(ctx, config, kube, namespace, template)
}

func probeNginxVersion(ctx context.Context, config *rest.Config, kube kubernetes.Interface, namespace string, template *corev1.PodTemplateSpec) string {
	if template == nil || len(template.Labels) == 0 {
		return ""
	}
	sel := labels.Set(template.Labels).AsSelector().String()
	pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return ""
	}
	for _, pod := range pods.Items {
		if !podIsReady(&pod) {
			continue
		}
		for _, c := range appContainers(pod.Spec.Containers) {
			if !looksLikeNginxContainer(c) {
				continue
			}
			if v := execNginxVersion(ctx, config, kube, namespace, pod.Name, c.Name); v != "" {
				return v
			}
		}
	}
	return ""
}

func looksLikeNginxContainer(c corev1.Container) bool {
	if ParseNginxVersion(c.Image) != "" {
		return true
	}
	lower := strings.ToLower(c.Image + " " + c.Name)
	return strings.Contains(lower, "nginx") || strings.Contains(lower, "openresty")
}

func execNginxVersion(ctx context.Context, config *rest.Config, kube kubernetes.Interface, namespace, podName, container string) string {
	for _, argv := range [][]string{
		{"nginx", "-v"},
		{"/bin/sh", "-c", "nginx -v 2>&1"},
	} {
		out, err := podExec(ctx, config, kube, namespace, podName, container, argv)
		if err != nil {
			continue
		}
		combined := strings.TrimSpace(out)
		if v := ParseNginxVersion(combined); v != "" {
			return v
		}
	}
	return ""
}

func podExec(ctx context.Context, config *rest.Config, kube kubernetes.Interface, namespace, podName, container string, argv []string) (string, error) {
	req := kube.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
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

	executor, err := remotecommand.NewSPDYExecutor(config, http.MethodPost, req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	execCtx, cancel := context.WithTimeout(ctx, nginxProbeTimeout)
	defer cancel()
	err = executor.StreamWithContext(execCtx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return "", err
	}
	if stdout.Len() > 0 {
		return stdout.String(), nil
	}
	return stderr.String(), nil
}
