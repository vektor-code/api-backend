package k8s

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ResolveApacheVersion returns the best-known httpd version from container spec fields,
// optionally probing a Ready pod with httpd -v when the version is still unknown.
func ResolveApacheVersion(ctx context.Context, kube kubernetes.Interface, config *rest.Config, namespace string, template *corev1.PodTemplateSpec) string {
	if template == nil {
		return ""
	}
	if v := resolveApacheVersionFromContainers(template.Spec.Containers); v != "" {
		return v
	}
	if config == nil || kube == nil || namespace == "" {
		return ""
	}
	return probeApacheVersion(ctx, config, kube, namespace, template)
}

func probeApacheVersion(ctx context.Context, config *rest.Config, kube kubernetes.Interface, namespace string, template *corev1.PodTemplateSpec) string {
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
			if !looksLikeApacheContainer(c) {
				continue
			}
			if v := execApacheVersion(ctx, config, kube, pod.Namespace, pod.Name, c.Name); v != "" {
				return v
			}
		}
	}
	return ""
}

func looksLikeApacheContainer(c corev1.Container) bool {
	if ParseApacheVersion(c.Image) != "" {
		return true
	}
	lower := strings.ToLower(c.Image + " " + c.Name)
	return strings.Contains(lower, "httpd") || strings.Contains(lower, "apache")
}

func execApacheVersion(ctx context.Context, config *rest.Config, kube kubernetes.Interface, namespace, podName, container string) string {
	for _, argv := range [][]string{
		{"httpd", "-v"},
		{"apachectl", "-v"},
		{"/bin/sh", "-c", "httpd -v 2>&1"},
		{"/bin/sh", "-c", "apachectl -v 2>&1"},
	} {
		out, err := podExec(ctx, config, kube, namespace, podName, container, argv)
		if err != nil {
			continue
		}
		if v := ParseApacheVersion(strings.TrimSpace(out)); v != "" {
			return v
		}
	}
	return ""
}
