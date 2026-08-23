package k8s

import (
	"context"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// WorkloadInfo describes a Kubernetes workload that can be instrumented.
type WorkloadInfo struct {
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	Kind         string            `json:"kind"`
	Replicas     int32             `json:"replicas"`
	Ready        int32             `json:"ready"`
	Language     string            `json:"language"`
	Instrumented bool              `json:"instrumented"`
	Labels       map[string]string `json:"labels"`
	Details      string            `json:"details"`
	IsFrontend   bool              `json:"isFrontend"`
}

// BuildRestConfig creates a rest.Config from a host and credentials (kubeconfig YAML or bearer token).
func BuildRestConfig(host, credentials string) (*rest.Config, error) {
	if strings.Contains(credentials, "apiVersion:") && strings.Contains(credentials, "clusters:") {
		clientConfig, err := clientcmd.NewClientConfigFromBytes([]byte(credentials))
		if err != nil {
			return nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
		}
		cfg, err := clientConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get rest config from kubeconfig: %w", err)
		}
		cfg.QPS = 50
		cfg.Burst = 100
		return cfg, nil
	}

	apiHost := host
	if apiHost != "" && !strings.HasPrefix(apiHost, "http://") && !strings.HasPrefix(apiHost, "https://") {
		apiHost = "https://" + apiHost + ":6443"
	}
	if apiHost == "" {
		return nil, fmt.Errorf("api server host is required for bearer token credentials")
	}

	return &rest.Config{
		Host:        apiHost,
		BearerToken: credentials,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
		QPS:   50,
		Burst: 100,
	}, nil
}

// BuildClientsForCluster constructs kubernetes and dynamic clients for a remote cluster.
func BuildClientsForCluster(host, credentials string) (kubernetes.Interface, dynamic.Interface, error) {
	cfg, err := BuildRestConfig(host, credentials)
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	return client, dynClient, nil
}

// TestClusterConnection verifies that credentials can reach the Kubernetes API.
func TestClusterConnection(ctx context.Context, host, credentials string) (string, error) {
	client, _, err := BuildClientsForCluster(host, credentials)
	if err != nil {
		return "", err
	}
	_, err = client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", err
	}
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return "connected", nil
	}
	return version.GitVersion, nil
}

// ListClusterNamespaces returns application namespace names on a cluster.
func ListClusterNamespaces(ctx context.Context, host, credentials string) ([]string, error) {
	return GetRemoteNamespaces(ctx, host, credentials)
}

// ListWorkloadsInNamespace lists Deployments, StatefulSets, and DaemonSets in a namespace.
func ListWorkloadsInNamespace(ctx context.Context, client kubernetes.Interface, namespace string) ([]WorkloadInfo, error) {
	var workloads []WorkloadInfo

	deployments, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	for _, d := range deployments.Items {
		workloads = append(workloads, workloadFromTemplate(d.Name, namespace, "Deployment", d.Spec.Replicas, d.Status.ReadyReplicas, d.Spec.Template))
	}

	statefulSets, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list statefulsets: %w", err)
	}
	for _, s := range statefulSets.Items {
		replicas := s.Spec.Replicas
		workloads = append(workloads, workloadFromTemplate(s.Name, namespace, "StatefulSet", replicas, s.Status.ReadyReplicas, s.Spec.Template))
	}

	daemonSets, err := client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list daemonsets: %w", err)
	}
	for _, d := range daemonSets.Items {
		var replicas int32 = d.Status.DesiredNumberScheduled
		workloads = append(workloads, workloadFromTemplate(d.Name, namespace, "DaemonSet", &replicas, d.Status.NumberReady, d.Spec.Template))
	}

	return workloads, nil
}

func containerImagesAndCommands(containers []corev1.Container) (images, commands []string) {
	for _, c := range containers {
		images = append(images, c.Image)
		commands = append(commands, c.Command...)
		commands = append(commands, c.Args...)
	}
	return images, commands
}

func resolveLanguage(requested string, containers []corev1.Container) string {
	req := strings.ToLower(strings.TrimSpace(requested))
	if req == "unknown" || req == "auto" {
		req = ""
	}
	images, commands := containerImagesAndCommands(containers)
	return resolveInject(req, images, commands)
}

func isStaticHTTPStack(lang string) bool {
	switch strings.ToLower(strings.TrimSpace(lang)) {
	case "nginx", "apache-httpd", "apache", "httpd":
		return true
	}
	return false
}

func workloadFromTemplate(name, namespace, kind string, replicas *int32, ready int32, template corev1.PodTemplateSpec) WorkloadInfo {
	labels := make(map[string]string)
	for k, v := range template.Labels {
		labels[k] = v
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: template.Annotations}, Spec: template.Spec}
	lang := resolveLanguage("", template.Spec.Containers)
	instr, _, details := detectInstrumentation(pod)
	return WorkloadInfo{
		Name:         name,
		Namespace:    namespace,
		Kind:         kind,
		Replicas:     derefInt32(replicas),
		Ready:        ready,
		Language:     lang,
		Instrumented: instr,
		Labels:       labels,
		Details:      details,
		IsFrontend:   isStaticHTTPStack(lang),
	}
}

func derefInt32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

// ApplyWorkloadInstrumentation patches a workload to enable or disable OTel injection.
// It returns the resolved language stack and any error.
func ApplyWorkloadInstrumentation(ctx context.Context, client kubernetes.Interface, clusterID, namespace, workloadName, kind, language, agentNamespace string, enabled bool) (string, error) {
	if agentNamespace == "" {
		agentNamespace = AgentNamespaceFallback()
	}
	if enabled && language != "" && language != "unknown" && language != "auto" && normalizeInjectLanguage(language) == "" {
		return "", fmt.Errorf("cannot auto-instrument %s/%s: language could not be detected (got %q) — supported languages are java, nodejs, python, go, dotnet, php", namespace, workloadName, language)
	}
	instrumentationName := namespace + "-instrumentation"
	endpoint := fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4317", agentNamespace)

	switch kind {
	case "Deployment":
		return patchDeployment(ctx, client, namespace, workloadName, language, instrumentationName, endpoint, clusterID, enabled)
	case "StatefulSet":
		return patchStatefulSet(ctx, client, namespace, workloadName, language, instrumentationName, endpoint, clusterID, enabled)
	case "DaemonSet":
		return patchDaemonSet(ctx, client, namespace, workloadName, language, instrumentationName, endpoint, clusterID, enabled)
	default:
		return "", fmt.Errorf("unsupported workload kind: %s", kind)
	}
}

func detectLanguageFromPodTemplate(template *corev1.PodTemplateSpec) string {
	if template == nil {
		return ""
	}
	return resolveLanguage("", template.Spec.Containers)
}

func patchDeployment(ctx context.Context, client kubernetes.Interface, namespace, name, language, instrumentationName, endpoint, clusterID string, enabled bool) (string, error) {
	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	resolvedLang := resolveLanguage(language, deploy.Spec.Template.Spec.Containers)
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument Deployment %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP, nginx) first", namespace, name)
	}
	patchPodTemplate(&deploy.Spec.Template, resolvedLang, instrumentationName, endpoint, namespace, clusterID, enabled)
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, deploy, metav1.UpdateOptions{})
	return resolvedLang, err
}

func patchStatefulSet(ctx context.Context, client kubernetes.Interface, namespace, name, language, instrumentationName, endpoint, clusterID string, enabled bool) (string, error) {
	sts, err := client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	resolvedLang := resolveLanguage(language, sts.Spec.Template.Spec.Containers)
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument StatefulSet %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP, nginx) first", namespace, name)
	}
	patchPodTemplate(&sts.Spec.Template, resolvedLang, instrumentationName, endpoint, namespace, clusterID, enabled)
	_, err = client.AppsV1().StatefulSets(namespace).Update(ctx, sts, metav1.UpdateOptions{})
	return resolvedLang, err
}

func patchDaemonSet(ctx context.Context, client kubernetes.Interface, namespace, name, language, instrumentationName, endpoint, clusterID string, enabled bool) (string, error) {
	ds, err := client.AppsV1().DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	resolvedLang := resolveLanguage(language, ds.Spec.Template.Spec.Containers)
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument DaemonSet %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP, nginx) first", namespace, name)
	}
	patchPodTemplate(&ds.Spec.Template, resolvedLang, instrumentationName, endpoint, namespace, clusterID, enabled)
	_, err = client.AppsV1().DaemonSets(namespace).Update(ctx, ds, metav1.UpdateOptions{})
	return resolvedLang, err
}

func patchPodTemplate(template *corev1.PodTemplateSpec, language, instrumentationName, endpoint, namespace, clusterID string, enabled bool) {
	if template.Annotations == nil {
		template.Annotations = make(map[string]string)
	}

	injectLang := normalizeInjectLanguage(language)
	if enabled && injectLang != "" {
		desiredInjectKey := "instrumentation.opentelemetry.io/inject-" + injectLang
		for k := range template.Annotations {
			if strings.HasPrefix(k, "instrumentation.opentelemetry.io/inject-") && k != desiredInjectKey {
				delete(template.Annotations, k)
			}
		}
		template.Annotations[desiredInjectKey] = instrumentationName
		if injectLang == "go" {
			if target, ok := template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"]; !ok || target == "" || target == "/app" {
				template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"] = guessGoTargetExe(template)
			}
		} else {
			delete(template.Annotations, "instrumentation.opentelemetry.io/otel-go-auto-target-exe")
		}
	} else {
		for k := range template.Annotations {
			if strings.HasPrefix(k, "instrumentation.opentelemetry.io/") {
				delete(template.Annotations, k)
			}
		}
	}

	httpEndpoint := strings.Replace(endpoint, ":4317", ":4318", 1)
	if !strings.Contains(httpEndpoint, "://") {
		httpEndpoint = "http://" + httpEndpoint
	}
	origLang := strings.ToLower(strings.TrimSpace(language))
	for i := range template.Spec.Containers {
		if enabled && (injectLang == "sdk" || origLang == "php" || origLang == "ruby" || origLang == "rails") {
			setLibraryEnvVars(&template.Spec.Containers[i], httpEndpoint, namespace, clusterID, origLang)
		} else {
			removeOTelEnvVars(&template.Spec.Containers[i])
		}
	}
}

func normalizeInjectLanguage(language string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "java", "nodejs", "python", "go", "dotnet", "nginx":
		return strings.ToLower(strings.TrimSpace(language))
	case "node":
		return "nodejs"
	case "php", "ruby", "rails":
		return "sdk"
	case "apache", "httpd", "apache-httpd", "apachehttpd":
		return "apache-httpd"
	default:
		return ""
	}
}

func guessGoTargetExe(template *corev1.PodTemplateSpec) string {
	if template == nil || len(template.Spec.Containers) == 0 {
		return "/app"
	}
	c := template.Spec.Containers[0]
	parts := append(append([]string{}, c.Command...), c.Args...)
	wrappers := map[string]bool{"sh": true, "bash": true, "ash": true, "/bin/sh": true, "/bin/bash": true, "entrypoint.sh": true, "docker-entrypoint.sh": true, "dumb-init": true, "tini": true, "env": true, "/usr/bin/env": true}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || strings.HasPrefix(part, "-") {
			continue
		}
		base := part
		if i := strings.LastIndex(part, "/"); i >= 0 {
			base = part[i+1:]
		}
		if wrappers[part] || wrappers[base] {
			continue
		}
		if strings.HasPrefix(part, "/") {
			return part
		}
		if strings.HasPrefix(part, "./") {
			return "/" + strings.TrimPrefix(part, "./")
		}
	}
	if name := executableName(c.Name); name != "" && !genericProcessName(name) {
		return "/" + name
	}
	imageName := c.Image
	if slash := strings.LastIndex(imageName, "/"); slash >= 0 {
		imageName = imageName[slash+1:]
	}
	if colon := strings.LastIndex(imageName, ":"); colon >= 0 {
		imageName = imageName[:colon]
	}
	if at := strings.LastIndex(imageName, "@"); at >= 0 {
		imageName = imageName[:at]
	}
	if name := executableName(imageName); name != "" && !genericProcessName(name) {
		return "/" + name
	}
	return "/app"
}

func genericProcessName(name string) bool {
	switch strings.ToLower(name) {
	case "app", "server", "web", "main", "container", "workload", "service":
		return true
	}
	return false
}

func executableName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, " \t\n") {
		return ""
	}
	return value
}

func setLibraryEnvVars(container *corev1.Container, endpoint, namespace, clusterID, lang string) {
	envMap := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
		"OTEL_TRACES_EXPORTER":        "otlp",
		"OTEL_METRICS_EXPORTER":       "none",
		"OTEL_LOGS_EXPORTER":          "none",
		"OTEL_SERVICE_NAME":           container.Name,
		"OTEL_RESOURCE_ATTRIBUTES":    fmt.Sprintf("k8s.namespace.name=%s,service.namespace=%s,k8s.cluster.name=%s", namespace, namespace, clusterID),
	}
	if lang == "php" {
		envMap["OTEL_PHP_AUTOLOAD_ENABLED"] = "true"
	}
	existing := make(map[string]int)
	for i, env := range container.Env {
		existing[env.Name] = i
	}
	for name, value := range envMap {
		if idx, ok := existing[name]; ok {
			container.Env[idx].Value = value
		} else {
			container.Env = append(container.Env, corev1.EnvVar{Name: name, Value: value})
		}
	}
}

func removeOTelEnvVars(container *corev1.Container) {
	var filtered []corev1.EnvVar
	for _, env := range container.Env {
		if strings.HasPrefix(env.Name, "OTEL_") {
			continue
		}
		filtered = append(filtered, env)
	}
	container.Env = filtered
}

// GetRemoteInstrumentations lists Instrumentation CRDs on a remote cluster.
func GetRemoteInstrumentations(ctx context.Context, dynClient dynamic.Interface) ([]*InstrumentationInfo, error) {
	gvr := schema.GroupVersionResource{
		Group:    "opentelemetry.io",
		Version:  "v1alpha1",
		Resource: "instrumentations",
	}
	list, err := dynClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []*InstrumentationInfo
	for _, item := range list.Items {
		if !isAppNamespace(item.GetNamespace()) {
			continue
		}
		spec, found, _ := unstructured.NestedMap(item.Object, "spec")
		var endpoint, sampler string
		if found {
			if exporter, ok, _ := unstructured.NestedMap(spec, "exporter"); ok {
				endpoint, _, _ = unstructured.NestedString(exporter, "endpoint")
			}
			if samplerMap, ok, _ := unstructured.NestedMap(spec, "sampler"); ok {
				sampler, _, _ = unstructured.NestedString(samplerMap, "type")
			}
		}
		result = append(result, &InstrumentationInfo{
			Name:      item.GetName(),
			Namespace: item.GetNamespace(),
			Endpoint:  endpoint,
			Sampler:   sampler,
		})
	}
	return result, nil
}

// FindAgentNamespace discovers the namespace where agent-backend is running on
// the target cluster. It returns "" when the agent pod cannot be found so the
// caller can decide the fallback (an explicit override or AgentNamespaceFallback).
func FindAgentNamespace(ctx context.Context, client kubernetes.Interface) string {
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		isAgent := strings.HasPrefix(pod.Name, "agent-backend") ||
			pod.Labels["app.kubernetes.io/name"] == "agent-backend" ||
			pod.Labels["app"] == "agent-backend" ||
			pod.Labels["app"] == "kubetrace-agent"
		if isAgent {
			return pod.Namespace
		}
	}
	return ""
}

// AgentNamespaceFallback returns the fallback agent-backend namespace, used only
// when the agent pod cannot be discovered dynamically. It is configurable via the
// AGENT_NAMESPACE env var and otherwise resolves to the api-backend's own
// namespace — it never hardcodes a cluster-specific value.
func AgentNamespaceFallback() string {
	if ns := os.Getenv("AGENT_NAMESPACE"); ns != "" {
		return ns
	}
	return getCurrentNamespace()
}
