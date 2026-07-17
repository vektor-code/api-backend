package k8s

import (
	"context"
	"fmt"
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

	// Fetch Ingresses to identify frontend services
	frontendServices := make(map[string]bool)
	ingresses, err := client.NetworkingV1().Ingresses(namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, ing := range ingresses.Items {
			if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
				frontendServices[ing.Spec.DefaultBackend.Service.Name] = true
			}
			for _, rule := range ing.Spec.Rules {
				if rule.HTTP == nil {
					continue
				}

				for _, path := range rule.HTTP.Paths {
					if path.Path == "/" || path.Path == "" || path.Path == "/*" {
						if path.Backend.Service != nil {
							frontendServices[path.Backend.Service.Name] = true
						}
					}
				}
			}
		}
	}

	// Fetch Services to map workload label selectors to frontend services
	services, err := client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	var svcList []corev1.Service
	if err == nil {
		svcList = services.Items
	}

	// Helper to check if a workload is a static frontend
	isWorkloadFrontend := func(workloadName string, template corev1.PodTemplateSpec) bool {
		// 1. If it has OTel auto-instrumentation annotations, it is definitely a backend microservice
		if template.Annotations != nil {
			for k, v := range template.Annotations {
				if strings.HasPrefix(k, "instrumentation.opentelemetry.io/inject-") && v != "" {
					return false
				}
			}
		}

		// 2. Check for backend runtimes or environment configurations
		isBackend := false
		for _, c := range template.Spec.Containers {
			img := strings.ToLower(c.Image)
			if strings.Contains(img, "java") || strings.Contains(img, "openjdk") || strings.Contains(img, "tomcat") || 
				strings.Contains(img, "python") || strings.Contains(img, "django") || strings.Contains(img, "flask") || 
				strings.Contains(img, "php") || strings.Contains(img, "fpm") || strings.Contains(img, "laravel") ||
				strings.Contains(img, "dotnet") || strings.Contains(img, "aspnet") || 
				strings.Contains(img, "golang") || strings.Contains(img, "node:") || strings.Contains(img, "node-") {
				isBackend = true
				break
			}
			for _, env := range c.Env {
				envName := strings.ToUpper(env.Name)
				if strings.Contains(envName, "DB_") || strings.Contains(envName, "DATABASE") || 
					strings.Contains(envName, "REDIS") || strings.Contains(envName, "KAFKA") || 
					strings.Contains(envName, "POSTGRES") || strings.Contains(envName, "MONGO") ||
					strings.Contains(envName, "RABBITMQ") || strings.Contains(envName, "SPRING_") {
					isBackend = true
					break
				}
			}
			if isBackend {
				break
			}
		}
		if isBackend {
			return false
		}

		// 3. Check if any container runs a static file web server (nginx, caddy, httpd, apache)
		for _, c := range template.Spec.Containers {
			img := strings.ToLower(c.Image)
			if strings.Contains(img, "nginx") || strings.Contains(img, "caddy") || strings.Contains(img, "httpd") || strings.Contains(img, "apache") {
				return true
			}
		}

		// 4. Check if the workload matches any service that is exposed at the root "/" in Ingresses
		for _, svc := range svcList {
			if !frontendServices[svc.Name] {
				continue
			}
			if len(svc.Spec.Selector) > 0 {
				matches := true
				for k, v := range svc.Spec.Selector {
					if template.Labels[k] != v {
						matches = false
						break
					}
				}
				if matches {
					return true
				}
			}
		}

		return false
	}

	deployments, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	for _, d := range deployments.Items {
		isFrontend := isWorkloadFrontend(d.Name, d.Spec.Template)
		workloads = append(workloads, workloadFromTemplate(d.Name, namespace, "Deployment", d.Spec.Replicas, d.Status.ReadyReplicas, d.Spec.Template, isFrontend))
	}

	statefulSets, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list statefulsets: %w", err)
	}
	for _, s := range statefulSets.Items {
		replicas := s.Spec.Replicas
		isFrontend := isWorkloadFrontend(s.Name, s.Spec.Template)
		workloads = append(workloads, workloadFromTemplate(s.Name, namespace, "StatefulSet", replicas, s.Status.ReadyReplicas, s.Spec.Template, isFrontend))
	}

	daemonSets, err := client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list daemonsets: %w", err)
	}
	for _, d := range daemonSets.Items {
		var replicas int32 = d.Status.DesiredNumberScheduled
		isFrontend := isWorkloadFrontend(d.Name, d.Spec.Template)
		workloads = append(workloads, workloadFromTemplate(d.Name, namespace, "DaemonSet", &replicas, d.Status.NumberReady, d.Spec.Template, isFrontend))
	}

	return workloads, nil
}

func workloadFromTemplate(name, namespace, kind string, replicas *int32, ready int32, template corev1.PodTemplateSpec, isFrontend bool) WorkloadInfo {
	labels := make(map[string]string)
	for k, v := range template.Labels {
		labels[k] = v
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: template.Annotations}, Spec: template.Spec}
	lang := detectLanguage(pod)
	if isFrontend {
		lang = ""
	}
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
		IsFrontend:   isFrontend,
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
		agentNamespace = "trace-prod"
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
	for _, c := range template.Spec.Containers {
		img := strings.ToLower(c.Image)
		if strings.Contains(img, "java") || strings.Contains(img, "openjdk") || strings.Contains(img, "jre") || strings.Contains(img, "tomcat") || strings.Contains(img, "spring") {
			return "java"
		}
		if strings.Contains(img, "node") || strings.Contains(img, "npm") {
			return "nodejs"
		}
		if strings.Contains(img, "python") || strings.Contains(img, "pip") {
			return "python"
		}
		if strings.Contains(img, "go") || strings.Contains(img, "golang") {
			return "go"
		}
		if strings.Contains(img, "dotnet") || strings.Contains(img, "aspnet") {
			return "dotnet"
		}
		if strings.Contains(img, "php") {
			return "php"
		}

		for _, env := range c.Env {
			name := strings.ToUpper(env.Name)
			if strings.Contains(name, "JAVA") {
				return "java"
			}
			if strings.Contains(name, "NODE") {
				return "nodejs"
			}
			if strings.Contains(name, "PYTHON") {
				return "python"
			}
			if strings.Contains(name, "GOPATH") || strings.Contains(name, "GOROOT") {
				return "go"
			}
			if strings.Contains(name, "DOTNET") {
				return "dotnet"
			}
			if strings.Contains(name, "PHP") {
				return "php"
			}
		}
	}
	return ""
}

func patchDeployment(ctx context.Context, client kubernetes.Interface, namespace, name, language, instrumentationName, endpoint, clusterID string, enabled bool) (string, error) {
	deploy, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	resolvedLang := language
	if resolvedLang == "" || resolvedLang == "unknown" || resolvedLang == "auto" {
		resolvedLang = detectLanguageFromPodTemplate(&deploy.Spec.Template)
	}
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument Deployment %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP) first", namespace, name)
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
	resolvedLang := language
	if resolvedLang == "" || resolvedLang == "unknown" || resolvedLang == "auto" {
		resolvedLang = detectLanguageFromPodTemplate(&sts.Spec.Template)
	}
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument StatefulSet %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP) first", namespace, name)
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
	resolvedLang := language
	if resolvedLang == "" || resolvedLang == "unknown" || resolvedLang == "auto" {
		resolvedLang = detectLanguageFromPodTemplate(&ds.Spec.Template)
	}
	if enabled && normalizeInjectLanguage(resolvedLang) == "" {
		return "", fmt.Errorf("cannot auto-instrument DaemonSet %s/%s: language could not be detected — please select a tech stack (Go, NodeJS, Python, Java, .NET, PHP) first", namespace, name)
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
	if enabled && injectLang != "" && injectLang != "php" {
		template.Annotations["instrumentation.opentelemetry.io/inject-"+injectLang] = instrumentationName
		if injectLang == "go" {
			if _, ok := template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"]; !ok {
				template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"] = guessGoTargetExe(template)
			}
		}
	} else {
		for k := range template.Annotations {
			if strings.HasPrefix(k, "instrumentation.opentelemetry.io/") {
				delete(template.Annotations, k)
			}
		}
	}

	for i := range template.Spec.Containers {
		if enabled && injectLang == "php" {
			setPHPEnvVars(&template.Spec.Containers[i], endpoint, namespace, clusterID)
		} else {
			removeOTelEnvVars(&template.Spec.Containers[i])
		}
	}
}

func normalizeInjectLanguage(language string) string {
	switch strings.ToLower(language) {
	case "java", "nodejs", "node", "python", "go", "dotnet", "php":
		if language == "node" {
			return "nodejs"
		}
		return strings.ToLower(language)
	default:
		return ""
	}
}

func guessGoTargetExe(template *corev1.PodTemplateSpec) string {
	for _, c := range template.Spec.Containers {
		if len(c.Command) > 0 {
			return c.Command[0]
		}
		if c.Args != nil && len(c.Args) > 0 {
			return c.Args[0]
		}
	}
	return "/app"
}

func setPHPEnvVars(container *corev1.Container, endpoint, namespace, clusterID string) {
	envMap := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
		"OTEL_TRACES_EXPORTER":        "otlp",
		"OTEL_PHP_AUTOLOAD_ENABLED":   "true",
		"OTEL_LOGS_EXPORTER":          "none",
		"OTEL_SERVICE_NAME":           container.Name,
		"OTEL_RESOURCE_ATTRIBUTES":    fmt.Sprintf("k8s.namespace.name=%s,service.namespace=%s,k8s.cluster.name=%s", namespace, namespace, clusterID),
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

// FindAgentNamespace discovers the namespace where agent-backend is running.
func FindAgentNamespace(ctx context.Context, client kubernetes.Interface) string {
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "trace-prod"
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
	return "trace-prod"
}
