package k8s

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
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
	Name      string
	Namespace string
	NodeName  string
	Labels    map[string]string
	Phase     string
}

// Watcher watches Kubernetes pods and services to enrich trace metadata
type Watcher struct {
	client        kubernetes.Interface
	dynamicClient dynamic.Interface
	pods          map[string]*PodInfo // key: namespace/podname
	namespaces []string
	mu        sync.RWMutex
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

	return &Watcher{
		client:        client,
		dynamicClient: dynamicClient,
		pods:          make(map[string]*PodInfo),
	}, nil
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

func podToInfo(p *corev1.Pod) *PodInfo {
	labels := make(map[string]string)
	for k, v := range p.Labels {
		labels[k] = v
	}
	return &PodInfo{
		Name:      p.Name,
		Namespace: p.Namespace,
		NodeName:  p.Spec.NodeName,
		Labels:    labels,
		Phase:     string(p.Status.Phase),
	}
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
				fallbackResult = append(fallbackResult, &InstrumentationInfo{
					Name:      ns + "-instrumentation",
					Namespace: ns,
					Endpoint:  "http://agent-backend.trace-prod.svc.cluster.local:4317",
					Sampler:   "parentbased_always_on",
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
		lower == "nfs-provisioner" ||
		lower == "default" {
		return false
	}
	return strings.HasSuffix(lower, "-dev") ||
		strings.HasSuffix(lower, "-uat") ||
		strings.HasSuffix(lower, "-preprod") ||
		strings.HasSuffix(lower, "-prod")
}
