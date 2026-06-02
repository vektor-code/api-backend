package k8s

import (
	"context"
	"fmt"
	"log"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
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
	client    kubernetes.Interface
	pods      map[string]*PodInfo // key: namespace/podname
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

	return &Watcher{
		client: client,
		pods:   make(map[string]*PodInfo),
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
