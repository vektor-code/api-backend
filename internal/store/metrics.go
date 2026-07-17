package store

import (
	"os"
	"sort"
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

// GetNamespaceStats returns pre-computed namespace-level statistics.
// Returns instantly from cache; the cache is rebuilt by syncState() after each merge.
func (s *Store) GetNamespaceStats() ([]*models.NamespaceStats, error) {
	s.precomputedStatsMu.RLock()
	cached := s.precomputedStats
	s.precomputedStatsMu.RUnlock()

	if cached != nil {
		return cached, nil
	}

	// First call before any sync has run — compute inline
	s.rebuildPrecomputedStats()

	s.precomputedStatsMu.RLock()
	defer s.precomputedStatsMu.RUnlock()
	return s.precomputedStats, nil
}

// rebuildPrecomputedStats recomputes the namespace stats from statsCache
// and stores the result for instant retrieval by GetNamespaceStats.
func (s *Store) rebuildPrecomputedStats() {
	s.statsMu.RLock()
	nsMap := make(map[string]*models.NamespaceStats)
	for key, svc := range s.statsCache {
		ns := strings.Split(key, ":")[0]
		if nsMap[ns] == nil {
			cluster := svc.Cluster
			if cluster == "" {
				cluster = s.getClusterName()
			}
			nsMap[ns] = &models.NamespaceStats{
				Namespace: ns,
				Cluster:   cluster,
			}
		}
		ns_stat := nsMap[ns]
		if ns_stat.Cluster == "" && svc.Cluster != "" {
			ns_stat.Cluster = svc.Cluster
		}
		ns_stat.Services = append(ns_stat.Services, models.ServiceStats(*svc))
		ns_stat.TraceCount += svc.RequestCount
		ns_stat.ErrorCount += svc.ErrorCount
		if svc.LastSeen.After(ns_stat.LastActivity) {
			ns_stat.LastActivity = svc.LastSeen
		}
	}
	s.statsMu.RUnlock()

	result := make([]*models.NamespaceStats, 0, len(nsMap))
	for _, ns := range nsMap {
		if ns.TraceCount > 0 {
			ns.ErrorRate = float64(ns.ErrorCount) / float64(ns.TraceCount) * 100
		}
		result = append(result, ns)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Namespace < result[j].Namespace
	})

	s.precomputedStatsMu.Lock()
	s.precomputedStats = result
	s.precomputedStatsMu.Unlock()
}

// GetTracePodsByNamespace scans recent traces and extracts unique pod metadata
// from span resource attributes (k8s.pod.name, k8s.node.name).
// This provides pod discovery for namespaces on remote clusters that send
// OTEL traces but are not reachable via the local K8s API watcher.
func (s *Store) GetTracePodsByNamespace(namespace string) []TracePodInfo {
	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	podMap := make(map[string]*TracePodInfo)
	for _, trace := range s.recentTraces {
		for _, sp := range trace.Spans {
			if sp.PodName == "" {
				continue
			}
			if namespace != "" && sp.Namespace != namespace {
				continue
			}
			key := sp.Namespace + "/" + sp.PodName
			if existing, ok := podMap[key]; !ok || sp.StartTime.After(existing.LastSeen) {
				podMap[key] = &TracePodInfo{
					Name:        sp.PodName,
					Namespace:   sp.Namespace,
					NodeName:    sp.NodeName,
					ServiceName: sp.ServiceName,
					Labels: map[string]string{
						"app":     sp.ServiceName,
						"version": "v1.0",
					},
					LastSeen: sp.StartTime,
				}
			}
		}
	}

	result := make([]TracePodInfo, 0, len(podMap))
	for _, p := range podMap {
		result = append(result, *p)
	}
	return result
}

func (s *Store) getClusterName() string {
	if name := os.Getenv("CLUSTER_NAME"); name != "" {
		return name
	}
	if name := os.Getenv("KUBERNETES_CLUSTER_NAME"); name != "" {
		return name
	}
	s.clustersMu.RLock()
	defer s.clustersMu.RUnlock()
	for c := range s.detectedClusters {
		if c != "" && c != "default" {
			return c
		}
	}
	return "cluster.local"
}
