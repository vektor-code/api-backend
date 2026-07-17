package store

import (
	"sort"
	"sync"
	"time"
)

// AgentClusterInfo tracks a cluster where agent-backend is actively running.
type AgentClusterInfo struct {
	ClusterID      string    `json:"clusterId"`
	AgentNamespace string    `json:"agentNamespace"`
	LastSeen       time.Time `json:"lastSeen"`
}

const agentHeartbeatTTL = 2 * time.Minute

type agentClusterRegistry struct {
	mu       sync.RWMutex
	clusters map[string]AgentClusterInfo
}

func (s *Store) initAgentRegistry() {
	if s.agentClusters == nil {
		s.agentClusters = &agentClusterRegistry{
			clusters: make(map[string]AgentClusterInfo),
		}
	}
}

// RegisterAgentHeartbeat records that agent-backend is alive on a target cluster.
func (s *Store) RegisterAgentHeartbeat(clusterID, agentNamespace string) {
	if clusterID == "" {
		clusterID = "default"
	}
	s.initAgentRegistry()
	s.agentClusters.mu.Lock()
	defer s.agentClusters.mu.Unlock()
	s.agentClusters.clusters[clusterID] = AgentClusterInfo{
		ClusterID:      clusterID,
		AgentNamespace: agentNamespace,
		LastSeen:       time.Now().UTC(),
	}
}

// GetAgentManagedClusters returns clusters with a recent agent-backend heartbeat.
func (s *Store) GetAgentManagedClusters() []AgentClusterInfo {
	s.initAgentRegistry()
	s.agentClusters.mu.RLock()
	defer s.agentClusters.mu.RUnlock()
	now := time.Now().UTC()
	var result []AgentClusterInfo
	for _, info := range s.agentClusters.clusters {
		if now.Sub(info.LastSeen) <= agentHeartbeatTTL {
			result = append(result, info)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ClusterID < result[j].ClusterID
	})
	return result
}

// IsAgentManagedCluster returns true when the cluster has a live agent-backend heartbeat.
func (s *Store) IsAgentManagedCluster(clusterID string) bool {
	s.initAgentRegistry()
	s.agentClusters.mu.RLock()
	defer s.agentClusters.mu.RUnlock()
	info, ok := s.agentClusters.clusters[clusterID]
	if !ok {
		return false
	}
	return time.Now().UTC().Sub(info.LastSeen) <= agentHeartbeatTTL
}

// GetAgentNamespaceForCluster returns the namespace where agent-backend runs on a cluster.
func (s *Store) GetAgentNamespaceForCluster(clusterID string) string {
	s.initAgentRegistry()
	s.agentClusters.mu.RLock()
	defer s.agentClusters.mu.RUnlock()
	if info, ok := s.agentClusters.clusters[clusterID]; ok {
		if info.AgentNamespace != "" {
			return info.AgentNamespace
		}
	}
	return "trace-prod"
}
