package api

import (
	"context"
	"fmt"

	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/store"
)

func reportedInstrumentationsToInfo(items []store.ReportedInstrumentation) []*k8s.InstrumentationInfo {
	out := make([]*k8s.InstrumentationInfo, 0, len(items))
	for _, item := range items {
		out = append(out, &k8s.InstrumentationInfo{
			Name:      item.Name,
			Namespace: item.Namespace,
			Endpoint:  item.Endpoint,
			Sampler:   item.Sampler,
		})
	}
	return out
}

func (h *Handler) adminInstrumentationTargets(clusterFilter string) []store.AgentClusterInfo {
	seen := map[string]struct{}{}
	var out []store.AgentClusterInfo
	add := func(id, agentNs string) {
		if id == "" {
			return
		}
		if clusterFilter != "" && id != clusterFilter {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, store.AgentClusterInfo{ClusterID: id, AgentNamespace: agentNs})
	}
	for _, ac := range h.store.GetAgentManagedClusters() {
		add(ac.ClusterID, ac.AgentNamespace)
	}
	for _, id := range h.store.ReportedInstrumentationClusterIDs() {
		add(id, h.store.GetAgentNamespaceForCluster(id))
	}
	if inv, err := h.store.GetClusterInventory(); err == nil {
		for _, cluster := range inv {
			if cluster.Token != "" && cluster.Status == "Active" {
				add(cluster.ID, cluster.AgentNamespace)
			}
		}
	}
	if clusterFilter != "" {
		add(clusterFilter, h.store.GetAgentNamespaceForCluster(clusterFilter))
	}
	return out
}

func (h *Handler) instrumentationsForCluster(ctx context.Context, clusterID string) []*k8s.InstrumentationInfo {
	inv, _ := h.store.GetClusterInventory()
	var cluster store.ClusterInventoryItem
	for _, item := range inv {
		if item.ID == clusterID {
			cluster = item
			break
		}
	}

	seen := map[string]struct{}{}
	var all []*k8s.InstrumentationInfo
	add := func(items []*k8s.InstrumentationInfo) {
		for _, inst := range items {
			if inst == nil {
				continue
			}
			key := inst.Namespace + "/" + inst.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			copyInst := *inst
			copyInst.Name = fmt.Sprintf("[%s] %s", clusterID, inst.Name)
			if h.store.IsNamespaceDisabledForCluster(clusterID, copyInst.Namespace) || h.store.IsNamespaceDisabled(copyInst.Namespace) {
				copyInst.Sampler = "always_off"
			}
			all = append(all, &copyInst)
		}
	}

	if cluster.Token != "" {
		if _, dynClient, err := k8s.BuildClientsForCluster(clusterHost(cluster), cluster.Token); err == nil {
			if remote, err := k8s.GetRemoteInstrumentations(ctx, dynClient); err == nil {
				add(remote)
			}
		}
	}
	add(reportedInstrumentationsToInfo(h.store.GetReportedInstrumentations(clusterID)))
	if all == nil {
		all = []*k8s.InstrumentationInfo{}
	}
	return all
}
