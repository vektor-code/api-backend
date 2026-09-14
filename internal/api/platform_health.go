package api

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/platformhealth"
)

const (
	agentHealthFresh = 2 * time.Minute
	agentHealthStale = 30 * time.Minute
)

// GET /api/admin/platform/health?namespace=&tailLines=&cluster=
//
// Collects APM pod diagnostics from:
//  1. local/monitoring cluster (api ServiceAccount) — same-cluster deploy
//  2. remote inventory tokens — optional admin-configured kube access
//  3. agent-pushed snapshots — separate-cluster deploy (agent has ClusterRole + pods/log there)
//
// Partial outages are non-fatal: if monitoring or the apps cluster is down, the
// other side's data (or last stale agent snapshot) is still returned with notes.
func (h *Handler) GetPlatformHealth(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	ns := strings.TrimSpace(c.Query("namespace"))
	clusterFilter := strings.TrimSpace(c.Query("cluster"))
	tail := int64(200)
	if raw := strings.TrimSpace(c.Query("tailLines")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			if n > 2000 {
				n = 2000
			}
			tail = n
		}
	}
	opts := platformhealth.Options{Namespace: ns, TailLines: tail}
	sources := make([]platformhealth.SourceReport, 0, 8)
	seenLiveRemote := map[string]bool{}

	// 1) Monitoring / same cluster via api-backend ServiceAccount.
	if h.k8s != nil && h.k8s.Client() != nil {
		local, err := platformhealth.Collect(c.Context(), h.k8s.Client(), opts)
		if err != nil {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       "local",
				Priority:      0,
				Mode:          "local",
				ClusterStatus: "error",
				Note:          "local/monitoring collect failed: " + err.Error(),
			})
		} else {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       "local",
				Priority:      0,
				Mode:          "local",
				ClusterStatus: "ok",
				Report:        local,
				LastSeen:      time.Now().UTC(),
			})
		}
	} else {
		sources = append(sources, platformhealth.SourceReport{
			Cluster:       "local",
			Priority:      0,
			Mode:          "local",
			ClusterStatus: "error",
			Note:          "kubernetes client unavailable on api-backend; using agent-pushed / remote-token sources only",
		})
	}

	if h.store == nil {
		return c.JSON(platformhealth.Merge(sources))
	}

	// 2) Separate clusters with inventory credentials (live pull).
	inv, _ := h.store.GetClusterInventory()
	for _, item := range inv {
		if strings.TrimSpace(item.Token) == "" {
			continue
		}
		if clusterFilter != "" && item.ID != clusterFilter {
			continue
		}
		client, _, err := k8s.BuildClientsForCluster(clusterHost(item), item.Token)
		if err != nil {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       item.ID,
				Priority:      1,
				Mode:          "remote-token",
				ClusterStatus: "unreachable",
				Note:          "remote client " + item.ID + ": " + err.Error(),
			})
			continue
		}
		remoteNS := ns
		if remoteNS == "" {
			remoteNS = item.AgentNamespace
		}
		if remoteNS == "" {
			remoteNS = h.store.GetAgentNamespaceForCluster(item.ID)
		}
		if remoteNS == "" {
			remoteNS = k8s.FindAgentNamespace(c.Context(), client)
		}
		if remoteNS == "" {
			remoteNS = platformhealth.DefaultNamespace()
		}
		remoteOpts := opts
		remoteOpts.Namespace = remoteNS
		remote, err := platformhealth.Collect(c.Context(), client, remoteOpts)
		if err != nil {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       item.ID,
				Priority:      1,
				Mode:          "remote-token",
				ClusterStatus: "unreachable",
				Note:          "remote collect " + item.ID + " (" + remoteNS + "): " + err.Error(),
			})
			continue
		}
		seenLiveRemote[item.ID] = true
		sources = append(sources, platformhealth.SourceReport{
			Cluster:       item.ID,
			Priority:      1,
			Mode:          "remote-token",
			ClusterStatus: "ok",
			Report:        remote,
			LastSeen:      time.Now().UTC(),
		})
	}

	// 3) Agent-pushed snapshots (works without storing a kube token on the API).
	// Keep stale snapshots so Admin still shows last known agent logs if the
	// apps cluster or network path is down.
	for _, snap := range h.store.GetAgentPlatformHealthSnapshots(agentHealthFresh, agentHealthStale) {
		if clusterFilter != "" && snap.ClusterID != clusterFilter {
			continue
		}
		if seenLiveRemote[snap.ClusterID] {
			continue // live token collect already covered this cluster
		}
		status := "ok"
		note := "agent-pushed health from cluster " + snap.ClusterID
		if snap.Stale {
			status = "stale"
			note = fmt.Sprintf(
				"STALE agent health for cluster %s (last push %s) — apps cluster or agent may be down; showing last known logs",
				snap.ClusterID,
				snap.Received.Format(time.RFC3339),
			)
		}
		sources = append(sources, platformhealth.SourceReport{
			Cluster:       snap.ClusterID,
			Priority:      2,
			Mode:          "agent-push",
			ClusterStatus: status,
			Report:        snap.Report,
			Note:          note,
			LastSeen:      snap.Received,
		})
	}

	// 4) Heartbeats with no usable health snapshot → mark unreachable.
	for _, hb := range h.store.ListAgentClusterHeartbeats() {
		if clusterFilter != "" && hb.ClusterID != clusterFilter {
			continue
		}
		if seenLiveRemote[hb.ClusterID] {
			continue
		}
		age := time.Since(hb.LastSeen)
		hasSnap := false
		for _, s := range sources {
			if s.Cluster == hb.ClusterID && s.Report != nil {
				hasSnap = true
				break
			}
		}
		if hasSnap {
			continue
		}
		if age <= agentHealthFresh {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       hb.ClusterID,
				Priority:      2,
				Mode:          "agent-push",
				ClusterStatus: "error",
				LastSeen:      hb.LastSeen,
				Note:          "agent on cluster " + hb.ClusterID + " is alive but has not pushed platform health yet",
			})
			continue
		}
		if age <= agentHealthStale {
			sources = append(sources, platformhealth.SourceReport{
				Cluster:       hb.ClusterID,
				Priority:      2,
				Mode:          "agent-push",
				ClusterStatus: "unreachable",
				LastSeen:      hb.LastSeen,
				Note: fmt.Sprintf(
					"agent heartbeat for cluster %s lost (last seen %s) — no recent health snapshot",
					hb.ClusterID,
					hb.LastSeen.Format(time.RFC3339),
				),
			})
		}
	}

	return c.JSON(platformhealth.Merge(sources))
}
