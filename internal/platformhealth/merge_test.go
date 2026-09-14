package platformhealth

import "testing"

func TestMergePrefersLowerPriority(t *testing.T) {
	local := &Report{
		Namespace: "apm-tracing",
		Components: []ComponentReport{{
			ID: "agent-backend",
			Pods: []PodReport{{
				Name:      "agent-backend-a",
				Namespace: "apm-tracing",
				Component: "agent-backend",
				Status:    SeverityOK,
				RecentLogs: []string{"local-log"},
			}},
		}},
	}
	pushed := &Report{
		Namespace: "apm-tracing",
		Components: []ComponentReport{{
			ID: "agent-backend",
			Pods: []PodReport{{
				Name:      "agent-backend-a",
				Namespace: "apm-tracing",
				Component: "agent-backend",
				Status:    SeverityWarning,
				RecentLogs: []string{"remote-log"},
			}},
		}},
	}
	merged := Merge([]SourceReport{
		{Cluster: "local", Priority: 0, Report: local},
		{Cluster: "apps", Priority: 2, Report: pushed},
	})
	var agent PodReport
	for _, c := range merged.Components {
		if c.ID == "agent-backend" && len(c.Pods) > 0 {
			agent = c.Pods[0]
		}
	}
	if len(agent.RecentLogs) != 1 || agent.RecentLogs[0] != "local-log" {
		t.Fatalf("expected local log win, got %#v", agent.RecentLogs)
	}
	if agent.Cluster != "local" {
		t.Fatalf("cluster=%s", agent.Cluster)
	}
}

func TestMergeKeepsNotesWhenReportNil(t *testing.T) {
	merged := Merge([]SourceReport{
		{Cluster: "local", Priority: 0, Mode: "local", ClusterStatus: "error", Note: "local collect failed: boom"},
		{Cluster: "apps", Priority: 2, Mode: "agent-push", ClusterStatus: "stale", Note: "STALE agent health", Report: &Report{
			Components: []ComponentReport{{ID: "agent-backend", Pods: []PodReport{{Name: "a", Namespace: "ns", Component: "agent-backend"}}}},
		}},
	})
	if len(merged.Notes) == 0 {
		t.Fatal("expected notes preserved")
	}
	if len(merged.Clusters) < 2 {
		t.Fatalf("clusters=%v", merged.Clusters)
	}
}
