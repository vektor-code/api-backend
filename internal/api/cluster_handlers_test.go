package api

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/store"
)

func TestWorkloadsFromReportedPodsPrefersNonEmptyLanguage(t *testing.T) {
	pods := []store.ReportedPod{
		{
			Namespace: "dev",
			Name:      "app-frontend-abc123-xyz",
			Labels:    map[string]string{"app.kubernetes.io/name": "app-frontend"},
			Language:  "",
			Ready:     true,
		},
		{
			Namespace: "dev",
			Name:      "app-frontend-def456-uvw",
			Labels:    map[string]string{"app.kubernetes.io/name": "app-frontend"},
			Language:  "nginx",
			Ready:     true,
		},
	}
	workloads := workloadsFromReportedPods(pods)
	if len(workloads) != 1 {
		t.Fatalf("got %d workloads, want 1", len(workloads))
	}
	w := workloads[0]
	if w.Language != "nginx" {
		t.Fatalf("Language = %q, want nginx", w.Language)
	}
	if w.Replicas != 2 {
		t.Fatalf("Replicas = %d, want 2", w.Replicas)
	}
	if w.Ready != 2 {
		t.Fatalf("Ready = %d, want 2", w.Ready)
	}
}

func TestWorkloadsFromReportedPodsSkipsDeadPods(t *testing.T) {
	pods := []store.ReportedPod{
		{
			Namespace: "dev",
			Name:      "app-abc",
			Labels:    map[string]string{"app.kubernetes.io/name": "app"},
			Phase:     "Running",
			Ready:     true,
		},
		{
			Namespace: "dev",
			Name:      "app-failed",
			Labels:    map[string]string{"app.kubernetes.io/name": "app"},
			Phase:     "Failed",
			Ready:     false,
		},
		{
			Namespace: "dev",
			Name:      "app-succeeded",
			Labels:    map[string]string{"app.kubernetes.io/name": "app"},
			Phase:     "Succeeded",
			Ready:     false,
		},
		{
			Namespace: "dev",
			Name:      "app-unknown",
			Labels:    map[string]string{"app.kubernetes.io/name": "app"},
			Phase:     "Unknown",
			Ready:     false,
		},
		{
			Namespace: "dev",
			Name:      "",
			Labels:    map[string]string{"app.kubernetes.io/name": "ghost"},
			Phase:     "Running",
			Ready:     true,
		},
	}
	workloads := workloadsFromReportedPods(pods)
	if len(workloads) != 1 {
		t.Fatalf("got %d workloads, want 1", len(workloads))
	}
	if workloads[0].Replicas != 1 {
		t.Fatalf("Replicas = %d, want 1 (dead/empty pods skipped)", workloads[0].Replicas)
	}
}
