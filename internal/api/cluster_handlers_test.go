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
