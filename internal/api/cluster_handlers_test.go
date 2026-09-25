package api

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/store"
)

func TestWorkloadsFromReportedWorkloadsMergesStatusReason(t *testing.T) {
	reported := []store.ReportedWorkload{
		{Name: "api", Namespace: "dev", Kind: "Deployment", Replicas: 1, Ready: 0},
	}
	pods := []store.ReportedPod{
		{
			Namespace:     "dev",
			Name:          "api-abc-xyz",
			Labels:        map[string]string{"app.kubernetes.io/name": "api"},
			StatusReason:  "ImagePullBackOff",
			StatusMessage: "not found",
			Ready:         false,
		},
	}
	workloads := workloadsFromReportedWorkloads(reported, pods)
	if len(workloads) != 1 {
		t.Fatalf("got %d workloads", len(workloads))
	}
	if workloads[0].StatusReason != "ImagePullBackOff" {
		t.Fatalf("StatusReason = %q, want ImagePullBackOff", workloads[0].StatusReason)
	}
	if workloads[0].StatusMessage != "not found" {
		t.Fatalf("StatusMessage = %q", workloads[0].StatusMessage)
	}
}

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

func TestWorkloadsFromReportedWorkloadsUsesControllerCounts(t *testing.T) {
	// The agent reported 3 desired / 1 ready, but only one pod made it into the
	// pod report. Pod aggregation would claim 1/1; the workload status wins.
	reported := []store.ReportedWorkload{
		{Name: "app-frontend", Namespace: "dev", Kind: "Deployment", Replicas: 3, Ready: 1},
	}
	pods := []store.ReportedPod{
		{
			Namespace:    "dev",
			Name:         "app-frontend-abc123-xyz",
			Labels:       map[string]string{"app.kubernetes.io/name": "app-frontend"},
			Language:     "nginx",
			Instrumented: true,
			Details:      "otel sidecar",
			Ready:        true,
		},
	}

	workloads := workloadsFromReportedWorkloads(reported, pods)
	if len(workloads) != 1 {
		t.Fatalf("got %d workloads, want 1", len(workloads))
	}
	w := workloads[0]
	if w.Replicas != 3 || w.Ready != 1 {
		t.Fatalf("Replicas/Ready = %d/%d, want 3/1", w.Replicas, w.Ready)
	}
	if w.Language != "nginx" {
		t.Fatalf("Language = %q, want nginx merged from reported pod", w.Language)
	}
	if !w.Instrumented {
		t.Fatal("Instrumented should be merged from reported pod")
	}
	if w.Details != "otel sidecar" {
		t.Fatalf("Details = %q, want merged from reported pod", w.Details)
	}
}

func TestWorkloadsFromReportedWorkloadsKeepsWorkloadLanguage(t *testing.T) {
	reported := []store.ReportedWorkload{
		{Name: "api", Namespace: "dev", Kind: "StatefulSet", Replicas: 2, Ready: 2, Language: "java"},
		{Name: "", Namespace: "dev", Kind: "Deployment", Replicas: 1},
	}
	pods := []store.ReportedPod{
		{Namespace: "dev", Name: "api-0", Labels: map[string]string{"app": "api"}, Language: "python"},
	}

	workloads := workloadsFromReportedWorkloads(reported, pods)
	if len(workloads) != 1 {
		t.Fatalf("got %d workloads, want 1 (unnamed workload skipped)", len(workloads))
	}
	if workloads[0].Language != "java" {
		t.Fatalf("Language = %q, want java (workload wins over pod)", workloads[0].Language)
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
