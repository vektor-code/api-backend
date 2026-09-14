package platformhealth

import (
	"strings"
	"testing"
	"time"
)

func TestComponentIDFromPodName(t *testing.T) {
	cases := map[string]string{
		"api-backend-7d9f8c":                 "api-backend",
		"crnet-apm-agent-backend-abc":        "agent-backend",
		"ingestor-backend-0":                 "ingestor-backend",
		"app-frontend-xyz":                   "app-frontend",
		"opentelemetry-operator-controller":  "opentelemetry-operator",
		"otel-operator-xyz":                  "opentelemetry-operator",
		"redis-0":                            "",
	}
	for in, want := range cases {
		if got := ComponentIDFromPodName(in); got != want {
			t.Fatalf("%s => %q, want %q", in, got, want)
		}
	}
}

func TestClassifyLogLine(t *testing.T) {
	if ClassifyLogLine("INFO started") != "" {
		t.Fatal("info should be ignored")
	}
	if ClassifyLogLine("[error] dial tcp failed") != SeverityCritical {
		t.Fatal("error line")
	}
	if ClassifyLogLine("WARN retrying kafka") != SeverityWarning {
		t.Fatal("warn line")
	}
	if ClassifyLogLine("FATAL panic: boom") != SeverityCritical {
		t.Fatal("fatal")
	}
}

func TestScanLogLines_KeepsTailAndIssues(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("info line\n")
	}
	b.WriteString("ERROR boom\n")
	b.WriteString("WARN slow\n")
	recent, issues := ScanLogLines(b.String(), 10, 5)
	if len(recent) != 10 {
		t.Fatalf("recent=%d", len(recent))
	}
	if len(issues) != 2 {
		t.Fatalf("issues=%d", len(issues))
	}
	if issues[0].Severity != SeverityCritical || !strings.Contains(issues[0].Line, "ERROR") {
		t.Fatalf("%+v", issues[0])
	}
}

func TestIssuesFromContainer_CrashLoopAndOOM(t *testing.T) {
	code := int32(137)
	waiting := ContainerReport{Name: "app", State: "waiting", Reason: "CrashLoopBackOff", Message: "back-off"}
	issues := IssuesFromContainer(waiting)
	if RollupSeverity(issues) != SeverityCritical {
		t.Fatalf("%+v", issues)
	}
	oom := ContainerReport{Name: "app", State: "terminated", Reason: "OOMKilled", ExitCode: &code}
	issues = IssuesFromContainer(oom)
	found := false
	for _, i := range issues {
		if i.Code == "OOMKilled" {
			found = true
		}
	}
	if !found {
		t.Fatalf("%+v", issues)
	}
}

func TestIssuesFromContainer_HighRestarts(t *testing.T) {
	issues := IssuesFromContainer(ContainerReport{Name: "c", State: "running", Ready: true, RestartCount: 8})
	if RollupSeverity(issues) != SeverityWarning {
		t.Fatalf("%+v", issues)
	}
}

func TestBuildSummaryAndRollup(t *testing.T) {
	comps := []ComponentReport{
		{ID: "api-backend", Status: SeverityOK, Pods: []PodReport{{Status: SeverityOK}}},
		{ID: "agent-backend", Status: SeverityWarning, Pods: []PodReport{{Status: SeverityWarning}, {Status: SeverityCritical}}},
	}
	sum := BuildSummary(comps)
	if sum.Status != SeverityWarning {
		// component statuses: worst of components is warning, but pods include critical
		// BuildSummary uses component.Status for overall - agent is warning.
		// Wait - agent Status is Warning but has critical pod. The Collect sets Status via RollupSeverity on issues.
		// BuildSummary uses pod.Status for counts and comp.Status for overall.
	}
	if sum.Critical != 1 || sum.Warning != 1 || sum.Healthy != 1 {
		t.Fatalf("%+v", sum)
	}
	if sum.Pods != 3 {
		t.Fatalf("pods=%d", sum.Pods)
	}
	// overall from components: max(ok, warning)=warning
	if sum.Status != SeverityWarning {
		t.Fatalf("status=%s", sum.Status)
	}
}

func TestRollupSeverity_EmptyIsOK(t *testing.T) {
	if RollupSeverity(nil) != SeverityOK {
		t.Fatal()
	}
	if RollupSeverity([]Issue{{Severity: SeverityInfo}, {Severity: SeverityCritical}}) != SeverityCritical {
		t.Fatal()
	}
}

func TestDefaultNamespace(t *testing.T) {
	t.Setenv("PLATFORM_NAMESPACE", "")
	t.Setenv("POD_NAMESPACE", "")
	t.Setenv("APM_NAMESPACE", "")
	if DefaultNamespace() != "apm-tracing" {
		t.Fatal(DefaultNamespace())
	}
	t.Setenv("POD_NAMESPACE", "ns-a")
	if DefaultNamespace() != "ns-a" {
		t.Fatal(DefaultNamespace())
	}
	t.Setenv("PLATFORM_NAMESPACE", "ns-b")
	if DefaultNamespace() != "ns-b" {
		t.Fatal(DefaultNamespace())
	}
}

func TestDedupeIssues(t *testing.T) {
	in := []Issue{
		{Severity: SeverityWarning, Source: "event", Code: "BackOff", Message: "x"},
		{Severity: SeverityWarning, Source: "event", Code: "BackOff", Message: "x"},
		{Severity: SeverityCritical, Source: "log", Message: "y"},
	}
	out := dedupeIssues(in)
	if len(out) != 2 {
		t.Fatalf("%d", len(out))
	}
}

func TestItoa32(t *testing.T) {
	if itoa32(0) != "0" || itoa32(42) != "42" || itoa32(-7) != "-7" {
		t.Fatal(itoa32(0), itoa32(42), itoa32(-7))
	}
	_ = time.Now()
}
