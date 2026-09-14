package alerts

import (
	"testing"
	"time"
)

func TestEvaluateRules_FiresOnErrorRateAndLatency(t *testing.T) {
	rules := []Rule{
		{ID: "r1", Name: "err", Metric: MetricErrorRate, Operator: ">", Threshold: 2, Active: true, Severity: SeverityCritical},
		{ID: "r2", Name: "p95", Metric: MetricP95Latency, Operator: ">=", Threshold: 500, Active: true, Namespace: "prod", Service: "checkout", Severity: SeverityWarning},
		{ID: "r3", Name: "off", Metric: MetricErrorRate, Operator: ">", Threshold: 0, Active: false},
	}
	samples := []ServiceSample{
		{ServiceName: "checkout", Namespace: "prod", ErrorRate: 5, P95Ms: 600, P99Ms: 900},
		{ServiceName: "other", Namespace: "prod", ErrorRate: 0.1, P95Ms: 10, P99Ms: 20},
		{ServiceName: "checkout", Namespace: "dev", ErrorRate: 10, P95Ms: 900, P99Ms: 1000},
	}
	got := EvaluateRules(rules, samples, mustTime("2026-01-02T03:04:05Z"))
	if len(got) != 3 {
		// r1 matches all three? checkout prod err, other no, checkout dev err = 2 from r1
		// r2 matches only checkout prod = 1
		// total 3
		t.Fatalf("firings = %d want 3: %+v", len(got), got)
	}
}

func TestEvaluateRules_ServiceAllAndOperatorNormalization(t *testing.T) {
	rules := []Rule{{
		ID: "r", Name: "all", Metric: MetricP99Latency, Operator: "gt", Threshold: 100,
		Service: "all", Active: true, Severity: SeverityInfo,
	}}
	samples := []ServiceSample{{ServiceName: "a", Namespace: "n", P99Ms: 101}}
	got := EvaluateRules(rules, samples, mustTime("2026-01-02T03:04:05Z"))
	if len(got) != 1 {
		t.Fatalf("got %d", len(got))
	}
	if got[0].Condition != "p99 Latency > 100" {
		t.Fatalf("condition = %q", got[0].Condition)
	}
}

func TestEvaluateRules_NoFireBelowThreshold(t *testing.T) {
	rules := []Rule{{ID: "r", Metric: MetricErrorRate, Operator: ">", Threshold: 5, Active: true}}
	samples := []ServiceSample{{ServiceName: "a", Namespace: "n", ErrorRate: 5}}
	if got := EvaluateRules(rules, samples, mustTime("2026-01-02T03:04:05Z")); len(got) != 0 {
		t.Fatalf("strict > should not fire on equal, got %+v", got)
	}
	rules[0].Operator = ">="
	if got := EvaluateRules(rules, samples, mustTime("2026-01-02T03:04:05Z")); len(got) != 1 {
		t.Fatalf(">= should fire, got %d", len(got))
	}
}

func TestCompare_UnknownOperator(t *testing.T) {
	if Compare("==", 1, 1) {
		t.Fatal("unknown op must be false")
	}
}

func TestSampleValue_UnknownMetric(t *testing.T) {
	if _, ok := SampleValue(ServiceSample{}, Metric("CPU Usage")); ok {
		t.Fatal("CPU Usage not supported server-side yet")
	}
}

func TestRuleMatchesService(t *testing.T) {
	rule := Rule{Namespace: "prod", Service: "api"}
	if RuleMatchesService(rule, ServiceSample{Namespace: "dev", ServiceName: "api"}) {
		t.Fatal("namespace mismatch")
	}
	if !RuleMatchesService(rule, ServiceSample{Namespace: "prod", ServiceName: "api"}) {
		t.Fatal("should match")
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
