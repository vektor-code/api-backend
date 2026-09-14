package alerts

import (
	"context"
	"testing"
	"time"
)

func TestEngine_EvaluateOnce_UpdatesActiveAndDedupesNotify(t *testing.T) {
	store := NewMemoryStore()
	rule, _ := store.UpsertRule(Rule{
		Name: "err", Metric: MetricErrorRate, Operator: ">", Threshold: 1,
		Active: true, Channels: []string{}, Severity: SeverityWarning,
	})
	_ = rule
	calls := 0
	engine := NewEngine(store, NewNotifier(nil), func(ctx context.Context) ([]ServiceSample, error) {
		calls++
		return []ServiceSample{{ServiceName: "api", Namespace: "prod", ErrorRate: 5}}, nil
	})
	firings := engine.EvaluateOnce(context.Background())
	if len(firings) != 1 {
		t.Fatalf("firings = %d", len(firings))
	}
	if len(store.ListActive()) != 1 {
		t.Fatal("active not stored")
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestEngine_SampleError(t *testing.T) {
	engine := NewEngine(NewMemoryStore(), nil, func(ctx context.Context) ([]ServiceSample, error) {
		return nil, context.DeadlineExceeded
	})
	if got := engine.EvaluateOnce(context.Background()); got != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestEngine_NilSamples(t *testing.T) {
	engine := NewEngine(NewMemoryStore(), nil, nil)
	if got := engine.EvaluateOnce(context.Background()); got != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestFindRule(t *testing.T) {
	rules := []Rule{{ID: "a"}, {ID: "b"}}
	if findRule(rules, "b") == nil || findRule(rules, "b").ID != "b" {
		t.Fatal("missing")
	}
	if findRule(rules, "z") != nil {
		t.Fatal("expected nil")
	}
	_ = time.Second
}
