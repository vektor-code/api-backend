package store

import (
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

// A browser propagates a traceparent but exports no span of its own, so every
// span the backend stores has a parent that was never received. Such a trace
// used to build with a nil root and render as a blank row.
func TestBuildTracePicksEntrySpanWhenRootMissing(t *testing.T) {
	base := time.Now()
	spans := []*models.Span{
		{SpanID: "db1", ParentSpanID: "srv1", Name: "SELECT users", Kind: models.SpanKindClient,
			StartTime: base.Add(20 * time.Millisecond), EndTime: base.Add(30 * time.Millisecond)},
		{SpanID: "srv1", ParentSpanID: "browser-never-sent", Name: "GET api/notifications/poll",
			Kind: models.SpanKindServer, StartTime: base, EndTime: base.Add(50 * time.Millisecond)},
	}

	trace := buildTrace("t1", spans)

	if trace.RootSpan == nil {
		t.Fatal("rootless trace produced no display root")
	}
	if trace.RootSpan.SpanID != "srv1" {
		t.Errorf("entry span = %q, want the inbound SERVER span srv1", trace.RootSpan.SpanID)
	}
	if !trace.Partial {
		t.Error("trace should be marked partial when its real root is missing")
	}
	if trace.ServiceName == "" && trace.RootSpan != nil {
		// Namespace/service are taken from the chosen root.
		t.Log("service name derived from entry span")
	}
}

// A trace that does have a real root must be unaffected, and not marked partial.
func TestBuildTraceKeepsRealRoot(t *testing.T) {
	base := time.Now()
	spans := []*models.Span{
		{SpanID: "child", ParentSpanID: "root", Name: "SELECT", Kind: models.SpanKindClient,
			StartTime: base.Add(5 * time.Millisecond), EndTime: base.Add(9 * time.Millisecond)},
		{SpanID: "root", ParentSpanID: "0000000000000000", Name: "GET /orders",
			Kind: models.SpanKindServer, StartTime: base, EndTime: base.Add(10 * time.Millisecond)},
	}

	trace := buildTrace("t2", spans)

	if trace.RootSpan == nil || trace.RootSpan.SpanID != "root" {
		t.Fatalf("real root not used: %+v", trace.RootSpan)
	}
	if trace.Partial {
		t.Error("a complete trace must not be marked partial")
	}
}

// With no inbound span at all, the earliest orphan stands in.
func TestEntrySpanFallsBackToEarliestOrphan(t *testing.T) {
	base := time.Now()
	spans := []*models.Span{
		{SpanID: "b", ParentSpanID: "missing", Name: "later", Kind: models.SpanKindClient,
			StartTime: base.Add(10 * time.Millisecond)},
		{SpanID: "a", ParentSpanID: "missing", Name: "earlier", Kind: models.SpanKindClient,
			StartTime: base},
	}

	got := entrySpan(spans)
	if got == nil || got.SpanID != "a" {
		t.Errorf("entrySpan = %+v, want the earliest orphan", got)
	}
}

func TestEntrySpanEmpty(t *testing.T) {
	if got := entrySpan(nil); got != nil {
		t.Errorf("entrySpan(nil) = %+v, want nil", got)
	}
}
