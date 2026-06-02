package api

import (
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestAnalyzeTrace_RootCauseAndLatency(t *testing.T) {
	now := time.Now()

	// Recreate a call tree:
	// Root Span (Service: gateway, Duration: 100ms)
	//   ├── Child 1 (Service: auth, Duration: 10ms)
	//   └── Child 2 (Service: billing, Duration: 80ms) - Bottleneck (Self: 50ms)
	//         └── Child 2.1 (Service: db, Duration: 30ms) - Root Cause Error
	
	rootSpan := &models.Span{
		TraceID:      "trace-123",
		SpanID:       "span-root",
		ParentSpanID: "",
		Name:         "HTTP GET /checkout",
		ServiceName:  "gateway",
		Namespace:    "dev",
		StartTime:    now,
		EndTime:      now.Add(100 * time.Millisecond),
		DurationMs:   100.0,
		Status:       models.SpanStatusError,
	}

	child1 := &models.Span{
		TraceID:      "trace-123",
		SpanID:       "span-child1",
		ParentSpanID: "span-root",
		Name:         "Authorize",
		ServiceName:  "auth",
		Namespace:    "dev",
		StartTime:    now.Add(5 * time.Millisecond),
		EndTime:      now.Add(15 * time.Millisecond),
		DurationMs:   10.0,
		Status:       models.SpanStatusOK,
	}

	child2 := &models.Span{
		TraceID:      "trace-123",
		SpanID:       "span-child2",
		ParentSpanID: "span-root",
		Name:         "ProcessPayment",
		ServiceName:  "billing",
		Namespace:    "dev",
		StartTime:    now.Add(15 * time.Millisecond),
		EndTime:      now.Add(95 * time.Millisecond),
		DurationMs:   80.0,
		Status:       models.SpanStatusError,
	}

	child2_1 := &models.Span{
		TraceID:      "trace-123",
		SpanID:       "span-child2-1",
		ParentSpanID: "span-child2",
		Name:         "SELECT balance FROM users",
		ServiceName:  "db",
		Namespace:    "dev",
		StartTime:    now.Add(20 * time.Millisecond),
		EndTime:      now.Add(50 * time.Millisecond),
		DurationMs:   30.0,
		Status:       models.SpanStatusError,
		Error:        "connection refused to database host",
		Attributes: map[string]string{
			"db.system":    "postgresql",
			"db.statement": "SELECT balance FROM users",
		},
	}

	trace := &models.Trace{
		TraceID:     "trace-123",
		RootSpan:    rootSpan,
		Spans:       []*models.Span{rootSpan, child1, child2, child2_1},
		Namespace:   "dev",
		ServiceName: "gateway",
		StartTime:   now,
		EndTime:     now.Add(100 * time.Millisecond),
		DurationMs:  100.0,
		SpanCount:   4,
		HasError:    true,
	}

	report := AnalyzeTrace(trace)

	// Verify root cause span identification (should be child2_1, the deepest failing span)
	if report.RootCauseSpanID != "span-child2-1" {
		t.Errorf("Expected root cause span ID to be 'span-child2-1', got '%s'", report.RootCauseSpanID)
	}
	if report.RootCauseService != "db" {
		t.Errorf("Expected root cause service to be 'db', got '%s'", report.RootCauseService)
	}
	if report.RootCauseMessage != "connection refused to database host" {
		t.Errorf("Expected root cause message, got '%s'", report.RootCauseMessage)
	}

	// Verify latency bottleneck identification (billing duration = 80ms, child DB = 30ms, self = 50ms = 50% of trace)
	if report.BottleneckSpanID != "span-child2" {
		t.Errorf("Expected bottleneck span ID to be 'span-child2', got '%s'", report.BottleneckSpanID)
	}
	if report.BottleneckService != "billing" {
		t.Errorf("Expected bottleneck service to be 'billing', got '%s'", report.BottleneckService)
	}
	if report.BottleneckDurationMs != 50.0 {
		t.Errorf("Expected bottleneck self duration to be 50.0ms, got %f", report.BottleneckDurationMs)
	}
	if report.BottleneckPercent != 50.0 {
		t.Errorf("Expected bottleneck percent to be 50.0%%, got %f", report.BottleneckPercent)
	}
}
