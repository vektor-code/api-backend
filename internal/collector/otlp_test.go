package collector

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestResolveServiceNamePrefersWorkloadOverUnknown(t *testing.T) {
	got := resolveServiceName(map[string]string{
		"service.name":        "unknown_service:java",
		"k8s.deployment.name": "api-backend",
		"k8s.container.name":  "app",
	})
	if got != "api-backend" {
		t.Fatalf("got %q, want api-backend", got)
	}
}

func TestResolveServiceNameKeepsRealName(t *testing.T) {
	got := resolveServiceName(map[string]string{
		"service.name":        "payments",
		"k8s.deployment.name": "payments-v2",
	})
	if got != "payments" {
		t.Fatalf("got %q, want payments", got)
	}
}

func TestConvertSpanStoresTraceparentFromSpanContext(t *testing.T) {
	pb := &tracepb.Span{
		TraceId: []byte("1234567890123456"),
		SpanId:  []byte("12345678"),
		Name:    "GET /orders",
		Flags:   0x101, // trace flags populated, sampled bit set
	}
	span := convertSpan(pb, "orders", "prod", "c1", "", "")

	want := "00-31323334353637383930313233343536-3132333435363738-01"
	if got := span.Attributes["w3c.traceparent"]; got != want {
		t.Fatalf("w3c.traceparent = %q, want %q", got, want)
	}
	if got := span.Attributes["w3c.trace_flags"]; got != "01" {
		t.Fatalf("w3c.trace_flags = %q, want 01", got)
	}
}

func TestConvertSpanRecordsUnsampledTraceFlags(t *testing.T) {
	pb := &tracepb.Span{
		TraceId: []byte("1234567890123456"),
		SpanId:  []byte("12345678"),
		Flags:   0x100, // flags populated, sampled bit clear
	}
	span := convertSpan(pb, "orders", "prod", "c1", "", "")

	want := "00-31323334353637383930313233343536-3132333435363738-00"
	if got := span.Attributes["w3c.traceparent"]; got != want {
		t.Fatalf("w3c.traceparent = %q, want %q", got, want)
	}
	if got := span.Attributes["w3c.trace_flags"]; got != "00" {
		t.Fatalf("w3c.trace_flags = %q, want 00", got)
	}
}

// A span with no usable IDs must not get a syntactically invalid header stored
// that the UI would then present as copyable.
func TestConvertSpanOmitsTraceparentWhenIDsMissing(t *testing.T) {
	span := convertSpan(&tracepb.Span{Name: "internal work"}, "orders", "prod", "c1", "", "")
	if _, ok := span.Attributes["w3c.traceparent"]; ok {
		t.Fatalf("expected no w3c.traceparent, got %q", span.Attributes["w3c.traceparent"])
	}
	if _, ok := span.Attributes["w3c.trace_flags"]; ok {
		t.Fatalf("expected no w3c.trace_flags, got %q", span.Attributes["w3c.trace_flags"])
	}
}
