package store

import (
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestTransactionIdentityIgnoresJDBCClientRoot(t *testing.T) {
	jdbc := &models.Span{
		Name:      "dashboard_db",
		Kind:      models.SpanKindClient,
		StartTime: time.Now(),
		Attributes: map[string]string{
			"db.system": "postgresql",
			"db.name":   "dashboard_db",
		},
	}
	trace := &models.Trace{RootSpan: jdbc, Spans: []*models.Span{jdbc}}
	if traceHasTransactionIdentity(trace) {
		t.Fatal("JDBC CLIENT root must not be a transaction")
	}
	if transactionIdentitySpan(trace) != nil {
		t.Fatal("expected no transaction identity span")
	}
}

func TestTransactionIdentityPrefersHTTPServerOverClientRoot(t *testing.T) {
	now := time.Now()
	client := &models.Span{
		Name:      "dashboard_db",
		Kind:      models.SpanKindClient,
		StartTime: now,
		Attributes: map[string]string{
			"db.system": "postgresql",
		},
	}
	server := &models.Span{
		Name:      "GET /api/users",
		Kind:      models.SpanKindServer,
		StartTime: now.Add(time.Millisecond),
		Attributes: map[string]string{
			"http.request.method": "GET",
			"http.route":          "/api/users",
		},
	}
	trace := &models.Trace{RootSpan: client, Spans: []*models.Span{client, server}}
	got := transactionIdentitySpan(trace)
	if got == nil || got.Name != "GET /api/users" {
		t.Fatalf("expected HTTP SERVER identity, got %#v", got)
	}
}

func TestIsRequestSpan(t *testing.T) {
	if isRequestSpan(&models.Span{Kind: models.SpanKindClient, Attributes: map[string]string{"db.system": "postgresql"}}) {
		t.Fatal("CLIENT must not count as a request")
	}
	if !isRequestSpan(&models.Span{Kind: models.SpanKindServer, Attributes: map[string]string{"http.request.method": "GET", "url.path": "/"}}) {
		t.Fatal("HTTP SERVER must count as a request")
	}
	if isRequestSpan(&models.Span{Kind: models.SpanKindInternal}) {
		t.Fatal("INTERNAL must not count as incoming throughput")
	}
}
