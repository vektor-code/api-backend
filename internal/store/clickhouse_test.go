package store

import (
	"strings"
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestChGetTraceQueryUsesProjection(t *testing.T) {
	q := chGetTraceQuery("abc123def456abc123def456abc123de")
	if !strings.Contains(q, "optimize_use_projections = 1") {
		t.Fatalf("expected projection setting, got %s", q)
	}
	if !strings.Contains(q, "trace_id = 'abc123def456abc123def456abc123de'") {
		t.Fatalf("expected exact trace id predicate, got %s", q)
	}
}

func TestChTraceIDPredicateUsesEqualityForFullHexID(t *testing.T) {
	predicate := chTraceIDPredicate("A1234567890ABCDEF1234567890ABCDE")
	if !strings.Contains(predicate, "trace_id =") {
		t.Fatalf("expected exact trace_id predicate, got %s", predicate)
	}
	if strings.Contains(predicate, "positionCaseInsensitive") {
		t.Fatalf("did not expect substring predicate for full trace id: %s", predicate)
	}
}

func TestChTraceIDPredicateKeepsPartialSearch(t *testing.T) {
	predicate := chTraceIDPredicate("abc123")
	if !strings.Contains(predicate, "positionCaseInsensitive") {
		t.Fatalf("expected substring predicate for partial trace id, got %s", predicate)
	}
}

func TestChEndpointRootPredicateGatesMalformedHTTPServer(t *testing.T) {
	pred := chEndpointRootPredicate()
	if !strings.Contains(pred, "upperUTF8(kind) = 'SERVER'") {
		t.Fatalf("missing SERVER entrypoint gate: %s", pred)
	}
	if !strings.Contains(pred, "CONSUMER") {
		t.Fatalf("missing CONSUMER entrypoint: %s", pred)
	}
	if !strings.Contains(pred, "http.request.method") {
		t.Fatalf("missing method key: %s", pred)
	}
	expr := chAnyIfTransactionExpr("service_name")
	if !strings.Contains(expr, "countIf") || !strings.Contains(expr, "argMinIf") {
		t.Fatalf("transaction identity should prefer the earliest incoming SERVER/CONSUMER span over the trace root:\n%s", expr)
	}
	// Duration still uses the ungated root predicate.
	if strings.Contains(chRootSpanPredicate, "http.request.method") {
		t.Fatal("chRootSpanPredicate must stay duration-unrelated")
	}
}

func TestChOperationHavingUsesTransactionName(t *testing.T) {
	predicate := chOperationHaving("GET /g/collect")
	if !strings.Contains(predicate, "transaction_name") {
		t.Fatalf("expected operation predicate to use transaction_name, got %s", predicate)
	}
	if !strings.Contains(predicate, "operation_name") {
		t.Fatalf("expected operation predicate to keep operation_name fallback, got %s", predicate)
	}
	if !strings.Contains(predicate, "GET /g/collect") {
		t.Fatalf("expected operation value in predicate, got %s", predicate)
	}
}

func TestTraceMatchesOperationUsesTransactionName(t *testing.T) {
	trace := &models.Trace{
		RootSpan: &models.Span{
			Name: "GET",
			Attributes: map[string]string{
				"http.method": "GET",
				"url.path":    "/g/collect",
			},
		},
	}

	if !traceMatchesOperation(trace, "GET /g/collect") {
		t.Fatal("expected trace to match derived transaction name")
	}
	if !traceMatchesOperation(trace, "GET") {
		t.Fatal("expected trace to keep matching raw root operation")
	}
	if traceMatchesOperation(trace, "GET /orders") {
		t.Fatal("did not expect trace to match unrelated operation")
	}
}

func TestChRoleHavingExcludesProbesAndBodies(t *testing.T) {
	q := &models.SearchQuery{ExcludeProbes: true, ExcludeStreams: true}
	having := chRoleHaving(q)
	joined := strings.Join(having, " ")
	if !strings.Contains(joined, "healthz") {
		t.Fatalf("expected probe exclusion in role having:\n%s", joined)
	}
	if !strings.Contains(joined, "stream") {
		t.Fatalf("expected stream exclusion in role having:\n%s", joined)
	}
	trueVal := true
	q.HasBody = &trueVal
	q.HttpMethod = "POST"
	joined = strings.Join(chRoleHaving(q), " ")
	if !strings.Contains(joined, "http.request.body") {
		t.Fatalf("expected body filter:\n%s", joined)
	}
	if !strings.Contains(joined, "POST") {
		t.Fatalf("expected method filter:\n%s", joined)
	}
}

func TestChQueryBoundsUsesRetentionWindow(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 720}

	start, end := s.chQueryBounds(&models.SearchQuery{}, now)
	if want := now.Add(-720 * time.Hour); !start.Equal(want) {
		t.Fatalf("expected start %s, got %s", want, start)
	}
	if want := now.Add(5 * time.Minute); !end.Equal(want) {
		t.Fatalf("expected end %s, got %s", want, end)
	}
}

func TestChQueryBoundsClampsExplicitStartToRetention(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 24}

	start, _ := s.chQueryBounds(&models.SearchQuery{
		StartTime: now.Add(-72 * time.Hour),
		EndTime:   now,
	}, now)
	if want := now.Add(-24 * time.Hour); !start.Equal(want) {
		t.Fatalf("expected retained start %s, got %s", want, start)
	}
}

func TestChQueryBoundsKeepsForeverRetentionBoundedByDefault(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 0}

	start, _ := s.chQueryBounds(&models.SearchQuery{}, now)
	if want := now.Add(-time.Duration(chDefaultForeverQueryHours) * time.Hour); !start.Equal(want) {
		t.Fatalf("expected bounded default start %s, got %s", want, start)
	}
}

func TestChSpanErrorSQLIncludesHTTP500(t *testing.T) {
	sql := chSpanErrorSQL()
	if !strings.Contains(sql, "status_code = 'ERROR'") {
		t.Fatalf("expected ERROR status match: %s", sql)
	}
	if !strings.Contains(sql, ">= 500") {
		t.Fatalf("expected HTTP >= 500 match: %s", sql)
	}
	if strings.Contains(sql, ">= 400") {
		t.Fatalf("must not treat all 4xx as errors: %s", sql)
	}
}

func TestPromoteExceptionFromEvents(t *testing.T) {
	span := &models.Span{
		Status: models.SpanStatusUnset,
		Events: []models.SpanEvent{{
			Name: "exception",
			Attributes: map[string]string{
				"exception.message":    "1 validation error for MDM\nmdm_opening_status\n  Input should be a valid string [type=string_type, input_value=None]",
				"exception.stacktrace": "Traceback (most recent call last):\n  File ...",
			},
		}},
	}
	promoteExceptionFromEvents(span)
	if span.Error == "" {
		t.Fatal("expected exception.message promoted to span.Error")
	}
	if span.Attributes["exception.message"] == "" {
		t.Fatal("expected exception.message attribute")
	}
	if span.Attributes["exception.stacktrace"] == "" {
		t.Fatal("expected exception.stacktrace attribute")
	}
	if span.Status != models.SpanStatusError {
		t.Fatalf("expected ERROR status, got %s", span.Status)
	}
}

func TestRedactSecretFragments(t *testing.T) {
	in := "ok\nauthorization: Bearer secret-token\napi-key: abc\nstill ok"
	out := redactSecretFragments(in)
	if strings.Contains(out, "Bearer secret-token") || strings.Contains(out, "api-key: abc") {
		t.Fatalf("expected secrets redacted, got %q", out)
	}
	if !strings.Contains(out, "still ok") {
		t.Fatalf("expected non-secret lines kept, got %q", out)
	}
}
