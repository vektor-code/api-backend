package tracediag

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

var testBase = time.Date(2026, 8, 18, 4, 53, 3, 489000000, time.UTC)

func tspan(id, parent string, kind models.SpanKind, name, service string, startOffset, dur time.Duration, status models.SpanStatus, attrs map[string]string) *models.Span {
	start := testBase.Add(startOffset)
	end := start.Add(dur)
	if attrs == nil {
		attrs = map[string]string{}
	}
	return &models.Span{
		TraceID:      "test-trace",
		SpanID:       id,
		ParentSpanID: parent,
		Name:         name,
		ServiceName:  service,
		Namespace:    "highping-dev",
		StartTime:    start,
		EndTime:      end,
		DurationMs:   float64(dur) / float64(time.Millisecond),
		Status:       status,
		Kind:         kind,
		Attributes:   attrs,
	}
}

func makeTrace(id string, spans ...*models.Span) *models.Trace {
	if id == "" {
		id = "test-trace"
	}
	for _, sp := range spans {
		sp.TraceID = id
	}
	tr := &models.Trace{
		TraceID:   id,
		Spans:     spans,
		SpanCount: len(spans),
	}
	if len(spans) > 0 {
		tr.RootSpan = spans[0]
		tr.ServiceName = spans[0].ServiceName
		tr.Namespace = spans[0].Namespace
		tr.StartTime = spans[0].StartTime
		tr.EndTime = spans[0].EndTime
		tr.DurationMs = spans[0].DurationMs
		for _, sp := range spans {
			if sp.StartTime.Before(tr.StartTime) {
				tr.StartTime = sp.StartTime
			}
			if sp.EndTime.After(tr.EndTime) {
				tr.EndTime = sp.EndTime
			}
			if sp.Status == models.SpanStatusError {
				tr.HasError = true
			}
		}
		tr.DurationMs = float64(tr.EndTime.Sub(tr.StartTime)) / float64(time.Millisecond)
	}
	return tr
}

func snapshot(tr *models.Trace) []byte {
	b, err := json.Marshal(tr.Spans)
	if err != nil {
		panic(err)
	}
	return b
}

func mustAnalyze(t *testing.T, tr *models.Trace, opts Options) *Diagnosis {
	t.Helper()
	before := snapshot(tr)
	d := Analyze(tr, opts)
	after := snapshot(tr)
	if string(before) != string(after) {
		t.Fatal("Analyze mutated span data")
	}
	return d
}

func requireClass(t *testing.T, d *Diagnosis, want Classification) {
	t.Helper()
	if d == nil {
		t.Fatalf("diagnosis is nil, want %s", want)
	}
	if d.Classification != want {
		t.Fatalf("classification=%s want %s (title=%q summary=%q evidence=%v)", d.Classification, want, d.Title, d.Summary, d.Evidence)
	}
}

func hasEvidence(d *Diagnosis, code string) bool {
	if d == nil {
		return false
	}
	for _, e := range d.Evidence {
		if e.Code == code {
			return true
		}
	}
	return false
}

func TestNormalHTTP200(t *testing.T) {
	tr := makeTrace("t-200",
		tspan("s1", "", models.SpanKindServer, "GET /ok", "api", 0, 12*time.Millisecond, models.SpanStatusOK, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/ok",
			"http.response.status_code": "200",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	if d != nil {
		t.Fatalf("healthy HTTP 200 should not produce a diagnosis, got %+v", d)
	}
}

func TestLegitimateHTTP503(t *testing.T) {
	tr := makeTrace("t-503",
		tspan("s1", "", models.SpanKindServer, "GET /healthz", "gtm-preview", 0, 400*time.Microsecond, models.SpanStatusError, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/healthz",
			"url.full":                  "http://10.233.115.249:8080/healthz",
			"http.response.status_code": "503",
			"otel.library.name":         "@opentelemetry/instrumentation-http",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationApplicationError)
	if d.Confidence != ConfidenceHigh {
		t.Fatalf("confidence=%s want HIGH", d.Confidence)
	}
	if !strings.Contains(d.Title, "503") {
		t.Fatalf("title=%q want HTTP 503", d.Title)
	}
	if !strings.Contains(d.Summary, "503") || !strings.Contains(d.Summary, "/healthz") {
		t.Fatalf("summary=%q", d.Summary)
	}
	if d.Classification == ClassificationInstrumentationAnomaly {
		t.Fatal("legitimate 503 must not be instrumentation")
	}
}

func TestGTMPreviewHealthzFixture(t *testing.T) {
	tr := makeTrace("cb2662a57a58455cbd1e96188877809e",
		tspan("s1", "", models.SpanKindServer, "GET /healthz", "gtm-preview", 0, 400*time.Microsecond, models.SpanStatusError, map[string]string{
			"http.request.method":       "GET",
			"http.method":               "GET",
			"url.path":                  "/healthz",
			"url.full":                  "http://10.233.115.249:8080/healthz",
			"http.response.status_code": "503",
			"otel.library.name":         "@opentelemetry/instrumentation-http",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationApplicationError)
	if d.Confidence != ConfidenceHigh {
		t.Fatalf("confidence=%s want HIGH", d.Confidence)
	}
	if d.Title != "HTTP 503 Service Unavailable" {
		t.Fatalf("title=%q", d.Title)
	}
	enc, _ := json.MarshalIndent(d, "", "  ")
	t.Log("\n" + string(enc))
}

func TestHTTP404(t *testing.T) {
	tr := makeTrace("t-404",
		tspan("s1", "", models.SpanKindServer, "GET /missing", "api", 0, 8*time.Millisecond, models.SpanStatusError, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/missing",
			"http.response.status_code": "404",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationClientError)
	if !strings.Contains(d.Title, "404") {
		t.Fatalf("title=%q", d.Title)
	}
}

func TestClientConnectionReset(t *testing.T) {
	server := tspan("s1", "", models.SpanKindServer, "GET /reset", "reverse-proxy", 0, 9*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/reset",
		"http.response.status_code": "503",
	})
	client := tspan("c1", "s1", models.SpanKindClient, "GET /reset", "reverse-proxy", time.Millisecond, 3*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/reset",
		"http.response.status_code": "0",
		"error.message":             "connection reset by peer",
	})
	client.Error = "read: connection reset by peer"
	d := mustAnalyze(t, makeTrace("t-reset", server, client), Options{})
	requireClass(t, d, ClassificationNetworkError)
	if !hasEvidence(d, "connection_reset") && !hasEvidence(d, "client_status_zero") {
		t.Fatalf("expected transport evidence, got %+v", d.Evidence)
	}
}

func TestClientTimeout(t *testing.T) {
	client := tspan("c1", "", models.SpanKindClient, "GET /slow", "api", 0, 5*time.Second, models.SpanStatusError, map[string]string{
		"http.request.method": "GET",
		"url.path":            "/slow",
		"error.message":       "context deadline exceeded",
	})
	client.Error = "context deadline exceeded"
	d := mustAnalyze(t, makeTrace("t-timeout", client), Options{})
	requireClass(t, d, ClassificationTimeout)
}

func TestClientETIMEDOUTConnect(t *testing.T) {
	client := tspan("c1", "", models.SpanKindClient, "POST /sgtm/a", "gtm-server", 0, 338*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method": "POST",
		"url.path":            "/sgtm/a",
		"exception.message":   "ETIMEDOUT",
	})
	client.Error = "Exception: ETIMEDOUT"
	tcp := tspan("c2", "c1", models.SpanKindInternal, "tcp.connect", "gtm-server", time.Millisecond, 335*time.Millisecond, models.SpanStatusError, nil)
	tls := tspan("c3", "c1", models.SpanKindInternal, "tls.connect", "gtm-server", time.Millisecond, 335*time.Millisecond, models.SpanStatusError, nil)
	d := mustAnalyze(t, makeTrace("t-etimedout", client, tcp, tls), Options{})
	requireClass(t, d, ClassificationTimeout)
	if d.SpanTree != "complete" {
		t.Fatalf("spanTree=%q, want complete", d.SpanTree)
	}
	if !hasEvidence(d, "span_tree_complete") {
		t.Fatalf("expected span_tree_complete, got %+v", d.Evidence)
	}
	if !hasEvidence(d, "timeout_message") {
		t.Fatalf("expected timeout_message, got %+v", d.Evidence)
	}
	if !hasEvidence(d, "connect_failure") {
		t.Fatalf("expected connect_failure evidence from tcp/tls.connect, got %+v", d.Evidence)
	}
}

func TestClientStatusZeroETIMEDOUTIsTimeoutNotMissingResponse(t *testing.T) {
	client := tspan("c1", "", models.SpanKindClient, "POST /sgtm/a", "gtm-server", 0, 338*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "POST",
		"url.path":                  "/sgtm/a",
		"http.response.status_code": "0",
		"exception.message":         "ETIMEDOUT",
	})
	client.Error = "ETIMEDOUT"
	d := mustAnalyze(t, makeTrace("t-etimedout-0", client), Options{})
	requireClass(t, d, ClassificationTimeout)
	if hasEvidence(d, "missing_http_response") || hasEvidence(d, "client_status_zero") {
		t.Fatalf("timeout should own this span, got %+v", d.Evidence)
	}
}

func TestDownstream503(t *testing.T) {
	server := tspan("s1", "", models.SpanKindServer, "GET /page", "frontend", 0, 20*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/page",
		"http.response.status_code": "503",
	})
	client := tspan("c1", "s1", models.SpanKindClient, "GET /api", "frontend", time.Millisecond, 12*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/api",
		"http.response.status_code": "503",
		"peer.service":              "backend",
	})
	down := tspan("s2", "c1", models.SpanKindServer, "GET /api", "backend", 2*time.Millisecond, 8*time.Millisecond, models.SpanStatusError, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/api",
		"http.response.status_code": "503",
	})
	d := mustAnalyze(t, makeTrace("t-down", server, client, down), Options{})
	requireClass(t, d, ClassificationDownstreamError)
	if !strings.Contains(strings.ToLower(d.Summary), "downstream") {
		t.Fatalf("summary=%q", d.Summary)
	}
}

func TestEmptyMethod(t *testing.T) {
	tr := makeTrace("t-empty-method",
		tspan("s1", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 10*time.Millisecond, models.SpanStatusUnset, map[string]string{
			"http.request.method":       "",
			"url.path":                  "/g/collect",
			"http.response.status_code": "200",
			"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if !hasEvidence(d, "empty_http_method") {
		t.Fatalf("missing empty_http_method evidence: %+v", d.Evidence)
	}
}

func TestInvalidHTTPStatus(t *testing.T) {
	tr := makeTrace("t-status-28",
		tspan("s1", "", models.SpanKindServer, "GET /g/collect", "reverse-proxy", 0, 10*time.Millisecond, models.SpanStatusUnset, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/g/collect",
			"http.response.status_code": "28",
			"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if !hasEvidence(d, "invalid_http_status") {
		t.Fatalf("missing invalid_http_status: %+v", d.Evidence)
	}
}

func TestEmptyMethodAndInvalidStatus(t *testing.T) {
	tr := makeTrace("t-empty-invalid",
		tspan("s1", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 10*time.Millisecond, models.SpanStatusUnset, map[string]string{
			"http.request.method":       "",
			"url.path":                  "",
			"http.response.status_code": "28",
			"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if !hasEvidence(d, "empty_http_method") || !hasEvidence(d, "invalid_http_status") {
		t.Fatalf("evidence=%+v", d.Evidence)
	}
	if d.Confidence == ConfidenceLow {
		t.Fatalf("combined malformed metadata should not be LOW, got %s score %d", d.Confidence, d.ConfidenceScore)
	}
}

func TestLongServerShortSuccessfulClient(t *testing.T) {
	server := tspan("s1", "", models.SpanKindServer, "GET /g/collect", "reverse-proxy", 0, 254*time.Second, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/g/collect",
		"http.response.status_code": "200",
	})
	client := tspan("c1", "s1", models.SpanKindClient, "OPTIONS /g/collect", "reverse-proxy", time.Millisecond, 25*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "OPTIONS",
		"url.path":                  "/g/collect",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-long-short", server, client), Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if !hasEvidence(d, "server_longer_than_children") {
		t.Fatalf("evidence=%+v", d.Evidence)
	}
}

func TestMalformedServerSuccessfulClient(t *testing.T) {
	server := tspan("s1", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 253990*time.Millisecond, models.SpanStatusUnset, map[string]string{
		"http.request.method":       "",
		"url.path":                  "",
		"http.response.status_code": "28",
		"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		"otel.library.version":      "v0.24.0",
	})
	client := tspan("c1", "s1", models.SpanKindClient, "OPTIONS /g/collect", "reverse-proxy", time.Millisecond, 25*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "OPTIONS",
		"url.path":                  "/g/collect",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-malformed-ok-client", server, client), Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if d.Confidence != ConfidenceHigh {
		t.Fatalf("confidence=%s score=%d want HIGH", d.Confidence, d.ConfidenceScore)
	}
}

func TestProduction254sFixture(t *testing.T) {
	const id = "72e9cdd87e2e6ce3f268e5bdd39f1d95"
	server := tspan("srv", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 253990*time.Millisecond, models.SpanStatusUnset, map[string]string{
		"http.request.method":       "",
		"url.path":                  "",
		"http.response.status_code": "28",
		"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		"otel.library.version":      "v0.24.0",
	})
	client := tspan("cli", "srv", models.SpanKindClient, "OPTIONS /g/collect", "reverse-proxy", time.Millisecond, 25*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "OPTIONS",
		"url.path":                  "/g/collect",
		"http.response.status_code": "200",
	})
	tr := makeTrace(id, server, client)
	d := mustAnalyze(t, tr, Options{KnownTimeouts: map[string]time.Duration{"reverse-proxy": 30 * time.Second}})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if d.Confidence != ConfidenceHigh {
		t.Fatalf("confidence=%s score=%d", d.Confidence, d.ConfidenceScore)
	}
	if d.Title != "HTTP SERVER span lifecycle inconsistency" {
		t.Fatalf("title=%q", d.Title)
	}
	if !strings.Contains(d.Summary, "253.99s") {
		t.Fatalf("summary should mention 253.99s: %q", d.Summary)
	}
	if !strings.Contains(d.Summary, "25ms") && !strings.Contains(d.Summary, "25 ms") {
		t.Fatalf("summary should mention 25ms: %q", d.Summary)
	}
	for _, code := range []string{"empty_http_method", "empty_url_path", "invalid_http_status", "child_client_succeeded", "duration_exceeds_known_timeout"} {
		if !hasEvidence(d, code) {
			t.Fatalf("missing evidence %s in %+v", code, d.Evidence)
		}
	}
	if !hasEvidence(d, "server_longer_than_children") {
		t.Fatalf("missing lifecycle evidence: %+v", d.Evidence)
	}
	joinedCauses := strings.Join(d.LikelyCauses, " ")
	if !strings.Contains(strings.ToLower(joinedCauses), "instrumentation") {
		t.Fatalf("causes=%v", d.LikelyCauses)
	}
	if strings.Contains(strings.ToLower(joinedCauses), "ebpf is") && strings.Contains(strings.ToLower(joinedCauses), "definitive") {
		t.Fatal("must not claim eBPF is the definitive cause")
	}
	enc, _ := json.MarshalIndent(d, "", "  ")
	t.Log("\n" + string(enc))
}

func TestDuplicateSpans(t *testing.T) {
	a := tspan("c1", "s1", models.SpanKindClient, "GET /api", "api", time.Millisecond, 10*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/api",
		"http.response.status_code": "200",
	})
	b := tspan("c2", "s1", models.SpanKindClient, "GET /api", "api", time.Millisecond, 11*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/api",
		"http.response.status_code": "200",
	})
	server := tspan("s1", "", models.SpanKindServer, "GET /page", "web", 0, 20*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/page",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-dup", server, a, b), Options{})
	requireClass(t, d, ClassificationDuplicateInstrumentation)
}

func TestBrokenParentChildTiming(t *testing.T) {
	parent := tspan("p", "", models.SpanKindServer, "GET /a", "api", 0, 10*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/a",
		"http.response.status_code": "200",
	})
	child := tspan("c", "p", models.SpanKindClient, "GET /b", "api", 50*time.Millisecond, 5*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/b",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-timing", parent, child), Options{})
	requireClass(t, d, ClassificationTraceContextAnomaly)
	if !hasEvidence(d, "child_starts_after_parent_end") {
		t.Fatalf("evidence=%+v", d.Evidence)
	}
}

func TestMissingParent(t *testing.T) {
	child := tspan("c", "missing-parent", models.SpanKindServer, "GET /a", "api", 0, 8*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "GET",
		"url.path":                  "/a",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-missing-parent", child), Options{})
	requireClass(t, d, ClassificationTraceContextAnomaly)
	if d.SpanTree != "broken" {
		t.Fatalf("spanTree=%q, want broken", d.SpanTree)
	}
	if !hasEvidence(d, "missing_parent") {
		t.Fatalf("evidence=%+v", d.Evidence)
	}
}

func TestAmbiguousTrace(t *testing.T) {
	sp := tspan("s1", "", models.SpanKindInternal, "doWork", "api", 0, 3*time.Millisecond, models.SpanStatusError, nil)
	d := mustAnalyze(t, makeTrace("t-ambiguous", sp), Options{})
	requireClass(t, d, ClassificationUnknown)
	if d.Confidence != ConfidenceLow {
		t.Fatalf("confidence=%s want LOW", d.Confidence)
	}
}

func TestDoesNotInventTimeout(t *testing.T) {
	server := tspan("srv", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 253990*time.Millisecond, models.SpanStatusUnset, map[string]string{
		"http.request.method":       "",
		"url.path":                  "",
		"http.response.status_code": "28",
		"otel.library.name":         "go.opentelemetry.io/auto/net/http",
	})
	client := tspan("cli", "srv", models.SpanKindClient, "OPTIONS /g/collect", "reverse-proxy", time.Millisecond, 25*time.Millisecond, models.SpanStatusOK, map[string]string{
		"http.request.method":       "OPTIONS",
		"url.path":                  "/g/collect",
		"http.response.status_code": "200",
	})
	d := mustAnalyze(t, makeTrace("t-no-timeout", server, client), Options{})
	requireClass(t, d, ClassificationInstrumentationAnomaly)
	if hasEvidence(d, "duration_exceeds_known_timeout") {
		t.Fatal("must not assume a timeout when none was provided")
	}
}

func TestConfidenceNeverWithoutEvidence(t *testing.T) {
	tr := makeTrace("t-503",
		tspan("s1", "", models.SpanKindServer, "GET /healthz", "gtm-preview", 0, 400*time.Microsecond, models.SpanStatusError, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/healthz",
			"http.response.status_code": "503",
		}),
	)
	d := mustAnalyze(t, tr, Options{})
	if d == nil || d.ConfidenceScore == 0 || len(d.Evidence) == 0 {
		t.Fatalf("confidence must expose evidence: %+v", d)
	}
	sum := 0
	for _, e := range d.Evidence {
		sum += e.Score
	}
	if sum == 0 {
		t.Fatal("evidence scores are all zero")
	}
}
