package traceinvest

import (
	"strconv"
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

func healthzTrace() (*models.Trace, *tracediag.Diagnosis) {
	now := time.Now()
	client := &models.Span{
		TraceID: "t-connrefused", SpanID: "c1", Name: "GET /g/collect", ServiceName: "highping-client",
		Namespace: "highping-dev", PodName: "highping-client-abc", Kind: models.SpanKindClient,
		Status: models.SpanStatusError, DurationMs: 2.7, StartTime: now, EndTime: now.Add(3 * time.Millisecond),
		Error:  "connection refused",
		Attributes: map[string]string{
			"http.request.method":        "GET",
			"url.path":                   "/g/collect",
			"url.full":                   "http://10.233.115.249:8080/g/collect",
			"http.response.status_code": "0",
			"error.message":             "connection refused",
		},
	}
	tr := &models.Trace{
		TraceID:     client.TraceID,
		Cluster:     "crtnet-ext-k8s",
		Namespace:   client.Namespace,
		ServiceName: client.ServiceName,
		RootSpan:    client,
		Spans:       []*models.Span{client},
	}
	return tr, tracediag.Analyze(tr, tracediag.Options{})
}

func TestBuildIntentIsEvidenceNotCommand(t *testing.T) {
	tr, diag := healthzTrace()
	in, ok := BuildIntent(tr, diag, tr.Cluster, time.Now())
	if !ok {
		t.Fatal("connection refused should produce an investigation intent")
	}
	if in.InvestigationType != TypeNetworkTimeout {
		t.Fatalf("type=%s", in.InvestigationType)
	}
	if in.Namespace != "highping-dev" || in.Destination != "10.233.115.249:8080" {
		t.Fatalf("intent=%+v", in)
	}
	if in.Fingerprint == "" || in.TraceID != "t-connrefused" {
		t.Fatalf("fingerprint/trace missing: %+v", in)
	}
	want := map[string]bool{
		CheckPodStatus: true, CheckServiceResolution: true, CheckEndpointHealth: true,
		CheckEvents: true, CheckNetworkPolicy: true, CheckHTTPRequest: true,
	}
	for _, c := range in.Checks {
		if !want[c] {
			t.Fatalf("unexpected check %s", c)
		}
		delete(want, c)
	}
	if len(want) != 0 {
		t.Fatalf("missing checks %v", want)
	}
}

func TestBuildIntentSkipsInstrumentation(t *testing.T) {
	now := time.Now()
	server := &models.Span{
		TraceID: "inst", SpanID: "srv", Name: "HTTP", ServiceName: "reverse-proxy", Namespace: "highping-dev",
		Kind: models.SpanKindServer, Status: models.SpanStatusUnset, DurationMs: 253990,
		StartTime: now, EndTime: now.Add(254 * time.Second),
		Attributes: map[string]string{"http.request.method": "", "url.path": "", "http.response.status_code": "28", "otel.library.name": "go.opentelemetry.io/auto/net/http"},
	}
	client := &models.Span{
		TraceID: "inst", SpanID: "cli", ParentSpanID: "srv", Name: "OPTIONS /g/collect", ServiceName: "reverse-proxy",
		Namespace: "highping-dev", Kind: models.SpanKindClient, Status: models.SpanStatusOK, DurationMs: 25,
		StartTime: now, EndTime: now.Add(25 * time.Millisecond),
		Attributes: map[string]string{"http.request.method": "OPTIONS", "url.path": "/g/collect", "http.response.status_code": "200"},
	}
	tr := &models.Trace{TraceID: "inst", Spans: []*models.Span{server, client}, RootSpan: server}
	diag := tracediag.Analyze(tr, tracediag.Options{})
	if _, ok := BuildIntent(tr, diag, "c", now); ok {
		t.Fatal("instrumentation anomaly must not enqueue a live job")
	}
}

func TestStoreCoalescesSameFingerprint(t *testing.T) {
	s := NewStore()
	now := time.Now()
	tr, diag := healthzTrace()
	first := s.Request(tr, diag, tr.Cluster, true, now)
	if first.Status != StatusPending {
		t.Fatalf("status=%s", first.Status)
	}
	for i := 0; i < 1999; i++ {
		copyTrace := *tr
		copyTrace.TraceID = "t-extra-" + strconv.Itoa(i)
		copyDiag := *diag
		copyDiag.TraceID = copyTrace.TraceID
		rep := s.Request(&copyTrace, &copyDiag, tr.Cluster, true, now)
		if rep.Fingerprint != first.Fingerprint {
			t.Fatalf("fingerprint split: %s vs %s", first.Fingerprint, rep.Fingerprint)
		}
		if rep.Status != StatusPending {
			t.Fatalf("status=%s", rep.Status)
		}
	}
	claimed := s.Claim(tr.Cluster, 10, now)
	if len(claimed) != 1 {
		t.Fatalf("2000 traces should collapse to 1 job, got %d", len(claimed))
	}
	if claimed[0].Fingerprint != first.Fingerprint {
		t.Fatalf("claimed fingerprint %s", claimed[0].Fingerprint)
	}

	s.Submit(Result{
		Fingerprint: first.Fingerprint,
		Status:      StatusComplete,
		Inference:   "backend is likely responsible",
		Confidence:  "HIGH",
		Observations: []Observation{
			{Kind: KindObserved, Code: "http_status", Message: "HTTP 503 returned by target", OK: boolPtr(false), Level: 2},
			{Kind: KindObserved, Code: "pod_status", Message: "target pod is Ready", OK: boolPtr(true), Level: 1},
			{Kind: KindInference, Code: "backend_responsible", Message: "backend is likely responsible"},
		},
	}, now)
	got := s.Request(tr, diag, tr.Cluster, true, now)
	if got.Status != StatusComplete || !got.Cached {
		t.Fatalf("shared result: %+v", got)
	}
	if got.ReferencedBy < 2 {
		t.Fatalf("expected many traces to share the result, referencedBy=%d", got.ReferencedBy)
	}
	if got.Inference != "backend is likely responsible" || got.Confidence != "HIGH" {
		t.Fatalf("inference missing: %+v", got)
	}
	if len(got.Checks) != 2 {
		t.Fatalf("checks should project observed facts only, got %+v", got.Checks)
	}
}

func TestStoreAgentOffline(t *testing.T) {
	s := NewStore()
	tr, diag := healthzTrace()
	rep := s.Request(tr, diag, tr.Cluster, false, time.Now())
	if rep.Status != StatusUnavailable {
		t.Fatalf("status=%s", rep.Status)
	}
	if len(s.Claim(tr.Cluster, 5, time.Now())) != 0 {
		t.Fatal("offline agent must not leave a job to claim")
	}
}

func TestExpiredJobIsNotReenqueuedByPoll(t *testing.T) {
	s := NewStore()
	now := time.Now()
	tr, diag := healthzTrace()
	first := s.Request(tr, diag, tr.Cluster, true, now)
	if first.Status != StatusPending {
		t.Fatalf("status=%s", first.Status)
	}
	later := s.Request(tr, diag, tr.Cluster, true, now.Add(jobTTL+time.Second))
	if later.Status != StatusExpired {
		t.Fatalf("status=%s want expired", later.Status)
	}
	if n := len(s.Claim(tr.Cluster, 5, now.Add(jobTTL+time.Second))); n != 0 {
		t.Fatalf("expired jobs must not be claimed, got %d", n)
	}
}

func TestClaimDoesNotReissueLeasedJob(t *testing.T) {
	s := NewStore()
	now := time.Now()
	tr, diag := healthzTrace()
	s.Request(tr, diag, tr.Cluster, true, now)
	first := s.Claim(tr.Cluster, 5, now)
	second := s.Claim(tr.Cluster, 5, now)
	if len(first) != 1 || len(second) != 0 {
		t.Fatalf("lease broken: first=%d second=%d", len(first), len(second))
	}
	later := s.Claim(tr.Cluster, 5, now.Add(claimTTL+time.Second))
	if len(later) != 1 {
		t.Fatal("expired lease should be reclaimable")
	}
}

func boolPtr(v bool) *bool { return &v }
