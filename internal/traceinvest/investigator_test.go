package traceinvest

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

type fakeCluster struct {
	mu        sync.Mutex
	pods      map[string]*k8s.PodView
	byIP      map[string]*k8s.PodView
	svcByIP   map[string]*k8s.ServiceView
	endpoints map[string]*k8s.EndpointsView
	diagPod   *k8s.PodView
	execOut   *k8s.ExecResult
	execErr   error
	gets      atomic.Int32
	execs     atomic.Int32
}

func (f *fakeCluster) GetPod(_ context.Context, ns, name string) (*k8s.PodView, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pods[ns+"/"+name], nil
}
func (f *fakeCluster) FindPodByIP(_ context.Context, _, ip string) (*k8s.PodView, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byIP[ip], nil
}
func (f *fakeCluster) FindRunningPods(_ context.Context, ns, workload string) ([]*k8s.PodView, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*k8s.PodView
	for _, p := range f.pods {
		if p.Namespace == ns && (workload == "" || p.Workload == workload) && p.Phase == "Running" {
			out = append(out, p)
		}
	}
	return out, nil
}
func (f *fakeCluster) FindServiceByIP(_ context.Context, _, ip string) (*k8s.ServiceView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.svcByIP[ip], nil
}
func (f *fakeCluster) GetEndpoints(_ context.Context, ns, service string) (*k8s.EndpointsView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.endpoints[ns+"/"+service], nil
}
func (f *fakeCluster) ListWarningEvents(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (f *fakeCluster) FindDiagnosticPod(context.Context, string) (*k8s.PodView, error) {
	return f.diagPod, nil
}
func (f *fakeCluster) Exec(context.Context, string, string, string, []string) (*k8s.ExecResult, error) {
	f.execs.Add(1)
	return f.execOut, f.execErr
}

func healthzTrace() (*models.Trace, *tracediag.Diagnosis) {
	now := time.Now()
	server := &models.Span{
		TraceID: "t-503", SpanID: "s1", Name: "GET /healthz", ServiceName: "gtm-preview",
		Namespace: "highping-dev", PodName: "gtm-preview-abc", Kind: models.SpanKindServer,
		Status: models.SpanStatusError, DurationMs: 0.4, StartTime: now, EndTime: now,
		Attributes: map[string]string{
			"http.request.method": "GET", "url.path": "/healthz",
			"url.full":                  "http://10.233.115.249:8080/healthz",
			"http.response.status_code": "503",
		},
	}
	tr := &models.Trace{TraceID: "t-503", Namespace: "highping-dev", ServiceName: "gtm-preview", RootSpan: server, Spans: []*models.Span{server}}
	return tr, tracediag.Analyze(tr, tracediag.Options{})
}

func TestInvestigateSkipsInstrumentation(t *testing.T) {
	fake := &fakeCluster{}
	inv := New(func(context.Context, *models.Trace) (Cluster, error) { return fake, nil })
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
	rep := inv.Investigate(context.Background(), tr, diag)
	if rep.Status != StatusSkipped {
		t.Fatalf("status=%s want skipped", rep.Status)
	}
	if fake.execs.Load() != 0 || fake.gets.Load() != 0 {
		t.Fatal("instrumentation anomaly must not touch Kubernetes")
	}
}

func TestInvestigateConfirms503FromExistingPod(t *testing.T) {
	pod := &k8s.PodView{Name: "gtm-preview-abc", Namespace: "highping-dev", IP: "10.233.115.249", Phase: "Running", Ready: true, Workload: "gtm-preview", Container: "gtm-preview"}
	fake := &fakeCluster{
		pods:      map[string]*k8s.PodView{"highping-dev/gtm-preview-abc": pod},
		byIP:      map[string]*k8s.PodView{"10.233.115.249": pod},
		svcByIP:   map[string]*k8s.ServiceView{"10.233.115.249": {Name: "gtm-preview", Namespace: "highping-dev", ClusterIP: "10.233.115.249"}},
		endpoints: map[string]*k8s.EndpointsView{"highping-dev/gtm-preview": {Service: "gtm-preview", Ready: []string{"10.233.115.249"}}},
		execOut:   &k8s.ExecResult{Stdout: "HTTP/1.1 503 Service Unavailable\n"},
	}
	inv := New(func(context.Context, *models.Trace) (Cluster, error) { return fake, nil })
	tr, diag := healthzTrace()
	rep := inv.Investigate(context.Background(), tr, diag)
	if rep.Status != StatusComplete && rep.Status != StatusPartial {
		t.Fatalf("status=%s %+v", rep.Status, rep)
	}
	if fake.execs.Load() != 1 {
		t.Fatalf("expected 1 exec into existing pod, got %d", fake.execs.Load())
	}
	if !strings.Contains(strings.ToLower(rep.Conclusion), "503") && !strings.Contains(strings.ToLower(rep.Conclusion), "backend") {
		t.Fatalf("conclusion=%q", rep.Conclusion)
	}
	foundProbe := false
	for _, c := range rep.Checks {
		if c.Code == "source_http_probe" && c.Level == 2 && c.OK {
			foundProbe = true
		}
	}
	if !foundProbe {
		t.Fatalf("missing level-2 probe: %+v", rep.Checks)
	}
}

func TestInvestigateCachesDestination(t *testing.T) {
	pod := &k8s.PodView{Name: "gtm-preview-abc", Namespace: "highping-dev", IP: "10.233.115.249", Phase: "Running", Ready: true, Workload: "gtm-preview", Container: "gtm-preview"}
	fake := &fakeCluster{
		pods:    map[string]*k8s.PodView{"highping-dev/gtm-preview-abc": pod},
		byIP:    map[string]*k8s.PodView{"10.233.115.249": pod},
		execOut: &k8s.ExecResult{Stdout: "HTTP/1.1 503\n"},
	}
	inv := New(func(context.Context, *models.Trace) (Cluster, error) { return fake, nil })
	tr, diag := healthzTrace()
	first := inv.Investigate(context.Background(), tr, diag)
	second := inv.Investigate(context.Background(), tr, diag)
	if fake.execs.Load() != 1 {
		t.Fatalf("cache should reuse exec, got %d", fake.execs.Load())
	}
	if !second.Cached {
		t.Fatal("second report should be cached")
	}
	if first.CacheKey == "" || first.CacheKey != second.CacheKey {
		t.Fatalf("cache keys %q %q", first.CacheKey, second.CacheKey)
	}
}

func TestInvestigateNoClusterAccess(t *testing.T) {
	inv := New(func(context.Context, *models.Trace) (Cluster, error) { return nil, nil })
	tr, diag := healthzTrace()
	rep := inv.Investigate(context.Background(), tr, diag)
	if rep.Status != StatusUnavailable {
		t.Fatalf("status=%s", rep.Status)
	}
}

func TestInvestigateUsesDiagnosticWorkerWhenNoSourcePod(t *testing.T) {
	worker := &k8s.PodView{Name: "crnet-diag-0", Namespace: "highping-dev", Phase: "Running", Ready: true, Workload: "crnet-diagnostics", Container: "diag"}
	dest := &k8s.PodView{Name: "gtm-preview-abc", Namespace: "highping-dev", IP: "10.233.115.249", Phase: "Running", Ready: true, Workload: "gtm-preview", Container: "gtm-preview"}
	fake := &fakeCluster{
		byIP:    map[string]*k8s.PodView{"10.233.115.249": dest},
		diagPod: worker,
		execOut: &k8s.ExecResult{Stdout: "HTTP/1.1 503\n"},
	}
	inv := New(func(context.Context, *models.Trace) (Cluster, error) { return fake, nil })
	tr, diag := healthzTrace()
	tr.Spans[0].PodName = "" // no source pod on the span
	rep := inv.Investigate(context.Background(), tr, diag)
	found := false
	for _, c := range rep.Checks {
		if c.Code == "source_http_probe" && c.Level == 3 && strings.Contains(c.Detail, "diagnostic worker") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected level-3 reusable worker, got %+v", rep.Checks)
	}
	if fake.execs.Load() != 1 {
		t.Fatalf("execs=%d", fake.execs.Load())
	}
}

func TestHttpProbeRejectsInjection(t *testing.T) {
	if _, err := httpProbeArgv("javascript:alert(1)"); err == nil {
		t.Fatal("non-http schemes must be rejected")
	}
	argv, err := httpProbeArgv("http://10.233.115.249:8080/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if len(argv) != 3 || argv[0] != "/bin/sh" {
		t.Fatalf("argv=%v", argv)
	}
	script := argv[2]
	if !strings.Contains(script, `"http://10.233.115.249:8080/healthz"`) {
		t.Fatalf("URL must be shell-quoted, got %s", script)
	}
	if !strings.Contains(script, "have_client") || !strings.Contains(script, "apk add --no-cache") {
		t.Fatal("probe must try existing clients before installing")
	}
	idxHave := strings.Index(script, "if have_client")
	idxApk := strings.Index(script, "apk add")
	if idxHave < 0 || idxApk < 0 || idxHave > idxApk {
		t.Fatal("existing clients must be tried before apk add")
	}
	if !strings.Contains(script, "trap cleanup EXIT") || !strings.Contains(script, "apk del") {
		t.Fatal("ephemeral wget must be removed via trap")
	}
	if strings.Contains(script, "apk upgrade") || strings.Contains(script, "apt-get upgrade") || strings.Contains(script, "apt-get update") {
		t.Fatal("probe must not upgrade packages or refresh apt indexes")
	}
}

func TestLimiterBlocksSecondDestination(t *testing.T) {
	l := newLimiter(Limits{Global: 10, Namespace: 3, Workload: 1, Destination: 1})
	release, ok := l.acquire("ns", "wl", "10.1.1.1:8080")
	if !ok {
		t.Fatal("first acquire")
	}
	if _, ok := l.acquire("ns", "wl", "10.1.1.1:8080"); ok {
		t.Fatal("same destination must be limited to 1")
	}
	release()
	if _, ok := l.acquire("ns", "wl", "10.1.1.1:8080"); !ok {
		t.Fatal("after release should allow")
	}
}
