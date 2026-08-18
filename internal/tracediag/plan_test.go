package tracediag

import (
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestLivePlanSkipsInstrumentationAnomaly(t *testing.T) {
	p := LivePlanFor(&Diagnosis{Classification: ClassificationInstrumentationAnomaly})
	if p.Recommended || p.MaxLevel != 0 {
		t.Fatalf("instrumentation must stay Level 0, got %+v", p)
	}
}

func TestLivePlanApplicationErrorAllowsExec(t *testing.T) {
	p := LivePlanFor(&Diagnosis{Classification: ClassificationApplicationError})
	if !p.Recommended || p.MaxLevel != 3 {
		t.Fatalf("application 5xx should allow Level 3 fallback, got %+v", p)
	}
}

func TestLivePlanClientErrorAPIOnly(t *testing.T) {
	p := LivePlanFor(&Diagnosis{Classification: ClassificationClientError})
	if !p.Recommended || p.MaxLevel != 1 {
		t.Fatalf("4xx should be Level 1 only, got %+v", p)
	}
}

func TestLivePlanNetworkAllowsExec(t *testing.T) {
	p := LivePlanFor(&Diagnosis{Classification: ClassificationNetworkError})
	if !p.Recommended || p.MaxLevel != 3 {
		t.Fatalf("network errors should allow Level 3 fallback, got %+v", p)
	}
}

func TestAnalyzeAttachesLivePlan(t *testing.T) {
	tr := makeTrace("t-503",
		tspan("s1", "", models.SpanKindServer, "GET /healthz", "gtm-preview", 0, 400*time.Microsecond, models.SpanStatusError, map[string]string{
			"http.request.method":       "GET",
			"url.path":                  "/healthz",
			"http.response.status_code": "503",
		}),
	)
	d := Analyze(tr, Options{})
	if d == nil || d.Live == nil {
		t.Fatal("expected live plan on diagnosis")
	}
	if !d.Live.Recommended || d.Live.MaxLevel < 2 {
		t.Fatalf("503 live plan: %+v", d.Live)
	}

	prod := makeTrace("t-inst",
		tspan("srv", "", models.SpanKindServer, "HTTP", "reverse-proxy", 0, 253990*time.Millisecond, models.SpanStatusUnset, map[string]string{
			"http.request.method":       "",
			"url.path":                  "",
			"http.response.status_code": "28",
			"otel.library.name":         "go.opentelemetry.io/auto/net/http",
		}),
		tspan("cli", "srv", models.SpanKindClient, "OPTIONS /g/collect", "reverse-proxy", time.Millisecond, 25*time.Millisecond, models.SpanStatusOK, map[string]string{
			"http.request.method":       "OPTIONS",
			"url.path":                  "/g/collect",
			"http.response.status_code": "200",
		}),
	)
	inst := Analyze(prod, Options{})
	if inst == nil || inst.Live == nil || inst.Live.Recommended || inst.Live.MaxLevel != 0 {
		t.Fatalf("instrumentation live plan: %+v", inst.Live)
	}
}
