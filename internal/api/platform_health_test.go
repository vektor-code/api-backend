package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/platformhealth"
	"github.com/kubetrace/api-backend/internal/store"
)

func TestGetPlatformHealth_RequiresAdmin(t *testing.T) {
	t.Setenv("JWT_SECRET", "platform-health-test-secret")
	h := &Handler{store: &store.Store{}}
	tok, err := issueAccessToken("admin", "Admin", "a@b.c", "admin")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/admin/platform/health", h.GetPlatformHealth)

	req := httptest.NewRequest(http.MethodGet, "/api/admin/platform/health", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var report platformhealth.Report
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Notes) == 0 && len(report.Clusters) == 0 {
		t.Fatalf("expected outage note or cluster status, got %#v", report)
	}
}

func TestGetPlatformHealth_ForbiddenForViewer(t *testing.T) {
	t.Setenv("JWT_SECRET", "platform-health-test-secret")
	h := &Handler{store: &store.Store{}}
	tok, err := issueAccessToken("bob", "Bob", "b@c.d", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/admin/platform/health", h.GetPlatformHealth)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/platform/health", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("%d", resp.StatusCode)
	}
}

func TestGetPlatformHealth_UnauthorizedWithoutToken(t *testing.T) {
	t.Setenv("JWT_SECRET", "platform-health-test-secret")
	h := &Handler{store: &store.Store{}}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/admin/platform/health", h.GetPlatformHealth)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/admin/platform/health", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("%d", resp.StatusCode)
	}
}

func TestGetPlatformHealth_MergesAgentPush(t *testing.T) {
	t.Setenv("JWT_SECRET", "platform-health-test-secret")
	st := &store.Store{}
	st.SetAgentPlatformHealth("apps", &platformhealth.Report{
		Namespace: "apm-tracing",
		Components: []platformhealth.ComponentReport{{
			ID:       "agent-backend",
			PodCount: 1,
			Pods: []platformhealth.PodReport{{
				Name:       "agent-backend-xyz",
				Namespace:  "apm-tracing",
				Component:  "agent-backend",
				Status:     platformhealth.SeverityWarning,
				RecentLogs: []string{"agent warn line"},
				LogErrors:  []platformhealth.LogLine{{Severity: platformhealth.SeverityWarning, Line: "agent warn line"}},
			}},
		}},
	})
	h := &Handler{store: st}
	tok, err := issueAccessToken("admin", "Admin", "a@b.c", "admin")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/admin/platform/health", h.GetPlatformHealth)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/platform/health", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	var report platformhealth.Report
	_ = json.Unmarshal(body, &report)
	found := false
	for _, comp := range report.Components {
		for _, pod := range comp.Pods {
			if pod.Name == "agent-backend-xyz" && pod.Cluster == "apps" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("missing agent pod in %#v", report.Components)
	}
}
