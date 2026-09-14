package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/alerts"
)

func TestAlertHandlers_CRUDAndEvaluate(t *testing.T) {
	t.Setenv("JWT_SECRET", "alert-handler-test-secret")

	store := alerts.NewMemoryStore()
	engine := alerts.NewEngine(store, alerts.NewNotifier(nil), func(ctx context.Context) ([]alerts.ServiceSample, error) {
		return []alerts.ServiceSample{{
			ServiceName: "checkout",
			Namespace:   "prod",
			ErrorRate:   9,
			P95Ms:       10,
			P99Ms:       20,
		}}, nil
	})
	h := &Handler{alerts: engine}

	tok, err := issueAccessToken("admin", "Admin", "a@b.c", "admin")
	if err != nil {
		t.Fatal(err)
	}

	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/alerts/rules", h.ListAlertRules)
	api.Post("/alerts/rules", h.UpsertAlertRule)
	api.Delete("/alerts/rules/:id", h.DeleteAlertRule)
	api.Get("/alerts/channels", h.ListAlertChannels)
	api.Post("/alerts/channels", h.UpsertAlertChannel)
	api.Get("/alerts/active", h.ListActiveAlerts)
	api.Post("/alerts/evaluate", h.EvaluateAlertsNow)

	// Create rule
	body := map[string]interface{}{
		"name": "high-errors", "metric": "Error Rate", "operator": ">", "threshold": 2,
		"active": true, "severity": "Critical", "namespace": "prod", "service": "checkout",
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/alerts/rules", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create rule status=%d body=%s", resp.StatusCode, b)
	}
	var created alerts.Rule
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("missing id")
	}

	// List rules
	req = httptest.NewRequest(http.MethodGet, "/api/alerts/rules", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("list rules %d", resp.StatusCode)
	}

	// Evaluate
	req = httptest.NewRequest(http.MethodPost, "/api/alerts/evaluate", bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("evaluate %d %s", resp.StatusCode, b)
	}
	var eval struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(b, &eval); err != nil {
		t.Fatal(err)
	}
	if eval.Count != 1 {
		t.Fatalf("count = %d body=%s", eval.Count, b)
	}

	// Active
	req = httptest.NewRequest(http.MethodGet, "/api/alerts/active", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("active %d", resp.StatusCode)
	}

	// Delete
	req = httptest.NewRequest(http.MethodDelete, "/api/alerts/rules/"+created.ID, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete %d", resp.StatusCode)
	}
}

func TestAlertHandlers_RequireAuth(t *testing.T) {
	t.Setenv("JWT_SECRET", "alert-handler-test-secret")
	h := &Handler{alerts: alerts.NewEngine(alerts.NewMemoryStore(), nil, nil)}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Get("/alerts/rules", h.ListAlertRules)
	req := httptest.NewRequest(http.MethodGet, "/api/alerts/rules", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAlertHandlers_Validation(t *testing.T) {
	t.Setenv("JWT_SECRET", "alert-handler-test-secret")
	h := &Handler{alerts: alerts.NewEngine(alerts.NewMemoryStore(), nil, nil)}
	tok, err := issueAccessToken("admin", "Admin", "a@b.c", "admin")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	api := app.Group("/api", AuthMiddleware())
	api.Post("/alerts/rules", h.UpsertAlertRule)
	req := httptest.NewRequest(http.MethodPost, "/api/alerts/rules", bytes.NewReader([]byte(`{"name":""}`)))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
