package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

func TestIngestAuthMiddleware_AllowsWhenTokenUnset(t *testing.T) {
	t.Setenv("CRNET_INGEST_TOKEN", "")
	t.Setenv("INGEST_TOKEN", "")
	t.Setenv("CENTRAL_TOKEN", "")
	t.Setenv("CRNET_REQUIRE_INGEST_AUTH", "")

	app := fiber.New()
	app.Post("/v1/traces", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 when token unset", resp.StatusCode)
	}
}

func TestIngestAuthMiddleware_RequireWithoutToken_ServiceUnavailable(t *testing.T) {
	t.Setenv("CRNET_INGEST_TOKEN", "")
	t.Setenv("INGEST_TOKEN", "")
	t.Setenv("CENTRAL_TOKEN", "")
	t.Setenv("CRNET_REQUIRE_INGEST_AUTH", "true")

	app := fiber.New()
	app.Post("/v1/traces", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestIngestAuthMiddleware_RejectsMissingAndWrongToken(t *testing.T) {
	t.Setenv("CRNET_INGEST_TOKEN", "secret-token-value")
	t.Setenv("CRNET_REQUIRE_INGEST_AUTH", "")

	app := fiber.New()
	app.Post("/v1/traces", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	missing := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	resp, err := app.Test(missing)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", resp.StatusCode)
	}

	wrong := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	wrong.Header.Set("Authorization", "Bearer wrong-token")
	resp, err = app.Test(wrong)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", resp.StatusCode)
	}
}

func TestIngestAuthMiddleware_AcceptsBearerHeaderAndCustomHeaders(t *testing.T) {
	const token = "prod-ingest-token-abc"
	t.Setenv("CRNET_INGEST_TOKEN", token)
	t.Setenv("CRNET_REQUIRE_INGEST_AUTH", "")

	app := fiber.New()
	app.Post("/v1/traces", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	cases := []struct {
		name   string
		header string
		value  string
	}{
		{"bearer", "Authorization", "Bearer " + token},
		{"ingest header", ingestTokenHeader, token},
		{"agent header", agentTokenHeader, token},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
			req.Header.Set(tc.header, tc.value)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d body=%s", resp.StatusCode, body)
			}
		})
	}
}

func TestIngestAuthMiddleware_AcceptsQueryToken(t *testing.T) {
	const token = "query-token"
	t.Setenv("CRNET_INGEST_TOKEN", token)

	app := fiber.New()
	app.Post("/api/ingest", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/ingest?ingest_token="+token, nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestIngestAuthMiddleware_RejectsTruncatedToken(t *testing.T) {
	t.Setenv("CRNET_INGEST_TOKEN", "abcdef")

	app := fiber.New()
	app.Post("/v1/traces", IngestAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
	req.Header.Set("Authorization", "Bearer abc")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestTokenMatches_EmptyNeverMatches(t *testing.T) {
	if tokenMatches("", "") {
		t.Fatal("empty tokens must not match")
	}
	if tokenMatches("a", "") {
		t.Fatal("empty presented must not match")
	}
	if tokenMatches("", "a") {
		t.Fatal("empty expected must not match")
	}
}

func TestExtractPresentedToken_PrefersIngestHeader(t *testing.T) {
	app := fiber.New()
	var got string
	app.Get("/", func(c *fiber.Ctx) error {
		got = extractPresentedToken(c)
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(ingestTokenHeader, "from-ingest")
	req.Header.Set("Authorization", "Bearer from-bearer")
	if _, err := app.Test(req); err != nil {
		t.Fatal(err)
	}
	if got != "from-ingest" {
		t.Fatalf("got %q, want from-ingest", got)
	}
}

func TestIngestToken_EnvPrecedence(t *testing.T) {
	t.Setenv("CRNET_INGEST_TOKEN", "primary")
	t.Setenv("INGEST_TOKEN", "secondary")
	t.Setenv("CENTRAL_TOKEN", "tertiary")
	if got := IngestToken(); got != "primary" {
		t.Fatalf("got %q", got)
	}
	t.Setenv("CRNET_INGEST_TOKEN", "")
	if got := IngestToken(); got != "secondary" {
		t.Fatalf("got %q", got)
	}
	t.Setenv("INGEST_TOKEN", "")
	if got := IngestToken(); got != "tertiary" {
		t.Fatalf("got %q", got)
	}
	t.Setenv("CENTRAL_TOKEN", "  ")
	if got := IngestToken(); got != "" {
		t.Fatalf("blank should be empty, got %q", got)
	}
}

func TestRequireIngestAuth_TruthyValues(t *testing.T) {
	for _, v := range []string{"true", "TRUE", "1", "yes", "on"} {
		t.Setenv("CRNET_REQUIRE_INGEST_AUTH", v)
		if !RequireIngestAuth() {
			t.Fatalf("%q should enable require", v)
		}
	}
	t.Setenv("CRNET_REQUIRE_INGEST_AUTH", "false")
	if RequireIngestAuth() {
		t.Fatal("false should disable")
	}
}

func TestWSAuthMiddleware_RejectsMissingToken(t *testing.T) {
	t.Setenv("JWT_SECRET", "test-secret-for-ws-auth-middleware")
	app := fiber.New()
	app.Get("/ws", WSAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestWSAuthMiddleware_AcceptsValidQueryToken(t *testing.T) {
	t.Setenv("JWT_SECRET", "test-secret-for-ws-auth-middleware")
	tok, err := issueAccessToken("alice", "Alice", "alice@example.com", "admin")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	app.Get("/ws", WSAuthMiddleware(), func(c *fiber.Ctx) error {
		claims, _ := c.Locals("user").(jwt.MapClaims)
		if claims == nil {
			return c.Status(500).SendString("missing claims")
		}
		if sub, _ := claims["sub"].(string); sub != "alice" {
			return c.Status(500).SendString("bad sub")
		}
		return c.SendStatus(fiber.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/ws?token="+tok, nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
}

func TestWSAuthMiddleware_RejectsGarbageToken(t *testing.T) {
	t.Setenv("JWT_SECRET", "test-secret-for-ws-auth-middleware")
	app := fiber.New()
	app.Get("/ws", WSAuthMiddleware(), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/ws?token=not.a.jwt", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "WS_UNAUTHORIZED") {
		t.Fatalf("body = %s", body)
	}
}
