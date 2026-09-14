package api

import (
	"crypto/subtle"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
)

const (
	ingestTokenHeader = "X-CRNET-Ingest-Token"
	agentTokenHeader  = "X-CRNET-Agent-Token"
)

// IngestToken returns the shared agent→API secret from the environment.
// Empty means auth is optional unless RequireIngestAuth is set.
func IngestToken() string {
	for _, key := range []string{"CRNET_INGEST_TOKEN", "INGEST_TOKEN", "CENTRAL_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

// RequireIngestAuth is true when CRNET_REQUIRE_INGEST_AUTH is enabled.
// When true and no token is configured, protected routes reject with 503.
func RequireIngestAuth() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("CRNET_REQUIRE_INGEST_AUTH")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func extractPresentedToken(c *fiber.Ctx) string {
	if v := strings.TrimSpace(c.Get(ingestTokenHeader)); v != "" {
		return v
	}
	if v := strings.TrimSpace(c.Get(agentTokenHeader)); v != "" {
		return v
	}
	auth := strings.TrimSpace(c.Get("Authorization"))
	if len(auth) >= 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	if v := strings.TrimSpace(c.Query("ingest_token")); v != "" {
		return v
	}
	return ""
}

func tokenMatches(expected, presented string) bool {
	if expected == "" || presented == "" {
		return false
	}
	if len(expected) != len(presented) {
		// Still compare to keep timing closer; reject.
		subtle.ConstantTimeCompare([]byte(expected), []byte(expected))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(presented)) == 1
}

// IngestAuthMiddleware protects OTLP ingest and agent control endpoints.
// Behaviour:
//   - token configured → presented token must match
//   - token empty + RequireIngestAuth → 503
//   - token empty + not required → allow (local/dev compatibility)
func IngestAuthMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		expected := IngestToken()
		if expected == "" {
			if RequireIngestAuth() {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					"error": "ingest authentication required but CRNET_INGEST_TOKEN is not configured",
					"code":  "INGEST_AUTH_NOT_CONFIGURED",
				})
			}
			return c.Next()
		}
		presented := extractPresentedToken(c)
		if !tokenMatches(expected, presented) {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid or missing ingest token",
				"code":  "INGEST_UNAUTHORIZED",
			})
		}
		return c.Next()
	}
}

// WSAuthMiddleware requires a valid JWT access token on WebSocket upgrade.
// Accepts Authorization: Bearer <jwt> or ?token=<jwt>.
func WSAuthMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := ""
		if auth := c.Get("Authorization"); auth != "" {
			if parsed, err := parseBearerToken(auth); err == nil {
				tokenString = parsed
			}
		}
		if tokenString == "" {
			tokenString = strings.TrimSpace(c.Query("token"))
		}
		if tokenString == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing websocket auth token",
				"code":  "WS_UNAUTHORIZED",
			})
		}
		token, claims, err := parseAccessToken(tokenString, 0)
		if err != nil || token == nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid or expired websocket auth token",
				"code":  "WS_UNAUTHORIZED",
			})
		}
		c.Locals("user", claims)
		return c.Next()
	}
}
