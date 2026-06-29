package api

import (
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/kubetrace/api-backend/internal/collector"
)

// SetupRouter configures all routes on the Fiber app
func SetupRouter(app *fiber.App, h *Handler, recv *collector.Receiver) {
	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format:     "${time} ${method} ${path} ${status} ${latency}\n",
		TimeFormat: "15:04:05",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Content-Type,Authorization",
	}))

	// Health
	app.Get("/health", h.Health)
	app.Get("/ready", h.Health)

	// OTLP ingestion
	app.Post("/v1/traces", recv.HandleHTTP)
	app.Post("/api/ingest", recv.IngestJSON)

	// REST API
	api := app.Group("/api", AuthMiddleware())
	api.Get("/health", h.Health)
	
	// Auth routes
	api.Post("/auth/login", h.LoginHandler)
	api.Get("/auth/me", h.GetMeHandler)

	api.Get("/namespaces", h.GetNamespaces)
	api.Get("/stats", h.GetStats)
	api.Get("/traces", h.ListTraces)
	api.Get("/traces/:id", h.GetTrace)
	api.Get("/traces/:id/diagnostics", h.GetTraceDiagnostics)
	api.Get("/metrics/database", h.GetDatabaseMetrics)
	api.Get("/services", h.GetServices)
	api.Get("/servicemap", h.GetServiceMap)
	api.Get("/pods", h.GetPods)

	// Admin & cluster routes
	api.Get("/clusters", h.GetClusters)
	api.Get("/admin/config", h.GetAdminConfig)
	api.Get("/admin/namespaces", h.GetNamespaceStatuses)
	api.Post("/admin/namespaces/toggle", h.ToggleNamespace)

	// WebSocket (WebSocket connections bypass middleware and authenticate using standard query tokens or handshake if needed, but we keep websocket endpoint unauthenticated for live-stream connections or let it pass through)
	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/ws", websocket.New(h.LiveStream))
}
