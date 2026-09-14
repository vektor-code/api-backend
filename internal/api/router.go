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
		AllowHeaders: "Content-Type,Authorization,X-CRNET-Ingest-Token,X-CRNET-Agent-Token",
	}))
	app.Use(LicenseMiddleware(h))

	// Health
	app.Get("/health", h.Health)
	app.Get("/ready", h.Health)

	// OTLP ingest + agent control — shared ingest token when configured.
	ingestAuth := IngestAuthMiddleware()
	app.Post("/v1/traces", ingestAuth, recv.HandleHTTP)
	app.Get("/v1/namespaces/config", ingestAuth, h.GetNamespaceConfig)
	app.Post("/v1/namespaces/config", ingestAuth, h.GetNamespaceConfig)
	app.Get("/v1/investigations/jobs", ingestAuth, h.ClaimInvestigationJobs)
	app.Post("/v1/investigations/results", ingestAuth, h.SubmitInvestigationResult)
	app.Post("/api/ingest", ingestAuth, recv.IngestJSON)

	// REST API
	api := app.Group("/api", AuthMiddleware())
	api.Get("/health", h.Health)

	// Auth routes
	api.Post("/auth/login", h.LoginHandler)
	api.Post("/auth/lookup", h.LookupAccountHandler)
	api.Post("/auth/refresh", h.RefreshHandler)
	api.Get("/auth/me", h.GetMeHandler)
	api.Get("/license", h.LicenseStatus)

	api.Get("/namespaces", h.GetNamespaces)
	api.Get("/stats", h.GetStats)
	api.Get("/traces", h.ListTraces)
	api.Get("/endpoints", h.ListEndpoints)
	api.Get("/traces/:id", h.GetTrace)
	api.Get("/traces/:id/diagnostics", h.GetTraceDiagnostics)
	api.Get("/traces/:id/failure-diagnosis", h.GetTraceFailureDiagnosis)
	api.Get("/traces/:id/investigation", h.GetTraceInvestigation)
	api.Get("/metrics/database", h.GetDatabaseMetrics)
	api.Get("/metrics/timeseries", h.GetTimeseries)
	api.Get("/metrics/latency-distribution", h.GetLatencyDistribution)
	api.Get("/metrics/infrastructure", h.GetInfrastructureMetrics)
	api.Get("/services", h.GetServices)
	api.Get("/servicemap", h.GetServiceMap)
	api.Get("/issues", h.GetIssues)
	api.Get("/pods", h.GetPods)

	// Alerting (server-side rules / channels / active firings)
	api.Get("/alerts/rules", h.ListAlertRules)
	api.Post("/alerts/rules", h.UpsertAlertRule)
	api.Delete("/alerts/rules/:id", h.DeleteAlertRule)
	api.Get("/alerts/channels", h.ListAlertChannels)
	api.Post("/alerts/channels", h.UpsertAlertChannel)
	api.Delete("/alerts/channels/:id", h.DeleteAlertChannel)
	api.Get("/alerts/active", h.ListActiveAlerts)
	api.Post("/alerts/evaluate", h.EvaluateAlertsNow)

	// Admin & cluster routes
	api.Get("/clusters", h.GetClusters)
	api.Get("/admin/clusters/inventory", h.GetClusterInventory)
	api.Post("/admin/clusters/inventory", h.SaveClusterInventory)
	api.Post("/admin/clusters/test", h.TestClusterConnection)
	api.Delete("/admin/clusters/:id", h.DeleteCluster)
	api.Get("/admin/clusters/:id/namespaces", h.GetClusterNamespaces)
	api.Get("/admin/clusters/:id/namespaces/:namespace/applications", h.GetClusterApplications)
	api.Get("/admin/clusters/:id/instrumentations", h.GetClusterInstrumentations)
	api.Post("/admin/applications/instrumentation/toggle", h.ToggleApplicationInstrumentation)
	api.Get("/admin/config", h.GetAdminConfig)
	api.Post("/admin/config", h.UpdateAdminConfig)
	api.Post("/admin/config/:tool/test", h.TestAdminToolConnection)
	api.Get("/admin/namespaces", h.GetNamespaceStatuses)
	api.Get("/admin/instrumentations", h.GetAdminInstrumentations)
	api.Post("/admin/namespaces/toggle", h.ToggleNamespace)
	api.Post("/admin/namespaces/add", h.AddNamespace)
	api.Post("/admin/namespaces/delete", h.DeleteNamespace)

	// Users & access control (LDAP user permissions, SonarQube-style templates)
	api.Get("/admin/users", h.GetUsers)
	api.Put("/admin/users/:username", h.SaveUser)
	api.Delete("/admin/users/:username", h.DeleteUser)
	api.Get("/admin/permission-templates", h.GetPermissionTemplates)
	api.Post("/admin/permission-templates", h.SavePermissionTemplate)
	api.Delete("/admin/permission-templates/:name", h.DeletePermissionTemplate)

	// Log retention and cleanup
	api.Get("/admin/retention", h.GetRetention)
	api.Post("/admin/retention", h.UpdateRetention)
	api.Post("/admin/retention/clear", h.ClearAllTraces)
	api.Get("/admin/platform/health", h.GetPlatformHealth)

	// WebSocket live stream — JWT required (query token= or Authorization)
	app.Use("/ws", func(c *fiber.Ctx) error {
		if !websocket.IsWebSocketUpgrade(c) {
			return fiber.ErrUpgradeRequired
		}
		return WSAuthMiddleware()(c)
	})
	app.Get("/ws", websocket.New(h.LiveStream))
}
