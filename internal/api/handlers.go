package api

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/store"
)

// Hub manages WebSocket live-stream connections
type Hub struct {
	clients map[*websocket.Conn]string // conn -> namespace filter
	mu      sync.RWMutex
}

func newHub() *Hub {
	return &Hub{clients: make(map[*websocket.Conn]string)}
}

func (h *Hub) register(c *websocket.Conn, ns string) {
	h.mu.Lock()
	h.clients[c] = ns
	h.mu.Unlock()
}

func (h *Hub) unregister(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *Hub) broadcast(span *models.Span) {
	msg := models.LiveSpan{Type: "span", Data: span}
	data, _ := json.Marshal(msg)

	h.mu.RLock()
	defer h.mu.RUnlock()
	for conn, ns := range h.clients {
		if ns != "" && ns != span.Namespace {
			continue
		}
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("[ws] write error: %v", err)
		}
	}
}

// Handler holds all HTTP handler state
type Handler struct {
	store   *store.Store
	k8s     *k8s.Watcher
	hub     *Hub
}

// NewHandler creates the API handler
func NewHandler(s *store.Store, w *k8s.Watcher) *Handler {
	return &Handler{
		store: s,
		k8s:   w,
		hub:   newHub(),
	}
}

// OnSpan is called by the collector when a new span arrives — broadcasts live
func (h *Handler) OnSpan(span *models.Span) {
	h.hub.broadcast(span)
}

// ——— REST Handlers ———

// GET /api/health
func (h *Handler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "ok",
		"timestamp": time.Now().UTC(),
		"version":   "1.0.0",
	})
}

// GET /api/namespaces
func (h *Handler) GetNamespaces(c *fiber.Ctx) error {
	ns := h.k8s.GetNamespaces()
	return c.JSON(fiber.Map{"namespaces": ns})
}

// GET /api/stats
func (h *Handler) GetStats(c *fiber.Ctx) error {
	stats, err := h.store.GetNamespaceStats()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"namespaces": stats})
}

// GET /api/traces?namespace=&service=&hasError=&limit=&offset=
func (h *Handler) ListTraces(c *fiber.Ctx) error {
	q := &models.SearchQuery{
		Namespace:   c.Query("namespace"),
		ServiceName: c.Query("service"),
		Limit:       c.QueryInt("limit", 50),
		Offset:      c.QueryInt("offset", 0),
	}

	if c.Query("hasError") == "true" {
		t := true
		q.HasError = &t
	} else if c.Query("hasError") == "false" {
		f := false
		q.HasError = &f
	}

	if minMs := c.QueryFloat("minDuration", 0); minMs > 0 {
		q.MinDurationMs = minMs
	}
	if maxMs := c.QueryFloat("maxDuration", 0); maxMs > 0 {
		q.MaxDurationMs = maxMs
	}

	if startStr := c.Query("startTime"); startStr != "" {
		t, err := time.Parse(time.RFC3339, startStr)
		if err == nil {
			q.StartTime = t
		}
	}
	if endStr := c.Query("endTime"); endStr != "" {
		t, err := time.Parse(time.RFC3339, endStr)
		if err == nil {
			q.EndTime = t
		}
	}

	traces, err := h.store.SearchTraces(q)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"traces": traces,
		"total":  len(traces),
		"limit":  q.Limit,
		"offset": q.Offset,
	})
}

// GET /api/traces/:id
func (h *Handler) GetTrace(c *fiber.Ctx) error {
	traceID := c.Params("id")
	trace, err := h.store.GetTrace(traceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	return c.JSON(trace)
}

// GET /api/services?namespace=
func (h *Handler) GetServices(c *fiber.Ctx) error {
	ns := c.Query("namespace")
	stats, err := h.store.GetNamespaceStats()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	var services []models.ServiceStats
	for _, nsStats := range stats {
		if ns == "" || nsStats.Namespace == ns {
			services = append(services, nsStats.Services...)
		}
	}
	return c.JSON(fiber.Map{"services": services})
}

// GET /api/servicemap?namespace=
func (h *Handler) GetServiceMap(c *fiber.Ctx) error {
	ns := c.Query("namespace", "")
	data, err := h.store.GetServiceMap(ns)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(data)
}

// GET /api/pods?namespace=
func (h *Handler) GetPods(c *fiber.Ctx) error {
	ns := c.Query("namespace", "")
	pods := h.k8s.GetPodsByNamespace(ns)
	return c.JSON(fiber.Map{"pods": pods, "count": len(pods)})
}

// WS /ws — live span streaming
func (h *Handler) LiveStream(c *websocket.Conn) {
	ns := c.Query("namespace", "")
	h.hub.register(c, ns)
	defer h.hub.unregister(c)

	// Send initial ping
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"connected","message":"KubeTrace live stream ready"}`))

	// Keep alive / read loop
	for {
		_, _, err := c.ReadMessage()
		if err != nil {
			break
		}
	}
}

// GET /api/traces/:id/diagnostics
func (h *Handler) GetTraceDiagnostics(c *fiber.Ctx) error {
	traceID := c.Params("id")
	trace, err := h.store.GetTrace(traceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	report := AnalyzeTrace(trace)
	return c.JSON(report)
}

// GET /api/metrics/database
func (h *Handler) GetDatabaseMetrics(c *fiber.Ctx) error {
	namespace := c.Query("namespace", "")

	spans := h.store.GetRecentSpans(namespace)

	type key struct {
		query   string
		service string
	}
	metricsMap := make(map[key]*DatabaseQueryMetric)

	for _, span := range spans {
		dbSystem, hasSystem := span.Attributes["db.system"]
		dbStatement, hasStatement := span.Attributes["db.statement"]

		if !hasSystem && !hasStatement {
			continue
		}

		query := dbStatement
		if query == "" {
			query = span.Name
		}

		system := dbSystem
		if system == "" {
			system = "unknown"
		}

		k := key{query: query, service: span.ServiceName}
		m, ok := metricsMap[k]
		if !ok {
			m = &DatabaseQueryMetric{
				Query:     query,
				System:    system,
				Service:   span.ServiceName,
				Namespace: span.Namespace,
			}
			metricsMap[k] = m
		}

		m.CallCount++
		if span.Status == models.SpanStatusError {
			m.ErrorCount++
		}

		if span.DurationMs > m.MaxDurationMs {
			m.MaxDurationMs = span.DurationMs
		}
		m.AvgDurationMs += span.DurationMs
	}

	var result []*DatabaseQueryMetric
	for _, m := range metricsMap {
		if m.CallCount > 0 {
			m.AvgDurationMs = m.AvgDurationMs / float64(m.CallCount)
			m.ErrorRate = (float64(m.ErrorCount) / float64(m.CallCount)) * 100.0
		}
		result = append(result, m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AvgDurationMs > result[j].AvgDurationMs
	})

	return c.JSON(fiber.Map{"metrics": result})
}

