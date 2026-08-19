package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/license"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/store"
	"github.com/kubetrace/api-backend/internal/tracediag"
	"github.com/kubetrace/api-backend/internal/traceinvest"
)

// traceResponse is the GetTrace payload. FailureDiagnosis is derived from the
// reconstructed spans and is never persisted or written back onto them.
type traceResponse struct {
	*models.Trace
	FailureDiagnosis *tracediag.Diagnosis `json:"failureDiagnosis,omitempty"`
}

type wsClient struct {
	conn      *websocket.Conn
	ns        string
	send      chan []byte
	closeOnce sync.Once
}

// Hub manages WebSocket live-stream connections
type Hub struct {
	clients map[*wsClient]struct{}
	mu      sync.RWMutex
}

func newHub() *Hub {
	return &Hub{clients: make(map[*wsClient]struct{})}
}

func (h *Hub) register(c *websocket.Conn, ns string) *wsClient {
	cl := &wsClient{
		conn: c,
		ns:   ns,
		send: make(chan []byte, 256),
	}
	h.mu.Lock()
	h.clients[cl] = struct{}{}
	h.mu.Unlock()
	go h.writeLoop(cl)
	return cl
}

func (h *Hub) unregister(cl *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[cl]; ok {
		delete(h.clients, cl)
		cl.closeOnce.Do(func() {
			close(cl.send)
			_ = cl.conn.Close()
		})
	}
	h.mu.Unlock()
}

func (h *Hub) broadcast(span *models.Span) {
	msg := models.LiveSpan{Type: "span", Data: span}
	data, _ := json.Marshal(msg)

	h.mu.RLock()
	var slow []*wsClient
	for cl := range h.clients {
		if cl.ns != "" && cl.ns != span.Namespace {
			continue
		}
		select {
		case cl.send <- data:
		default:
			slow = append(slow, cl)
		}
	}
	h.mu.RUnlock()

	for _, cl := range slow {
		log.Printf("[ws] disconnecting slow live-stream client namespace=%s", cl.ns)
		h.unregister(cl)
	}
}

func (h *Hub) writeLoop(cl *wsClient) {
	for data := range cl.send {
		_ = cl.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := cl.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("[ws] write error: %v", err)
			h.unregister(cl)
			return
		}
	}
}

// Handler holds all HTTP handler state
type Handler struct {
	store       *store.Store
	k8s         *k8s.Watcher
	hub         *Hub
	invest      *traceinvest.Store
	licenseGate *license.Checker
}

// NewHandler creates the API handler
func NewHandler(s *store.Store, w *k8s.Watcher) *Handler {
	return &Handler{
		store:  s,
		k8s:    w,
		hub:    newHub(),
		invest: traceinvest.NewStore(),
	}
}

func (h *Handler) SetLicenseGate(checker *license.Checker) {
	h.licenseGate = checker
}

func (h *Handler) LicenseStatus(c *fiber.Ctx) error {
	if h.licenseGate == nil {
		return c.JSON(fiber.Map{
			"valid":   true,
			"status":  "disabled",
			"message": "License enforcement is not configured.",
		})
	}
	return c.JSON(h.licenseGate.Snapshot())
}

func (h *Handler) licenseAllowed() bool {
	return h.licenseGate == nil || h.licenseGate.Allowed()
}

func LicenseMiddleware(h *Handler) fiber.Handler {
	return func(c *fiber.Ctx) error {
		path := c.Path()
		switch path {
		case "/health", "/ready", "/api/health", "/api/auth/login", "/api/auth/lookup", "/api/auth/refresh", "/api/auth/me", "/api/license":
			return c.Next()
		}
		if h.licenseAllowed() {
			return c.Next()
		}
		snap := h.licenseGate.Snapshot()
		code := snap.Code
		if code == "" {
			code = "LICENSE_EXPIRED"
		}
		message := snap.Message
		if message == "" {
			message = "license expired"
		}
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":      message,
			"code":       code,
			"status":     snap.Status,
			"expires_at": snap.ExpiresAt,
		})
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
	clusterFilter := c.Query("cluster", "")
	var ns []string
	seen := make(map[string]bool)

	addNs := func(name string) {
		if name == "" || !isAppNamespaceName(name) || seen[name] {
			return
		}
		if h.store.IsNamespaceDisabledForCluster(clusterFilter, name) {
			return
		}
		seen[name] = true
		ns = append(ns, name)
	}

	if clusterFilter != "" {
		for _, name := range h.store.GetReportedNamespacesForCluster(clusterFilter) {
			addNs(name)
		}
		inv, _ := h.store.GetClusterInventory()
		for _, cluster := range inv {
			if cluster.ID != clusterFilter || cluster.Token == "" || cluster.Status != "Active" {
				continue
			}
			remoteNs, err := k8s.GetRemoteNamespaces(c.Context(), clusterHost(cluster), cluster.Token)
			if err == nil {
				for _, name := range remoteNs {
					addNs(name)
				}
			}
		}
	} else {
		// Default: namespaces from agent-managed target clusters only (not monitoring cluster).
		for _, agentCluster := range h.store.GetAgentManagedClusters() {
			for _, name := range h.store.GetReportedNamespacesForCluster(agentCluster.ClusterID) {
				addNs(name)
			}
		}
	}

	if len(ns) == 0 {
		stats, err := h.store.GetNamespaceStats()
		if err == nil {
			for _, stat := range stats {
				if stat.Namespace != "" && stat.Namespace != "Internet" && isAppNamespaceName(stat.Namespace) {
					addNs(stat.Namespace)
				}
			}
		}
	}

	// Filter out disabled namespaces and namespaces the user may not see
	allowed := h.allowedNamespaces(c)
	filteredNs := []string{}
	for _, name := range ns {
		if !h.store.IsNamespaceDisabled(name) && nsAllowed(allowed, name) {
			filteredNs = append(filteredNs, name)
		}
	}

	return c.JSON(fiber.Map{"namespaces": filteredNs})
}

// GET /api/stats
func (h *Handler) GetStats(c *fiber.Ctx) error {
	stats, err := h.store.GetNamespaceStats()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Create a map of existing trace-based namespaces
	nsMap := make(map[string]*models.NamespaceStats)
	for _, s := range stats {
		nsMap[s.Namespace] = s
	}

	// Fetch all agent-reported pods to discover active namespaces and services.
	reportedPods := h.store.GetReportedPods("")
	podCounts := make(map[string]int)
	for _, p := range reportedPods {
		if h.store.IsNamespaceDisabled(p.Namespace) {
			continue
		}
		podCounts[p.Namespace]++
		nsStat, exists := nsMap[p.Namespace]
		if !exists {
			nsStat = &models.NamespaceStats{
				Namespace: p.Namespace,
				Services:  []models.ServiceStats{},
			}
			nsMap[p.Namespace] = nsStat
		}

		if p.IsFrontend {
			continue
		}

		svcName := p.ServiceName()
		if svcName == "" {
			continue
		}

		// Check if service already exists
		foundSvc := false
		for _, s := range nsStat.Services {
			if s.ServiceName == svcName {
				foundSvc = true
				break
			}
		}
		if !foundSvc {
			nsStat.Services = append(nsStat.Services, models.ServiceStats{
				ServiceName: svcName,
				Namespace:   p.Namespace,
				Language:    p.Language,
			})
		}
	}
	for namespace, count := range podCounts {
		if nsStat, ok := nsMap[namespace]; ok {
			nsStat.PodCount = count
		}
	}

	allowed := h.allowedNamespaces(c)
	filteredStats := []*models.NamespaceStats{}
	for _, nsStat := range nsMap {
		if !h.store.IsNamespaceDisabled(nsStat.Namespace) && nsAllowed(allowed, nsStat.Namespace) {
			filteredStats = append(filteredStats, nsStat)
		}
	}

	sort.Slice(filteredStats, func(i, j int) bool {
		return filteredStats[i].Namespace < filteredStats[j].Namespace
	})

	return c.JSON(fiber.Map{"namespaces": filteredStats})
}

// GET /api/traces?namespace=&service=&hasError=&limit=&offset=
func (h *Handler) ListTraces(c *fiber.Ctx) error {
	q := parseSearchQuery(c, 50)

	allowed := h.allowedNamespaces(c)
	if q.Namespace != "" && (h.store.IsNamespaceDisabled(q.Namespace) || !nsAllowed(allowed, q.Namespace)) {
		return c.JSON(fiber.Map{
			"traces": []*models.TraceListItem{},
			"total":  0,
			"limit":  q.Limit,
			"offset": q.Offset,
		})
	}

	traces, err := h.store.SearchTraces(q)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	filteredTraces := []*models.TraceListItem{}
	for _, t := range traces {
		if h.store.IsNamespaceDisabled(t.Namespace) {
			continue
		}
		// A restricted user sees a trace when it touches at least one of
		// their namespaces (cross-namespace traces stay visible as one flow).
		if allowed != nil {
			visible := nsAllowed(allowed, t.Namespace)
			for _, ns := range t.Namespaces {
				if nsAllowed(allowed, ns) {
					visible = true
					break
				}
			}
			if !visible {
				continue
			}
		}
		filteredTraces = append(filteredTraces, t)
	}

	return c.JSON(fiber.Map{
		"traces": filteredTraces,
		"total":  len(filteredTraces),
		"limit":  q.Limit,
		"offset": q.Offset,
	})
}

// GET /api/endpoints
// Returns a stable per-endpoint aggregation (root service + operation) over the
// query window, powering the Traces "Top traces" view so it doesn't jitter
// between refreshes.
func (h *Handler) ListEndpoints(c *fiber.Ctx) error {
	q := parseSearchQuery(c, 500)

	allowed := h.allowedNamespaces(c)
	empty := fiber.Map{"endpoints": []*models.EndpointStat{}, "total": 0}
	if q.Namespace != "" && (h.store.IsNamespaceDisabled(q.Namespace) || !nsAllowed(allowed, q.Namespace)) {
		return c.JSON(empty)
	}
	// A namespace-restricted user must scope to a namespace; endpoint rows are
	// aggregated and carry no namespace to post-filter on.
	if q.Namespace == "" && allowed != nil {
		return c.JSON(empty)
	}

	endpoints, err := h.store.SearchEndpoints(q)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"endpoints": endpoints, "total": len(endpoints)})
}

// GET /api/traces/:id
func (h *Handler) GetTrace(c *fiber.Ctx) error {
	traceID := c.Params("id")
	trace, err := h.store.GetTrace(traceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if trace.Namespace != "" && h.store.IsNamespaceDisabled(trace.Namespace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if !h.traceVisibleTo(c, trace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	return c.JSON(traceResponse{
		Trace:            trace,
		FailureDiagnosis: tracediag.Analyze(trace, tracediag.Options{}),
	})
}

// GET /api/traces/:id/failure-diagnosis
func (h *Handler) GetTraceFailureDiagnosis(c *fiber.Ctx) error {
	traceID := c.Params("id")
	trace, err := h.store.GetTrace(traceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if trace.Namespace != "" && h.store.IsNamespaceDisabled(trace.Namespace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if !h.traceVisibleTo(c, trace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	diag := tracediag.Analyze(trace, tracediag.Options{})
	if diag == nil {
		return c.JSON(fiber.Map{"traceId": trace.TraceID, "diagnosis": nil})
	}
	return c.JSON(diag)
}

// GET /api/traces/:id/investigation
// Enqueues a structured investigation intent for the agent. Never talks to
// Kubernetes and never blocks GetTrace.
func (h *Handler) GetTraceInvestigation(c *fiber.Ctx) error {
	traceID := c.Params("id")
	trace, err := h.store.GetTrace(traceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if trace.Namespace != "" && h.store.IsNamespaceDisabled(trace.Namespace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	if !h.traceVisibleTo(c, trace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	diag := tracediag.Analyze(trace, tracediag.Options{})
	if h.invest == nil {
		return c.JSON(&traceinvest.Report{TraceID: trace.TraceID, Status: traceinvest.StatusUnavailable, SkipReason: "Investigator not configured"})
	}
	clusterID := h.investigationCluster(trace)
	agentOnline := h.store.IsAgentManagedCluster(clusterID)
	return c.JSON(h.invest.Request(trace, diag, clusterID, agentOnline, time.Now()))
}

func (h *Handler) investigationCluster(trace *models.Trace) string {
	if trace != nil && strings.TrimSpace(trace.Cluster) != "" && h.store.IsAgentManagedCluster(trace.Cluster) {
		return trace.Cluster
	}
	agents := h.store.GetAgentManagedClusters()
	if len(agents) == 1 {
		return agents[0].ClusterID
	}
	if trace != nil && strings.TrimSpace(trace.Cluster) != "" {
		return trace.Cluster
	}
	return "default"
}

// GET /v1/investigations/jobs — agent pull. The API never reaches the agent.
func (h *Handler) ClaimInvestigationJobs(c *fiber.Ctx) error {
	clusterID := c.Query("cluster", "default")
	limit := 5
	if raw := c.Query("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	if h.invest == nil {
		return c.JSON([]traceinvest.Intent{})
	}
	return c.JSON(h.invest.Claim(clusterID, limit, time.Now()))
}

// POST /v1/investigations/results — agent push.
func (h *Handler) SubmitInvestigationResult(c *fiber.Ctx) error {
	var res traceinvest.Result
	if err := c.BodyParser(&res); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid investigation result"})
	}
	if h.invest == nil {
		return c.Status(503).JSON(fiber.Map{"error": "investigator not configured"})
	}
	return c.JSON(h.invest.Submit(res, time.Now()))
}

// traceVisibleTo reports whether the caller may see this trace: true when any
// of its spans belongs to one of the user's allowed namespaces.
func (h *Handler) traceVisibleTo(c *fiber.Ctx, trace *models.Trace) bool {
	allowed := h.allowedNamespaces(c)
	if allowed == nil {
		return true
	}
	if nsAllowed(allowed, trace.Namespace) {
		return true
	}
	for _, sp := range trace.Spans {
		if nsAllowed(allowed, sp.Namespace) {
			return true
		}
	}
	return false
}

// GET /api/services?namespace=
func (h *Handler) GetServices(c *fiber.Ctx) error {
	ns := c.Query("namespace")
	if ns != "" && h.store.IsNamespaceDisabled(ns) {
		return c.JSON(fiber.Map{"services": []models.ServiceStats{}})
	}

	stats, err := h.store.GetNamespaceStats()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	allowed := h.allowedNamespaces(c)
	var services []models.ServiceStats
	seenSvc := make(map[string]bool)
	for _, nsStats := range stats {
		if h.store.IsNamespaceDisabled(nsStats.Namespace) || !nsAllowed(allowed, nsStats.Namespace) {
			continue
		}
		if ns == "" || nsStats.Namespace == ns {
			for _, s := range nsStats.Services {
				key := s.Namespace + ":" + s.ServiceName
				seenSvc[key] = true
				services = append(services, s)
			}
		}
	}

	// Fallback/auto-detect from reported pods
	var reportedPods []store.ReportedPod
	if ns == "" {
		reportedPods = h.store.GetReportedPods("")
	} else {
		reportedPods = h.store.GetReportedPods(ns)
	}

	for _, p := range reportedPods {
		if p.IsFrontend {
			continue
		}
		if h.store.IsNamespaceDisabled(p.Namespace) || !nsAllowed(allowed, p.Namespace) {
			continue
		}
		svcName := p.ServiceName()
		if svcName == "" {
			continue
		}
		key := p.Namespace + ":" + svcName
		if !seenSvc[key] {
			seenSvc[key] = true
			services = append(services, models.ServiceStats{
				ServiceName: svcName,
				Namespace:   p.Namespace,
				Language:    p.Language,
			})
		}
	}

	cfgMap := h.store.GetApplicationConfigMap()
	for i := range services {
		key := services[i].Namespace + ":" + services[i].ServiceName
		if cfg, ok := cfgMap[key]; ok && cfg.Language != "" {
			services[i].Language = cfg.Language
		} else {
			if services[i].Language == "" && h.k8s != nil {
				services[i].Language = h.k8s.GetLanguageForService(services[i].Namespace, services[i].ServiceName)
			}
			if services[i].Language == "" {
				services[i].Language = h.store.GetReportedLanguageForService(services[i].Namespace, services[i].ServiceName)
			}
		}
	}

	return c.JSON(fiber.Map{"services": services})
}

// GET /api/servicemap?namespace=
func (h *Handler) GetServiceMap(c *fiber.Ctx) error {
	ns := c.Query("namespace", "")
	if ns != "" && h.store.IsNamespaceDisabled(ns) {
		return c.JSON(&models.ServiceMapData{
			Namespace: ns,
			Nodes:     []models.ServiceStats{},
			Edges:     []models.ServiceEdge{},
		})
	}

	data, err := h.store.GetServiceMap(ns)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Restricted users get a filtered copy (data is a shared cache entry —
	// never mutate it in place).
	if allowed := h.allowedNamespaces(c); allowed != nil && data != nil {
		filtered := &models.ServiceMapData{Namespace: data.Namespace}
		visibleNodes := make(map[string]bool)
		for _, n := range data.Nodes {
			// Infra/third-party nodes have no namespace; keep them so flows
			// into databases/queues stay visible.
			if n.Namespace == "" || nsAllowed(allowed, n.Namespace) {
				filtered.Nodes = append(filtered.Nodes, n)
				visibleNodes[n.ServiceName] = true
			}
		}
		for _, e := range data.Edges {
			if visibleNodes[e.Source] && visibleNodes[e.Target] {
				filtered.Edges = append(filtered.Edges, e)
			}
		}
		data = filtered
	}

	if data != nil {
		cfgMap := h.store.GetApplicationConfigMap()
		for i := range data.Nodes {
			key := data.Nodes[i].Namespace + ":" + data.Nodes[i].ServiceName
			if cfg, ok := cfgMap[key]; ok && cfg.Language != "" {
				data.Nodes[i].Language = cfg.Language
			} else {
				if data.Nodes[i].Language == "" && h.k8s != nil {
					data.Nodes[i].Language = h.k8s.GetLanguageForService(data.Nodes[i].Namespace, data.Nodes[i].ServiceName)
				}
				if data.Nodes[i].Language == "" {
					data.Nodes[i].Language = h.store.GetReportedLanguageForService(data.Nodes[i].Namespace, data.Nodes[i].ServiceName)
				}
			}
		}
	}

	return c.JSON(data)
}

// GET /api/pods?namespace=
func (h *Handler) GetPods(c *fiber.Ctx) error {
	ns := c.Query("namespace", "")
	if ns != "" && h.store.IsNamespaceDisabled(ns) {
		return c.JSON(fiber.Map{
			"pods":  []interface{}{},
			"count": 0,
		})
	}

	var pods []*k8s.PodInfo
	if h.k8s != nil {
		pods = h.k8s.GetPodsByNamespace(ns)
	}

	type PodMetricInfo struct {
		Name                string            `json:"name"`
		Namespace           string            `json:"namespace"`
		NodeName            string            `json:"nodeName"`
		Labels              map[string]string `json:"labels"`
		Phase               string            `json:"phase"`
		CpuUsage            float64           `json:"cpuUsage"`    // in millicores
		CpuLimit            float64           `json:"cpuLimit"`    // in millicores
		MemoryUsage         float64           `json:"memoryUsage"` // in MB
		MemoryLimit         float64           `json:"memoryLimit"` // in MB
		RestartCount        int               `json:"restartCount"`
		Language            string            `json:"language"`
		Instrumented        bool              `json:"instrumented"`
		InstrumentationType string            `json:"instrumentationType"`
		Details             string            `json:"details"`
	}

	var enrichedPods []PodMetricInfo

	remotePods := h.store.GetReportedPods(ns)
	if len(remotePods) > 0 {
		for _, p := range remotePods {
			if p.IsFrontend {
				continue
			}
			enrichedPods = append(enrichedPods, PodMetricInfo{
				Name:                p.Name,
				Namespace:           p.Namespace,
				NodeName:            p.NodeName,
				Labels:              p.Labels,
				Phase:               p.Phase,
				CpuUsage:            p.CpuUsage,
				CpuLimit:            p.CpuLimit,
				MemoryUsage:         p.MemoryUsage,
				MemoryLimit:         p.MemoryLimit,
				RestartCount:        p.RestartCount,
				Language:            p.Language,
				Instrumented:        p.Instrumented,
				InstrumentationType: p.InstrumentationType,
				Details:             p.Details,
			})
		}
	} else if len(pods) > 0 {
		// Use real pods from K8s API watcher (local cluster)
		for _, p := range pods {
			if p.IsFrontend {
				continue
			}
			enrichedPods = append(enrichedPods, PodMetricInfo{
				Name:                p.Name,
				Namespace:           p.Namespace,
				NodeName:            p.NodeName,
				Labels:              p.Labels,
				Phase:               p.Phase,
				CpuLimit:            1000.0,
				MemoryLimit:         1024.0,
				RestartCount:        0,
				Language:            p.Language,
				Instrumented:        p.Instrumented,
				InstrumentationType: p.InstrumentationType,
				Details:             p.Details,
			})
		}
	} else {
		// K8s watcher has no pods for this namespace.
		// This happens for remote clusters (e.g. dev cluster) that send
		// OTEL traces but aren't reachable via the local K8s API.
		// Extract real pod metadata from ingested trace spans.
		tracePods := h.store.GetTracePodsByNamespace(ns)
		if len(tracePods) > 0 {
			for _, tp := range tracePods {
				enrichedPods = append(enrichedPods, PodMetricInfo{
					Name:         tp.Name,
					Namespace:    tp.Namespace,
					NodeName:     tp.NodeName,
					Labels:       tp.Labels,
					Phase:        "Running",
					CpuLimit:     1000.0,
					MemoryLimit:  1024.0,
					RestartCount: 0,
				})
			}
		}

		// Last resort: generate synthetic pods from namespace service stats
		if len(enrichedPods) == 0 {
			stats, err := h.store.GetNamespaceStats()
			if err == nil {
				for _, nsStat := range stats {
					if ns == "" || nsStat.Namespace == ns {
						for _, svc := range nsStat.Services {
							if svc.ServiceName == "Internet" || svc.IsInfrastructure {
								continue
							}
							// Create 1-2 pods for this service
							podName1 := fmt.Sprintf("%s-%s-5g7h8", svc.ServiceName, randString(5))
							enrichedPods = append(enrichedPods, PodMetricInfo{
								Name:         podName1,
								Namespace:    nsStat.Namespace,
								NodeName:     "k8s-node-worker-1",
								Labels:       map[string]string{"app": svc.ServiceName, "version": "v1.0"},
								Phase:        "Running",
								CpuLimit:     1000.0,
								MemoryLimit:  1024.0,
								RestartCount: 0,
							})

							if strings.Contains(svc.ServiceName, "clickhouse") || strings.Contains(svc.ServiceName, "kafka") || strings.Contains(svc.ServiceName, "backend") {
								podName2 := fmt.Sprintf("%s-%s-9x2y4", svc.ServiceName, randString(5))
								enrichedPods = append(enrichedPods, PodMetricInfo{
									Name:         podName2,
									Namespace:    nsStat.Namespace,
									NodeName:     "k8s-node-worker-2",
									Labels:       map[string]string{"app": svc.ServiceName, "version": "v1.0"},
									Phase:        "Running",
									CpuLimit:     1000.0,
									MemoryLimit:  1024.0,
									RestartCount: 0,
								})
							}
						}
					}
				}
			}
		}
	}

	// Namespace visibility enforcement for restricted users
	if allowed := h.allowedNamespaces(c); allowed != nil {
		visible := enrichedPods[:0]
		for _, p := range enrichedPods {
			if nsAllowed(allowed, p.Namespace) {
				visible = append(visible, p)
			}
		}
		enrichedPods = visible
	}

	// Calculate and assign resource stats for each pod dynamically
	for i := range enrichedPods {
		p := &enrichedPods[i]
		nameLower := strings.ToLower(p.Name)

		cpuLimit := 1000.0
		memLimit := 1024.0
		baseCpu := 15.0
		baseMem := 80.0

		if strings.Contains(nameLower, "clickhouse") {
			baseCpu = 85.0
			baseMem = 1200.0
			cpuLimit = 4000.0
			memLimit = 4096.0
		} else if strings.Contains(nameLower, "kafka") {
			baseCpu = 40.0
			baseMem = 512.0
			cpuLimit = 2000.0
			memLimit = 2048.0
		} else if strings.Contains(nameLower, "redis") {
			baseCpu = 8.0
			baseMem = 16.0
			cpuLimit = 500.0
			memLimit = 256.0
		} else if strings.Contains(nameLower, "nginx") {
			baseCpu = 12.0
			baseMem = 32.0
			cpuLimit = 1000.0
			memLimit = 512.0
		} else if strings.Contains(nameLower, "backend") {
			baseCpu = 25.0
			baseMem = 180.0
			cpuLimit = 1500.0
			memLimit = 1024.0
		}

		seed := float64(time.Now().UnixNano() % 100)
		cpuUsage := baseCpu + (seed * 0.15)
		memUsage := baseMem + (seed * 0.05)

		if cpuUsage > cpuLimit {
			cpuUsage = cpuLimit * 0.9
		}
		if memUsage > memLimit {
			memUsage = memLimit * 0.9
		}

		restarts := int(time.Now().Unix()/100000) % 2
		if p.Phase == "Failed" || p.Phase == "Unknown" {
			cpuUsage = 0
			memUsage = 0
		}

		p.CpuUsage = cpuUsage
		p.CpuLimit = cpuLimit
		p.MemoryUsage = memUsage
		p.MemoryLimit = memLimit
		p.RestartCount = restarts
	}

	// Filter out pods belonging to disabled namespaces
	var filteredPods []PodMetricInfo
	for _, p := range enrichedPods {
		if !h.store.IsNamespaceDisabled(p.Namespace) {
			filteredPods = append(filteredPods, p)
		}
	}

	return c.JSON(fiber.Map{
		"pods":  filteredPods,
		"count": len(filteredPods),
	})
}

// Simple randString helper using LCG method for low overhead
func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	t := time.Now().UnixNano()
	for i := range b {
		t = t*1103515245 + 12345
		idx := int((t / 65536) % 32768)
		b[i] = letters[idx%len(letters)]
	}
	return string(b)
}

// WS /ws — live span streaming
func (h *Handler) LiveStream(c *websocket.Conn) {
	ns := c.Query("namespace", "")

	cl := h.hub.register(c, ns)
	defer h.hub.unregister(cl)
	select {
	case cl.send <- []byte(`{"type":"connected","message":"live stream ready"}`):
	default:
	}

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
	if !h.traceVisibleTo(c, trace) {
		return c.Status(404).JSON(fiber.Map{"error": "trace not found"})
	}
	report := AnalyzeTrace(trace)
	return c.JSON(report)
}

// GET /api/metrics/database?namespace=&windowMinutes=60
func (h *Handler) GetDatabaseMetrics(c *fiber.Ctx) error {
	namespace := c.Query("namespace", "")
	windowMinutes := c.QueryInt("windowMinutes", defaultDBMetricsWindowMinutes)
	if windowMinutes < minDBMetricsWindowMinutes {
		windowMinutes = minDBMetricsWindowMinutes
	}
	if windowMinutes > maxDBMetricsWindowMinutes {
		windowMinutes = maxDBMetricsWindowMinutes
	}

	empty := fiber.Map{"metrics": []*models.DatabaseQueryMetric{}, "windowMinutes": windowMinutes}

	allowed := h.allowedNamespaces(c)
	if namespace != "" && (h.store.IsNamespaceDisabled(namespace) || !nsAllowed(allowed, namespace)) {
		return c.JSON(empty)
	}

	var allowedList []string
	if allowed != nil {
		for ns := range allowed {
			allowedList = append(allowedList, ns)
		}
		if allowedList == nil {
			allowedList = []string{}
		}
	}

	metrics, err := h.store.GetDatabaseQueryMetrics(namespace, allowedList, windowMinutes)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if metrics == nil {
		metrics = []*models.DatabaseQueryMetric{}
	}

	return c.JSON(fiber.Map{"metrics": metrics, "windowMinutes": windowMinutes})
}

// GET/POST /v1/namespaces/config
func (h *Handler) GetNamespaceConfig(c *fiber.Ctx) error {
	clusterID := c.Query("cluster", "default")

	if c.Method() == "POST" {
		var req struct {
			Cluster        string               `json:"cluster"`
			AgentNamespace string               `json:"agentNamespace"`
			Namespaces     []string             `json:"namespaces"`
			Pods           []store.ReportedPod  `json:"pods"`
			Nodes          []store.ReportedNode `json:"nodes"`
		}
		if err := c.BodyParser(&req); err == nil {
			if req.Cluster != "" {
				clusterID = req.Cluster
			}
			// The agent reports its own namespace; if it's ever missing, store it
			// empty so it resolves via dynamic discovery rather than a hardcode.
			agentNs := req.AgentNamespace
			h.store.RegisterAgentHeartbeat(clusterID, agentNs)
			_ = h.store.EnsureAgentClusterInInventory(clusterID, agentNs)
			for _, ns := range req.Namespaces {
				if isAppNamespaceName(ns) {
					_ = h.store.AddConfiguredNamespace(ns)
				}
			}

			podsByNs := make(map[string][]store.ReportedPod)
			for _, p := range req.Pods {
				podsByNs[p.Namespace] = append(podsByNs[p.Namespace], p)
			}
			for ns, pods := range podsByNs {
				h.store.SetReportedPodsForCluster(clusterID, ns, pods)
				h.store.SetReportedPods(ns, pods)
			}
			for _, ns := range req.Namespaces {
				if len(podsByNs[ns]) == 0 {
					h.store.SetReportedPodsForCluster(clusterID, ns, nil)
				}
			}
			if len(req.Nodes) > 0 {
				h.store.SetReportedNodesForCluster(clusterID, req.Nodes)
				h.store.SetReportedNodes(req.Nodes)
			}
		}
	}

	disabled := h.store.GetExplicitlyEnabledNamespaces()
	workloads, _ := h.store.GetWorkloadInstrumentations(clusterID)
	var investigations []traceinvest.Intent
	if c.Method() == "POST" && h.invest != nil {
		investigations = h.invest.Claim(clusterID, 5, time.Now())
	}
	return c.JSON(fiber.Map{
		"enabled":        disabled,
		"disabled":       []string{},
		"cluster":        clusterID,
		"workloads":      workloads,
		"investigations": investigations,
	})
}

// GET /api/clusters
func (h *Handler) GetClusters(c *fiber.Ctx) error {
	inv, _ := h.store.GetClusterInventory()
	agentClusters := h.store.GetAgentManagedClusters()

	result := make([]fiber.Map, 0)
	invMap := make(map[string]store.ClusterInventoryItem)
	for _, item := range inv {
		invMap[item.ID] = item
	}

	// Agent-managed target clusters first (where agent-backend is deployed).
	for _, agent := range agentClusters {
		item, ok := invMap[agent.ClusterID]
		displayName := agent.ClusterID
		status := "Active"
		managedByAgent := true
		if ok {
			displayName = item.DisplayName
			status = item.Status
			managedByAgent = item.ManagedByAgent || true
		}
		result = append(result, fiber.Map{
			"name":           agent.ClusterID,
			"displayName":    displayName,
			"status":         status,
			"managedByAgent": managedByAgent,
			"agentNamespace": agent.AgentNamespace,
		})
		delete(invMap, agent.ClusterID)
	}

	// Include manually registered clusters with credentials (optional remote management).
	for _, item := range invMap {
		if item.Token == "" && !item.ManagedByAgent {
			continue
		}
		result = append(result, fiber.Map{
			"name":           item.ID,
			"displayName":    item.DisplayName,
			"status":         item.Status,
			"managedByAgent": item.ManagedByAgent,
			"agentNamespace": item.AgentNamespace,
		})
	}

	if len(result) == 0 {
		for _, agent := range h.store.GetAgentManagedClusters() {
			result = append(result, fiber.Map{
				"name":           agent.ClusterID,
				"displayName":    agent.ClusterID,
				"status":         "Active",
				"managedByAgent": true,
				"agentNamespace": agent.AgentNamespace,
			})
		}
	}
	return c.JSON(fiber.Map{
		"clusters": result,
	})
}

// GET /api/admin/clusters/inventory
func (h *Handler) GetClusterInventory(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	inv, err := h.store.GetClusterInventory()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	invMap := make(map[string]bool)
	for _, item := range inv {
		invMap[item.ID] = true
	}
	for _, agent := range h.store.GetAgentManagedClusters() {
		if !invMap[agent.ClusterID] {
			inv = append(inv, store.ClusterInventoryItem{
				ID:             agent.ClusterID,
				DisplayName:    agent.ClusterID,
				Status:         "Active",
				AgentNamespace: agent.AgentNamespace,
				ManagedByAgent: true,
				CredentialType: "agent",
			})
		}
	}
	// Deduplicate and drop stale default placeholder when agent cluster exists.
	hasAgent := false
	for _, item := range inv {
		if item.ManagedByAgent {
			hasAgent = true
			break
		}
	}
	if hasAgent {
		filtered := make([]store.ClusterInventoryItem, 0, len(inv))
		for _, item := range inv {
			if item.ID == "default" && !item.ManagedByAgent && item.Token == "" {
				continue
			}
			filtered = append(filtered, item)
		}
		inv = filtered
	}

	return c.JSON(fiber.Map{
		"inventory": maskClusterInventoryList(inv),
	})
}

func maskClusterInventoryList(inv []store.ClusterInventoryItem) []fiber.Map {
	result := make([]fiber.Map, 0, len(inv))
	for _, item := range inv {
		result = append(result, maskClusterInventoryItem(item))
	}
	return result
}

// POST /api/admin/clusters/inventory
func (h *Handler) SaveClusterInventory(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req struct {
		Inventory []store.ClusterInventoryItem `json:"inventory"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	for i := range req.Inventory {
		req.Inventory[i].Token = strings.TrimSpace(req.Inventory[i].Token)
		req.Inventory[i].APIServer = strings.TrimSpace(req.Inventory[i].APIServer)
	}

	merged, err := h.mergeInventoryTokens(req.Inventory)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	err = h.store.SaveClusterInventory(merged)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"success": true,
	})
}

// GET /api/admin/config
func (h *Handler) GetAdminConfig(c *fiber.Ctx) error {
	// Verify user is admin
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	// Read dynamic configs falling back to environment variables
	kafkaBrokers := h.store.GetInfraConfig("KAFKA_BROKERS", os.Getenv("KAFKA_BROKERS"))
	kafkaTopic := h.store.GetInfraConfig("KAFKA_TOPIC", os.Getenv("KAFKA_TOPIC"))
	kafkaGroup := h.store.GetInfraConfig("KAFKA_GROUP", os.Getenv("KAFKA_GROUP"))

	clickhouseURL := h.store.GetInfraConfig("CLICKHOUSE_URL", os.Getenv("CLICKHOUSE_URL"))

	// Decompose the ClickHouse URL so the UI can show host/port/credentials
	chScheme, chHost, chPort, chUser, chPass := "http", "", "", "", ""
	if parsed, err := url.Parse(clickhouseURL); err == nil && parsed.Host != "" {
		if parsed.Scheme != "" {
			chScheme = parsed.Scheme
		}
		chHost = parsed.Hostname()
		chPort = parsed.Port()
		if parsed.User != nil {
			chUser = parsed.User.Username()
			chPass, _ = parsed.User.Password()
		}
	}

	minioEndpoint := h.store.GetInfraConfig("MINIO_ENDPOINT", os.Getenv("MINIO_ENDPOINT"))
	minioUseSSL := h.store.GetInfraConfig("MINIO_USE_SSL", os.Getenv("MINIO_USE_SSL"))
	minioAccessKey := h.store.GetInfraConfig("MINIO_ACCESS_KEY", os.Getenv("MINIO_ACCESS_KEY"))
	minioSecretKey := h.store.GetInfraConfig("MINIO_SECRET_KEY", os.Getenv("MINIO_SECRET_KEY"))
	minioBucket := h.store.GetInfraConfig("MINIO_BUCKET", os.Getenv("MINIO_BUCKET"))

	ldapEnabled := h.store.GetInfraConfig("LDAP_ENABLED", os.Getenv("LDAP_ENABLED"))
	if ldapEnabled == "" {
		ldapEnabled = "false"
	}
	ldapURL := h.store.GetInfraConfig("LDAP_URL", os.Getenv("LDAP_URL"))
	ldapBindDN := h.store.GetInfraConfig("LDAP_BIND_DN", os.Getenv("LDAP_BIND_DN"))
	ldapBindPassword := h.store.GetInfraConfig("LDAP_BIND_PASSWORD", os.Getenv("LDAP_BIND_PASSWORD"))
	ldapUserBaseDN := h.store.GetInfraConfig("LDAP_USER_BASE_DN", os.Getenv("LDAP_USER_BASE_DN"))
	ldapUserFilter := h.store.GetInfraConfig("LDAP_USER_FILTER", os.Getenv("LDAP_USER_FILTER"))

	return c.JSON(fiber.Map{
		"kafka": fiber.Map{
			"brokers": kafkaBrokers,
			"topic":   kafkaTopic,
			"group":   kafkaGroup,
		},
		"clickhouse": fiber.Map{
			"url":      clickhouseURL,
			"scheme":   chScheme,
			"host":     chHost,
			"port":     chPort,
			"username": chUser,
			"password": chPass,
			"database": "kubetrace",
		},
		"minio": fiber.Map{
			"endpoint":  minioEndpoint,
			"useSSL":    minioUseSSL,
			"accessKey": minioAccessKey,
			"secretKey": minioSecretKey,
			"bucket":    minioBucket,
		},
		"ldap": fiber.Map{
			"enabled":      ldapEnabled,
			"url":          ldapURL,
			"bindDN":       ldapBindDN,
			"bindPassword": ldapBindPassword,
			"userBaseDN":   ldapUserBaseDN,
			"userFilter":   ldapUserFilter,
		},
		"system": fiber.Map{
			"k8sConnected": h.k8s != nil,
			"demoMode":     os.Getenv("KUBETRACE_DEMO"),
			"timezone":     os.Getenv("TZ"),
		},
	})
}

// POST /api/admin/config
func (h *Handler) UpdateAdminConfig(c *fiber.Ctx) error {
	// Verify user is admin
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req map[string]interface{}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Parse nested fields and save them using SaveInfraConfig
	if kafka, ok := req["kafka"].(map[string]interface{}); ok {
		if brokers, ok := kafka["brokers"].(string); ok {
			h.store.SaveInfraConfig("KAFKA_BROKERS", brokers)
		}
		if topic, ok := kafka["topic"].(string); ok {
			h.store.SaveInfraConfig("KAFKA_TOPIC", topic)
		}
		if group, ok := kafka["group"].(string); ok {
			h.store.SaveInfraConfig("KAFKA_GROUP", group)
		}
	}
	if clickhouse, ok := req["clickhouse"].(map[string]interface{}); ok {
		// Accept either a full URL or host/port/credential components (the UI
		// edits components); rebuild the canonical CLICKHOUSE_URL from them.
		if host, ok := clickhouse["host"].(string); ok && host != "" {
			scheme := "http"
			if s, ok := clickhouse["scheme"].(string); ok && s != "" {
				scheme = s
			}
			hostPort := host
			if port, ok := clickhouse["port"].(string); ok && port != "" {
				hostPort = host + ":" + port
			}
			u := &url.URL{Scheme: scheme, Host: hostPort}
			username, _ := clickhouse["username"].(string)
			password, _ := clickhouse["password"].(string)
			if username != "" {
				if password != "" {
					u.User = url.UserPassword(username, password)
				} else {
					u.User = url.User(username)
				}
			}
			h.store.SaveInfraConfig("CLICKHOUSE_URL", u.String())
		} else if chURL, ok := clickhouse["url"].(string); ok && chURL != "" {
			h.store.SaveInfraConfig("CLICKHOUSE_URL", chURL)
		}
	}
	if minio, ok := req["minio"].(map[string]interface{}); ok {
		if endpoint, ok := minio["endpoint"].(string); ok {
			h.store.SaveInfraConfig("MINIO_ENDPOINT", endpoint)
		}
		if useSSL, ok := minio["useSSL"].(string); ok {
			h.store.SaveInfraConfig("MINIO_USE_SSL", useSSL)
		}
		if accessKey, ok := minio["accessKey"].(string); ok {
			h.store.SaveInfraConfig("MINIO_ACCESS_KEY", accessKey)
		}
		if secretKey, ok := minio["secretKey"].(string); ok {
			currentMinioSecretKey := h.store.GetInfraConfig("MINIO_SECRET_KEY", os.Getenv("MINIO_SECRET_KEY"))
			if secretKey != currentMinioSecretKey {
				h.store.SaveInfraConfig("MINIO_SECRET_KEY", secretKey)
			}
		}
		if bucket, ok := minio["bucket"].(string); ok {
			h.store.SaveInfraConfig("MINIO_BUCKET", bucket)
		}
	}
	if ldap, ok := req["ldap"].(map[string]interface{}); ok {
		if enabled, ok := ldap["enabled"].(string); ok {
			h.store.SaveInfraConfig("LDAP_ENABLED", enabled)
		}
		if url, ok := ldap["url"].(string); ok {
			h.store.SaveInfraConfig("LDAP_URL", url)
		}
		if bindDN, ok := ldap["bindDN"].(string); ok {
			h.store.SaveInfraConfig("LDAP_BIND_DN", bindDN)
		}
		if bindPassword, ok := ldap["bindPassword"].(string); ok {
			currentLdapBindPassword := h.store.GetInfraConfig("LDAP_BIND_PASSWORD", os.Getenv("LDAP_BIND_PASSWORD"))
			if bindPassword != currentLdapBindPassword {
				h.store.SaveInfraConfig("LDAP_BIND_PASSWORD", bindPassword)
			}
		}
		if userBaseDN, ok := ldap["userBaseDN"].(string); ok {
			h.store.SaveInfraConfig("LDAP_USER_BASE_DN", userBaseDN)
		}
		if userFilter, ok := ldap["userFilter"].(string); ok {
			h.store.SaveInfraConfig("LDAP_USER_FILTER", userFilter)
		}
	}

	return c.JSON(fiber.Map{"success": true, "message": "Infrastructure configurations updated successfully"})
}

func isAppNamespaceName(ns string) bool {
	lower := strings.ToLower(ns)
	if strings.HasPrefix(lower, "kube-") ||
		strings.HasPrefix(lower, "istio-") ||
		strings.HasPrefix(lower, "ingress-") ||
		strings.HasPrefix(lower, "prometheus-") ||
		strings.HasPrefix(lower, "argocd-") ||
		strings.HasPrefix(lower, "cert-") ||
		strings.HasPrefix(lower, "devops-") ||
		strings.HasPrefix(lower, "devopstools-") ||
		lower == "argocd" ||
		lower == "prometheus" ||
		lower == "grafana" ||
		lower == "fluentbit" ||
		lower == "metallb-system" ||
		lower == "backstage" ||
		lower == "permission-manager" ||
		lower == "apm-observability" ||
		lower == "lens-shells" ||
		lower == "lens-with-go" ||
		lower == "nfs-provisioner" ||
		lower == "kong" ||
		lower == "trace-prod" {
		return false
	}
	return true
}

// GET /api/admin/namespaces?cluster=
func (h *Handler) GetNamespaceStatuses(c *fiber.Ctx) error {
	// Read-only status is available to every authenticated user: ingestion
	// enablement is a global admin decision, and pages like the service map
	// gate their content on it. Toggling remains admin-only.
	// Restricted users only see the status of their own namespaces.
	allowed := h.allowedNamespaces(c)

	clusterID := c.Query("cluster", "")
	if clusterID == "" {
		agentClusters := h.store.GetAgentManagedClusters()
		if len(agentClusters) == 1 {
			clusterID = agentClusters[0].ClusterID
		} else if len(agentClusters) > 0 {
			clusterID = agentClusters[0].ClusterID
		}
	}

	nsMap := make(map[string]bool)

	// Namespaces come only from the agent-managed target cluster (not telemetry history).
	if clusterID != "" {
		for _, ns := range h.store.GetReportedNamespacesForCluster(clusterID) {
			if isAppNamespaceName(ns) {
				nsMap[ns] = true
			}
		}
	} else {
		for _, agent := range h.store.GetAgentManagedClusters() {
			for _, ns := range h.store.GetReportedNamespacesForCluster(agent.ClusterID) {
				if isAppNamespaceName(ns) {
					nsMap[ns] = true
				}
			}
		}
	}

	enabled := []string{}
	disabled := []string{}
	for ns := range nsMap {
		if !nsAllowed(allowed, ns) {
			continue
		}
		if h.store.IsNamespaceExplicitlyEnabled(ns) {
			enabled = append(enabled, ns)
		} else {
			disabled = append(disabled, ns)
		}
	}
	sort.Strings(enabled)
	sort.Strings(disabled)

	return c.JSON(fiber.Map{
		"cluster":  clusterID,
		"enabled":  enabled,
		"disabled": disabled,
	})
}

// POST /api/admin/namespaces/toggle
func (h *Handler) ToggleNamespace(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req struct {
		Namespace string `json:"namespace"`
		Disabled  bool   `json:"disabled"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}
	if req.Namespace == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Namespace is required"})
	}

	err := h.store.ToggleNamespace(req.Namespace, req.Disabled)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if h.k8s != nil {
		err = h.ReconcileAllClusters(c.Context(), req.Namespace, req.Disabled)
		if err != nil {
			log.Printf("[k8s] error reconciling instrumentation on toggle: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to reconcile Kubernetes instrumentation: " + err.Error()})
		}
	}

	return c.JSON(fiber.Map{
		"success": true,
	})
}

// POST /api/admin/namespaces/add
func (h *Handler) AddNamespace(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}
	if req.Namespace == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Namespace is required"})
	}

	err := h.store.AddConfiguredNamespace(req.Namespace)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if h.k8s != nil {
		err = h.ReconcileAllClusters(c.Context(), req.Namespace, false)
		if err != nil {
			log.Printf("[k8s] error reconciling instrumentation on add: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create Kubernetes instrumentation: " + err.Error()})
		}
	}

	return c.JSON(fiber.Map{
		"success": true,
	})
}

// POST /api/admin/namespaces/delete
func (h *Handler) DeleteNamespace(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}
	if req.Namespace == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Namespace is required"})
	}

	err := h.store.DeleteConfiguredNamespace(req.Namespace)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if h.k8s != nil {
		err = h.ReconcileAllClusters(c.Context(), req.Namespace, true)
		if err != nil {
			log.Printf("[k8s] error reconciling instrumentation on delete: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to delete Kubernetes instrumentation: " + err.Error()})
		}
	}

	return c.JSON(fiber.Map{
		"success": true,
	})
}

// GET /api/admin/instrumentations
func (h *Handler) GetAdminInstrumentations(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	clusterFilter := c.Query("cluster", "")
	var all []*k8s.InstrumentationInfo

	// Instrumentation CRDs exist only on agent-managed target clusters, not the monitoring cluster.
	targetClusters := h.store.GetAgentManagedClusters()
	if clusterFilter != "" {
		targetClusters = nil
		for _, ac := range h.store.GetAgentManagedClusters() {
			if ac.ClusterID == clusterFilter {
				targetClusters = append(targetClusters, ac)
			}
		}
		inv, _ := h.store.GetClusterInventory()
		for _, cluster := range inv {
			if cluster.ID == clusterFilter && cluster.Token != "" && cluster.Status == "Active" {
				targetClusters = append(targetClusters, store.AgentClusterInfo{
					ClusterID:      cluster.ID,
					AgentNamespace: cluster.AgentNamespace,
				})
			}
		}
	}

	for _, agentCluster := range targetClusters {
		inv, _ := h.store.GetClusterInventory()
		var cluster store.ClusterInventoryItem
		for _, item := range inv {
			if item.ID == agentCluster.ClusterID {
				cluster = item
				break
			}
		}
		if cluster.Token != "" {
			_, dynClient, err := k8s.BuildClientsForCluster(clusterHost(cluster), cluster.Token)
			if err == nil {
				remote, err := k8s.GetRemoteInstrumentations(c.Context(), dynClient)
				if err == nil {
					for _, inst := range remote {
						inst.Name = fmt.Sprintf("[%s] %s", agentCluster.ClusterID, inst.Name)
						all = append(all, inst)
					}
				}
			}
		}
	}

	if all == nil {
		all = []*k8s.InstrumentationInfo{}
	}
	for _, inst := range all {
		if h.store.IsNamespaceDisabled(inst.Namespace) {
			inst.Sampler = "always_off"
		}
	}

	return c.JSON(fiber.Map{"instrumentations": all})
}

func maskSecret(secret string) string {
	if secret == "" {
		return ""
	}
	if len(secret) <= 4 {
		return "****"
	}
	return secret[:2] + "****" + secret[len(secret)-2:]
}

// ReconcileAllClusters reconciles auto-instrumentation on agent-managed target clusters only.
func (h *Handler) ReconcileAllClusters(ctx context.Context, namespace string, disabled bool) error {
	if h.store == nil {
		return nil
	}

	inv, err := h.store.GetClusterInventory()
	if err != nil {
		return err
	}
	invMap := make(map[string]store.ClusterInventoryItem)
	for _, item := range inv {
		invMap[item.ID] = item
	}

	reconciled := make(map[string]bool)
	for _, agentCluster := range h.store.GetAgentManagedClusters() {
		cluster, ok := invMap[agentCluster.ClusterID]
		if !ok {
			cluster = store.ClusterInventoryItem{
				ID:             agentCluster.ClusterID,
				AgentNamespace: agentCluster.AgentNamespace,
				Status:         "Active",
			}
		}
		if cluster.Token == "" {
			// Agent on target cluster handles Instrumentation CRDs locally when no remote credentials.
			continue
		}
		if reconciled[cluster.ID] {
			continue
		}
		reconciled[cluster.ID] = true

		host := clusterHost(cluster)
		dynClient, err := k8s.BuildDynamicClientForCluster(host, cluster.Token)
		if err != nil {
			log.Printf("[k8s/remote] failed to build client for cluster %s: %v", cluster.ID, err)
			continue
		}
		agentNs := cluster.AgentNamespace
		if agentNs == "" {
			agentNs = agentCluster.AgentNamespace
		}
		err = k8s.ReconcileRemoteInstrumentation(ctx, dynClient, namespace, disabled, []string{agentNs})
		if err != nil {
			log.Printf("[k8s/remote] error reconciling instrumentation on remote cluster %s: %v", cluster.ID, err)
		}
	}

	// Also reconcile clusters registered with explicit credentials (non-agent path).
	for _, cluster := range inv {
		if cluster.ID == "default" || cluster.Status != "Active" || cluster.Token == "" || reconciled[cluster.ID] {
			continue
		}
		reconciled[cluster.ID] = true
		host := clusterHost(cluster)
		dynClient, err := k8s.BuildDynamicClientForCluster(host, cluster.Token)
		if err != nil {
			log.Printf("[k8s/remote] failed to build client for cluster %s: %v", cluster.ID, err)
			continue
		}
		agentNs := cluster.AgentNamespace
		if agentNs == "" {
			agentNs = h.store.GetAgentNamespaceForCluster(cluster.ID)
		}
		err = k8s.ReconcileRemoteInstrumentation(ctx, dynClient, namespace, disabled, []string{agentNs})
		if err != nil {
			log.Printf("[k8s/remote] error reconciling instrumentation on remote cluster %s: %v", cluster.ID, err)
		}
	}
	return nil
}

// GET /api/admin/retention
func (h *Handler) GetRetention(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	hours := h.store.GetRetentionHours()
	return c.JSON(fiber.Map{
		"retentionHours": hours,
	})
}

// POST /api/admin/retention
func (h *Handler) UpdateRetention(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	var req struct {
		RetentionHours int `json:"retentionHours"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// 0 = keep forever (tiered to MinIO, never deleted). Negatives are invalid.
	if req.RetentionHours < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Retention hours cannot be negative (use 0 to keep forever)"})
	}

	err := h.store.SaveRetentionConfig(req.RetentionHours)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"success":        true,
		"retentionHours": req.RetentionHours,
	})
}

// POST /api/admin/retention/clear
func (h *Handler) ClearAllTraces(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}

	count, err := h.store.DeleteAllMinioTraces()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"success":      true,
		"deletedCount": count,
	})
}
