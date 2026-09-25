package api

import (
	"log"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/store"
)

func (h *Handler) requireAdmin(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return fiber.NewError(fiber.StatusForbidden, "Forbidden: admin access required")
		}
	}
	return nil
}

func clusterHost(item store.ClusterInventoryItem) string {
	if item.APIServer != "" {
		return item.APIServer
	}
	host := item.ID
	if host == "default" {
		return ""
	}
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		return "https://" + host + ":6443"
	}
	return host
}

func maskClusterInventoryItem(item store.ClusterInventoryItem) fiber.Map {
	// Agent-managed clusters intentionally have no central kubeconfig/token —
	// the agent uses an in-cluster ServiceAccount + ClusterRole. Treating an
	// empty Token as "missing credentials" made Admin show a false red "Not set".
	agentManaged := item.ManagedByAgent || strings.EqualFold(item.CredentialType, "agent")
	credType := item.CredentialType
	if credType == "" {
		if agentManaged {
			credType = "agent"
		} else {
			credType = "kubeconfig"
		}
	}
	accessMode := "none"
	switch {
	case agentManaged:
		accessMode = "agent"
	case item.Token != "":
		accessMode = credType
	}
	return fiber.Map{
		"id":             item.ID,
		"displayName":    item.DisplayName,
		"token":          item.Token,
		"hasCredentials": item.Token != "" || agentManaged,
		"status":         item.Status,
		"credentialType": credType,
		"apiServer":      item.APIServer,
		"agentNamespace": item.AgentNamespace,
		"managedByAgent": agentManaged,
		"accessMode":     accessMode,
	}
}

// POST /api/admin/clusters/test
func (h *Handler) TestClusterConnection(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	var req struct {
		ID             string `json:"id"`
		Token          string `json:"token"`
		CredentialType string `json:"credentialType"`
		APIServer      string `json:"apiServer"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	token := strings.TrimSpace(req.Token)
	if token == "******" || token == maskSecret(token) {
		if req.ID != "" {
			existing, err := h.store.GetClusterByID(req.ID)
			if err == nil {
				token = existing.Token
			}
		}
	}

	// No central credentials: agent clusters prove reachability via heartbeat
	// (in-cluster ClusterRole), not a kubeconfig dial from api-backend.
	if token == "" {
		if ok, detail := h.agentClusterTestResult(req.ID, req.CredentialType); ok {
			return c.JSON(fiber.Map{
				"success":       true,
				"serverVersion": "agent",
				"message":       detail,
				"mode":          "agent",
			})
		}
		if req.ID != "" {
			if existing, err := h.store.GetClusterByID(req.ID); err == nil {
				if existing.ManagedByAgent || strings.EqualFold(existing.CredentialType, "agent") {
					return c.JSON(fiber.Map{
						"success": false,
						"error":   "Agent is registered but not reporting (heartbeat older than 2 minutes)",
						"mode":    "agent",
					})
				}
			}
			if strings.EqualFold(req.CredentialType, "agent") {
				return c.JSON(fiber.Map{
					"success": false,
					"error":   "Agent is registered but not reporting (heartbeat older than 2 minutes)",
					"mode":    "agent",
				})
			}
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Credentials are required"})
	}

	host := req.APIServer
	if host == "" && req.ID != "" {
		host = clusterHost(store.ClusterInventoryItem{ID: req.ID, APIServer: req.APIServer})
	}

	version, err := k8s.TestClusterConnection(c.Context(), host, token)
	if err != nil {
		return c.JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"success":       true,
		"serverVersion": version,
		"message":       "Successfully connected to Kubernetes API",
		"mode":          "token",
	})
}

// agentClusterTestResult returns true when the cluster has a live agent heartbeat.
func (h *Handler) agentClusterTestResult(clusterID, credentialType string) (bool, string) {
	if clusterID == "" && !strings.EqualFold(credentialType, "agent") {
		return false, ""
	}
	if clusterID != "" && h.store.IsAgentManagedCluster(clusterID) {
		ns := h.store.GetAgentNamespaceForCluster(clusterID)
		if ns == "" {
			ns = "unknown"
		}
		return true, "Agent heartbeat healthy (in-cluster ClusterRole); namespace " + ns
	}
	return false, ""
}

// DELETE /api/admin/clusters/:id
func (h *Handler) DeleteCluster(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	clusterID := c.Params("id")
	if clusterID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Cluster ID is required"})
	}

	inv, err := h.store.GetClusterInventory()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	filtered := make([]store.ClusterInventoryItem, 0, len(inv))
	found := false
	for _, item := range inv {
		if item.ID == clusterID {
			found = true
			continue
		}
		filtered = append(filtered, item)
	}
	if !found {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Cluster not found"})
	}
	if err := h.store.SaveClusterInventory(filtered); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"success": true})
}

// GET /api/admin/clusters/:id/namespaces
func (h *Handler) GetClusterNamespaces(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	clusterID := c.Params("id")
	cluster, err := h.store.GetClusterByID(clusterID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}

	nsSet := make(map[string]bool)

	// From agent-reported pods
	for _, ns := range h.store.GetReportedNamespacesForCluster(clusterID) {
		if isAppNamespaceName(ns) {
			nsSet[ns] = true
		}
	}

	// From remote API if credentials available
	if cluster.Token != "" && cluster.Status == "Active" {
		remoteNs, err := k8s.ListClusterNamespaces(c.Context(), clusterHost(*cluster), cluster.Token)
		if err == nil {
			for _, ns := range remoteNs {
				if isAppNamespaceName(ns) {
					nsSet[ns] = true
				}
			}
		}
	}

	namespaces := make([]fiber.Map, 0, len(nsSet))
	disabledMap := make(map[string]bool)
	for _, d := range h.store.GetDisabledNamespacesForCluster(clusterID) {
		disabledMap[d] = true
	}
	for ns := range nsSet {
		namespaces = append(namespaces, fiber.Map{
			"name":     ns,
			"disabled": disabledMap[ns] || h.store.IsNamespaceDisabledForCluster(clusterID, ns),
			"cluster":  clusterID,
		})
	}
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i]["name"].(string) < namespaces[j]["name"].(string)
	})

	return c.JSON(fiber.Map{
		"cluster":    clusterID,
		"namespaces": namespaces,
	})
}

// GET /api/admin/clusters/:id/namespaces/:namespace/applications
func (h *Handler) GetClusterApplications(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	clusterID := c.Params("id")
	namespace := c.Params("namespace")
	cluster, err := h.store.GetClusterByID(clusterID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}

	savedConfigs, _ := h.store.GetWorkloadInstrumentations(clusterID)
	configMap := make(map[string]store.WorkloadInstrumentation)
	for _, cfg := range savedConfigs {
		if cfg.Namespace == namespace {
			key := cfg.WorkloadKind + "/" + cfg.WorkloadName
			configMap[key] = cfg
		}
	}

	var workloads []k8s.WorkloadInfo

	if cluster.Token != "" && cluster.Status == "Active" {
		client, _, err := k8s.BuildClientsForCluster(clusterHost(*cluster), cluster.Token)
		if err == nil {
			workloads, err = k8s.ListWorkloadsInNamespace(c.Context(), client, namespace)
		}
	}

	// No live credentials: fall back to what the agent reported. Prefer the
	// workloads it read straight off the Deployment/STS/DS status, since pod
	// aggregation undercounts replicas whenever a pod is missing from the
	// report (pending, evicted, or filtered).
	if len(workloads) == 0 {
		pods := h.store.GetReportedPodsForCluster(clusterID, namespace)
		if len(pods) == 0 {
			pods = h.store.GetReportedPods(namespace)
		}
		if reported := h.store.GetReportedWorkloadsForCluster(clusterID, namespace); len(reported) > 0 {
			workloads = workloadsFromReportedWorkloads(reported, pods)
		} else {
			workloads = workloadsFromReportedPods(pods)
		}
	}

	applications := make([]fiber.Map, 0, len(workloads))
	for _, w := range workloads {
		key := w.Kind + "/" + w.Name
		cfg, hasCfg := configMap[key]
		enabled := w.Instrumented
		manualOverride := false
		if hasCfg {
			manualOverride = cfg.ManualOverride
			// RememberDetectedLanguage may insert cache-only rows (Enabled=false,
			// Language empty). Those must not override live inject annotations.
			// Trust Enabled only when Admin assigned a stack, flipped the toggle
			// on, or set a manual override.
			if cfg.ManualOverride || store.IsAssignedStack(cfg.Language) || cfg.Enabled {
				enabled = cfg.Enabled
			}
		}
		detected := w.Language
		if detected == "" && hasCfg && cfg.LastDetectedLanguage != "" {
			detected = cfg.LastDetectedLanguage
		}
		// Only persist freshly observed detection, not the fallback echo.
		if w.Language != "" && store.IsAssignedStack(w.Language) {
			_ = h.store.RememberDetectedLanguage(clusterID, namespace, w.Kind, w.Name, w.Language)
		}
		lang := detected
		if hasCfg && store.IsAssignedStack(cfg.Language) {
			lang = cfg.Language
		}
		app := fiber.Map{
			"name":             w.Name,
			"namespace":        w.Namespace,
			"kind":             w.Kind,
			"replicas":         w.Replicas,
			"ready":            w.Ready,
			"language":         lang,
			"detectedLanguage": detected,
			"instrumented":     enabled,
			"manualOverride":   manualOverride,
			"details":          w.Details,
			"labels":           w.Labels,
			"cluster":          clusterID,
			"statusReason":     w.StatusReason,
			"statusMessage":    w.StatusMessage,
		}
		if strings.EqualFold(lang, "nginx") || strings.EqualFold(detected, "nginx") {
			app["nginxVersion"] = w.NginxVersion
			app["injectCompatible"] = w.InjectCompatible
			if w.InjectBlockedReason != "" {
				app["injectBlockedReason"] = w.InjectBlockedReason
			}
		}
		applications = append(applications, app)
	}

	sort.Slice(applications, func(i, j int) bool {
		return applications[i]["name"].(string) < applications[j]["name"].(string)
	})

	return c.JSON(fiber.Map{
		"cluster":      clusterID,
		"namespace":    namespace,
		"applications": applications,
	})
}

func isDeadPodPhase(phase string) bool {
	switch phase {
	case "Failed", "Succeeded", "Unknown":
		return true
	default:
		return false
	}
}

// workloadsFromReportedWorkloads converts agent-reported workloads into the
// shape Admin renders. Replica/ready counts come from the workload itself; the
// language, labels and instrumentation details only exist on pods, so they are
// merged in from the matching reported pods of the same service.
func workloadsFromReportedWorkloads(reported []store.ReportedWorkload, pods []store.ReportedPod) []k8s.WorkloadInfo {
	result := make([]k8s.WorkloadInfo, 0, len(reported))
	for _, w := range reported {
		if w.Name == "" {
			continue
		}
		info := k8s.WorkloadInfo{
			Name:          w.Name,
			Namespace:     w.Namespace,
			Kind:          w.Kind,
			Replicas:      w.Replicas,
			Ready:         w.Ready,
			Language:      w.Language,
			Instrumented:  w.Instrumented,
			StatusReason:  w.StatusReason,
			StatusMessage: w.StatusMessage,
		}
		for _, p := range pods {
			if p.Namespace != w.Namespace || !p.MatchesService(w.Name) {
				continue
			}
			if info.Language == "" && p.Language != "" {
				info.Language = p.Language
			}
			if p.Instrumented {
				info.Instrumented = true
			}
			if info.Labels == nil && len(p.Labels) > 0 {
				info.Labels = p.Labels
			}
			if info.Details == "" && p.Details != "" {
				info.Details = p.Details
			}
			if p.IsFrontend {
				info.IsFrontend = true
			}
			if info.Ready < info.Replicas && p.StatusReason != "" {
				info.StatusReason, info.StatusMessage = k8s.PickWorstStatus(
					info.StatusReason, info.StatusMessage, p.StatusReason, p.StatusMessage,
				)
			}
		}
		result = append(result, info)
	}
	return result
}

func workloadsFromReportedPods(pods []store.ReportedPod) []k8s.WorkloadInfo {
	seen := make(map[string]k8s.WorkloadInfo)
	for _, p := range pods {
		if p.Name == "" || isDeadPodPhase(p.Phase) {
			continue
		}
		name := p.ServiceName()
		key := p.Namespace + "/" + name
		if existing, ok := seen[key]; ok {
			existing.Replicas++
			if p.Ready {
				existing.Ready++
			}
			if p.Instrumented {
				existing.Instrumented = true
			}
			if existing.Language == "" && p.Language != "" {
				existing.Language = p.Language
			}
			if !p.Ready && p.StatusReason != "" {
				existing.StatusReason, existing.StatusMessage = k8s.PickWorstStatus(
					existing.StatusReason, existing.StatusMessage, p.StatusReason, p.StatusMessage,
				)
			}
			seen[key] = existing
			continue
		}
		var ready int32
		if p.Ready {
			ready = 1
		}
		info := k8s.WorkloadInfo{
			Name:         name,
			Namespace:    p.Namespace,
			Kind:         "Deployment",
			Replicas:     1,
			Ready:        ready,
			Language:     p.Language,
			Instrumented: p.Instrumented,
			Labels:       p.Labels,
			Details:      p.Details,
			IsFrontend:   p.IsFrontend,
		}
		if !p.Ready {
			info.StatusReason = p.StatusReason
			info.StatusMessage = p.StatusMessage
		}
		seen[key] = info
	}
	result := make([]k8s.WorkloadInfo, 0, len(seen))
	for _, w := range seen {
		result = append(result, w)
	}
	return result
}

// POST /api/admin/applications/instrumentation/toggle
func (h *Handler) ToggleApplicationInstrumentation(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	var req struct {
		ClusterID    string `json:"clusterId"`
		Namespace    string `json:"namespace"`
		WorkloadName string `json:"workloadName"`
		WorkloadKind string `json:"workloadKind"`
		Language     string `json:"language"`
		Enabled      bool   `json:"enabled"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}
	if req.ClusterID == "" || req.Namespace == "" || req.WorkloadName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "clusterId, namespace, and workloadName are required"})
	}
	if req.WorkloadKind == "" {
		req.WorkloadKind = "Deployment"
	}

	cluster, err := h.store.GetClusterByID(req.ClusterID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}

	resolvedLang := req.Language
	if cluster.Token != "" && cluster.Status == "Active" {
		restConfig, err := k8s.BuildRestConfig(clusterHost(*cluster), cluster.Token)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to connect to cluster: " + err.Error()})
		}
		client, dynClient, err := k8s.BuildClientsForCluster(clusterHost(*cluster), cluster.Token)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to connect to cluster: " + err.Error()})
		}

		// Discovery is authoritative: the agent's real namespace on the target
		// cluster wins over any stale/stored value. Fall back to an explicit
		// per-cluster override, then to the configurable default.
		agentNs := k8s.FindAgentNamespace(c.Context(), client)
		if agentNs == "" {
			agentNs = cluster.AgentNamespace
		}
		if agentNs == "" {
			agentNs = k8s.AgentNamespaceFallback()
		}

		if req.Enabled {
			if err := k8s.ReconcileRemoteInstrumentation(c.Context(), dynClient, req.Namespace, false, []string{agentNs}); err != nil {
				log.Printf("[k8s] namespace instrumentation reconcile: %v", err)
			}
		}

		detectedLang, err := k8s.ApplyWorkloadInstrumentation(c.Context(), client, restConfig, req.ClusterID, req.Namespace, req.WorkloadName, req.WorkloadKind, req.Language, agentNs, req.Enabled)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to patch workload: " + err.Error()})
		}
		resolvedLang = detectedLang
	}

	item := store.WorkloadInstrumentation{
		ClusterID:      req.ClusterID,
		Namespace:      req.Namespace,
		WorkloadName:   req.WorkloadName,
		WorkloadKind:   req.WorkloadKind,
		Enabled:        req.Enabled,
		Language:       resolvedLang,
		ManualOverride: !isAutoStack(req.Language),
	}
	if err := h.store.SaveWorkloadInstrumentation(item); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"success": true})
}

// mergeInventoryTokens preserves existing tokens when UI sends masked values.
func (h *Handler) GetClusterInstrumentations(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	clusterID := c.Params("id")
	if _, err := h.store.GetClusterByID(clusterID); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"cluster":          clusterID,
		"instrumentations": h.instrumentationsForCluster(c.Context(), clusterID),
	})
}

// mergeInventoryTokens preserves existing tokens when UI sends masked values.
func (h *Handler) mergeInventoryTokens(incoming []store.ClusterInventoryItem) ([]store.ClusterInventoryItem, error) {
	existing, err := h.store.GetClusterInventory()
	if err != nil {
		return incoming, nil
	}
	existingMap := make(map[string]store.ClusterInventoryItem)
	for _, item := range existing {
		existingMap[item.ID] = item
	}
	for i, item := range incoming {
		if item.Token == "" || item.Token == "******" || item.Token == maskSecret(existingMap[item.ID].Token) {
			if prev, ok := existingMap[item.ID]; ok {
				incoming[i].Token = prev.Token
			}
		}
		if incoming[i].CredentialType == "" {
			incoming[i].CredentialType = "kubeconfig"
		}
		// Leave AgentNamespace empty when unset — it is discovered dynamically
		// from where agent-backend actually runs, not pinned to a hardcoded value.
	}
	return incoming, nil
}

func isAutoStack(language string) bool {
	return !store.IsAssignedStack(language)
}
