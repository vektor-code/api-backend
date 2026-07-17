package store

import (
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

// GetServiceMap returns a pre-built service dependency graph.
// It returns immediately from cache when available; otherwise it builds,
// stores in cache, and returns the result.
func (s *Store) GetServiceMap(namespace string) (*models.ServiceMapData, error) {
	s.serviceMapMu.RLock()
	cached, ok := s.serviceMapCache[namespace]
	s.serviceMapMu.RUnlock()
	if ok && cached != nil {
		return cached, nil
	}

	// Cache miss — build now
	result := s.buildServiceMap(namespace)

	s.serviceMapMu.Lock()
	s.serviceMapCache[namespace] = result
	s.serviceMapMu.Unlock()

	return result, nil
}

// InvalidateServiceMapCache clears all pre-computed maps.
// Should be called whenever recentTraces or statsCache are updated (e.g. after syncState).
func (s *Store) InvalidateServiceMapCache() {
	s.serviceMapMu.Lock()
	s.serviceMapCache = make(map[string]*models.ServiceMapData)
	s.serviceMapMu.Unlock()
}

// buildServiceMap performs the full trace-scan computation.
// Expensive — call sparingly (only on cache miss or after invalidation).
func (s *Store) buildServiceMap(namespace string) *models.ServiceMapData {
	// Preload the activation map once outside the traces & stats locks to avoid DB queries inside locks
	activeMap := s.GetApplicationActivationMap()
	configMap := s.GetApplicationConfigMap()

	// Take both locks upfront and release at the end — this is safe because
	// buildServiceMap never calls isMicroservice (which would re-acquire statsMu).
	s.statsMu.RLock()
	s.tracesMu.RLock()
	defer s.statsMu.RUnlock()
	defer s.tracesMu.RUnlock()

	// Snapshot the statsCache keys once for O(1) microservice lookups
	statsCacheSnapshot := s.statsCache

	disabledMap := make(map[string]bool)
	for _, ns := range s.GetDisabledNamespaces() {
		disabledMap[ns] = true
	}

	data := &models.ServiceMapData{Namespace: namespace}
	edgeMap := make(map[string]*models.ServiceEdge)
	infraNodes := make(map[string]*models.ServiceStats)

	// Scan recent traces for edges
	for _, trace := range s.recentTraces {
		if namespace != "" {
			hasNs := false
			for _, sp := range trace.Spans {
				if disabledMap[sp.Namespace] {
					continue
				}
				if sp.Namespace == namespace {
					hasNs = true
					break
				}
			}
			if !hasNs {
				continue
			}
		}

		// Enrich spans ONCE and build lookup structures
		spanMap := make(map[string]*models.Span, len(trace.Spans))
		parentSet := make(map[string]bool, len(trace.Spans))
		for _, sp := range trace.Spans {
			if disabledMap[sp.Namespace] {
				continue
			}
			s.enrichSpanMetadata(sp)
			spanMap[sp.SpanID] = sp
			if !isRootParentID(sp.ParentSpanID) {
				parentSet[sp.ParentSpanID] = true
			}
		}

		for _, sp := range trace.Spans {
			if disabledMap[sp.Namespace] {
				continue
			}
			if enabled, exists := activeMap[sp.Namespace+":"+sp.ServiceName]; exists && !enabled {
				continue
			}
			// Check for external database or messaging infrastructure calls,
			// or uninstrumented client calls (e.g. Vault, MinIO, external HTTP APIs)
			dbSystem, hasDb := sp.Attributes["db.system"]
			messagingSystem, hasMsg := sp.Attributes["messaging.system"]
			isClient := sp.Kind == models.SpanKindClient || sp.Kind == "CLIENT"

			if hasDb || hasMsg || (isClient && !parentSet[sp.SpanID]) {
				baseName := ""
				targetNamespace := ""
				if hasDb {
					baseName = dbSystem
				} else if hasMsg {
					baseName = messagingSystem
				} else {
					rawHost := getClientDependencyName(sp)
					var parsedNs string
					baseName, parsedNs = s.parseK8sServiceAndNamespace(rawHost)
					targetNamespace = parsedNs
				}

				if baseName != "" {
					// Use the snapshot — no lock re-acquisition needed
					if isClient && !hasDb && !hasMsg && isMicroserviceFromSnapshot(statsCacheSnapshot, sp.Namespace, baseName) {
						if targetNamespace == "" {
							targetNamespace = resolveServiceNamespaceFromSnapshot(statsCacheSnapshot, sp.Namespace, baseName)
						}
						edgeKey := sp.Namespace + ":" + sp.ServiceName + "->" + targetNamespace + ":" + baseName
						e, ok := edgeMap[edgeKey]
						if !ok {
							e = &models.ServiceEdge{
								Source:          sp.ServiceName,
								Target:          baseName,
								SourceNamespace: sp.Namespace,
								TargetNamespace: targetNamespace,
							}
							edgeMap[edgeKey] = e
						}
						e.CallCount++
						e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-1) + sp.DurationMs) / float64(e.CallCount)
						if sp.Status == models.SpanStatusError {
							e.ErrorCount++
						}
						continue
					}

					infraName := getInfraNodeName(sp, baseName)

					// Avoid self-loop rendering if client name matches service name
					if infraName == sp.ServiceName {
						continue
					}

					edgeKey := sp.Namespace + ":" + sp.ServiceName + "->" + sp.Namespace + ":" + infraName
					e, ok := edgeMap[edgeKey]
					if !ok {
						e = &models.ServiceEdge{
							Source:          sp.ServiceName,
							Target:          infraName,
							SourceNamespace: sp.Namespace,
							TargetNamespace: sp.Namespace,
						}
						edgeMap[edgeKey] = e
					}
					e.CallCount++
					e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-1) + sp.DurationMs) / float64(e.CallCount)
					if sp.Status == models.SpanStatusError {
						e.ErrorCount++
					}

					// Track metrics for the virtual infrastructure node
					infraKey := sp.Namespace + ":" + infraName
					infra, ok := infraNodes[infraKey]
					if !ok {
						infra = &models.ServiceStats{
							ServiceName:      infraName,
							Namespace:        sp.Namespace,
							IsInfrastructure: true,
						}
						infraNodes[infraKey] = infra
					}
					infra.RequestCount++
					if sp.Status == models.SpanStatusError {
						infra.ErrorCount++
					}
					updateLatencyEstimates(infra, sp.DurationMs)
				}
			}

			if sp.ParentSpanID == "" || sp.ParentSpanID == "0" || sp.ParentSpanID == "0000000000000000" {
				// Trace entry point from external traffic
				edgeKey := namespace + ":Internet->" + sp.Namespace + ":" + sp.ServiceName
				e, ok := edgeMap[edgeKey]
				if !ok {
					e = &models.ServiceEdge{
						Source:          "Internet",
						Target:          sp.ServiceName,
						SourceNamespace: namespace,
						TargetNamespace: sp.Namespace,
					}
					edgeMap[edgeKey] = e
				}
				e.CallCount++
				e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-1) + sp.DurationMs) / float64(e.CallCount)
				if sp.Status == models.SpanStatusError {
					e.ErrorCount++
				}
				continue
			}
			parent, ok := spanMap[sp.ParentSpanID]
			if !ok {
				// Orphan span whose parent is not in this trace fragment - count as external gateway call
				edgeKey := namespace + ":Internet->" + sp.Namespace + ":" + sp.ServiceName
				e, ok := edgeMap[edgeKey]
				if !ok {
					e = &models.ServiceEdge{
						Source:          "Internet",
						Target:          sp.ServiceName,
						SourceNamespace: namespace,
						TargetNamespace: sp.Namespace,
					}
					edgeMap[edgeKey] = e
				}
				e.CallCount++
				e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-1) + sp.DurationMs) / float64(e.CallCount)
				if sp.Status == models.SpanStatusError {
					e.ErrorCount++
				}
				continue
			}
			if parent.ServiceName == sp.ServiceName {
				continue
			}
			edgeKey := parent.Namespace + ":" + parent.ServiceName + "->" + sp.Namespace + ":" + sp.ServiceName
			e, ok := edgeMap[edgeKey]
			if !ok {
				e = &models.ServiceEdge{
					Source:          parent.ServiceName,
					Target:          sp.ServiceName,
					SourceNamespace: parent.Namespace,
					TargetNamespace: sp.Namespace,
				}
				edgeMap[edgeKey] = e
			}
			e.CallCount++
			e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-1) + sp.DurationMs) / float64(e.CallCount)
			if sp.Status == models.SpanStatusError {
				e.ErrorCount++
			}
		}
	}

	seenNodes := make(map[string]bool)
	for key, svc := range s.statsCache {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if disabledMap[parts[0]] {
			continue
		}

		cfg, exists := configMap[parts[0]+":"+svc.ServiceName]
		if exists {
			if !cfg.Enabled {
				continue
			}
		} else {
			if enabled, activeExists := activeMap[parts[0]+":"+svc.ServiceName]; activeExists && !enabled {
				continue
			}
		}

		if namespace == "" || parts[0] == namespace {
			node := *svc
			if exists && cfg.Language != "" {
				node.Language = cfg.Language
			}
			data.Nodes = append(data.Nodes, node)
			seenNodes[parts[0]+":"+svc.ServiceName] = true
		}
	}

	// Auto-detect and add any standalone/uninstrumented services from reported pods
	var reportedPods []ReportedPod
	if namespace == "" {
		reportedPods = s.GetReportedPods("")
	} else {
		reportedPods = s.GetReportedPods(namespace)
	}

	for _, p := range reportedPods {
		if p.IsFrontend {
			continue
		}
		if disabledMap[p.Namespace] {
			continue
		}
		svcName := p.ServiceName()
		if svcName == "" {
			continue
		}

		cfg, exists := configMap[p.Namespace+":"+svcName]
		if exists {
			if !cfg.Enabled {
				continue
			}
		} else {
			if enabled, activeExists := activeMap[p.Namespace+":"+svcName]; activeExists && !enabled {
				continue
			}
		}

		key := p.Namespace + ":" + svcName
		if !seenNodes[key] {
			seenNodes[key] = true
			lang := p.Language
			if exists && cfg.Language != "" {
				lang = cfg.Language
			}
			data.Nodes = append(data.Nodes, models.ServiceStats{
				ServiceName: svcName,
				Namespace:   p.Namespace,
				Language:    lang,
			})
		}
	}

	// Append infrastructure nodes
	for _, infra := range infraNodes {
		if namespace == "" || infra.Namespace == namespace {
			finalizeServiceStats(infra)
			data.Nodes = append(data.Nodes, *infra)
		}
	}

	// Add Internet node if there are connections from it
	hasInternetEdge := false
	var internetCalls int64 = 0
	var internetErrors int64 = 0
	for _, edge := range edgeMap {
		if edge.Source == "Internet" {
			hasInternetEdge = true
			internetCalls += edge.CallCount
			internetErrors += edge.ErrorCount
		}
	}
	if hasInternetEdge {
		internet := models.ServiceStats{
			ServiceName:  "Internet",
			Namespace:    namespace,
			RequestCount: internetCalls,
			ErrorCount:   internetErrors,
			P50Ms:        0,
			P95Ms:        0,
			P99Ms:        0,
		}
		finalizeServiceStats(&internet)
		data.Nodes = append(data.Nodes, internet)
	}

	for _, edge := range edgeMap {
		data.Edges = append(data.Edges, *edge)
	}

	return data
}

// isMicroserviceFromSnapshot checks if a service name represents an instrumented service
// using a pre-captured statsCache snapshot (no lock needed).
func isMicroserviceFromSnapshot(cache map[string]*models.ServiceStats, namespace, name string) bool {
	nameLower := strings.ToLower(name)
	if strings.HasSuffix(nameLower, "-backend") ||
		nameLower == "ingress-nginx" || nameLower == "gateway" {
		return true
	}
	if _, ok := cache[namespace+":"+name]; ok {
		return true
	}
	for key := range cache {
		if strings.HasSuffix(key, ":"+name) {
			return true
		}
	}
	return false
}

// resolveServiceNamespaceFromSnapshot finds the namespace of a service using
// a pre-captured statsCache snapshot (no lock needed).
func resolveServiceNamespaceFromSnapshot(cache map[string]*models.ServiceStats, defaultNamespace, serviceName string) string {
	if _, ok := cache[defaultNamespace+":"+serviceName]; ok {
		return defaultNamespace
	}
	for key := range cache {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 && parts[1] == serviceName {
			return parts[0]
		}
	}
	return defaultNamespace
}

// isMicroservice checks if a service name represents an instrumented service rather than infrastructure.
// Kept for callers outside buildServiceMap that do not hold statsMu.
func (s *Store) isMicroservice(namespace string, name string) bool {
	nameLower := strings.ToLower(name)
	if strings.HasSuffix(nameLower, "-backend") || nameLower == "ingress-nginx" || nameLower == "gateway" {
		return true
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	// Check if this service exists in our stats cache for this namespace
	if _, ok := s.statsCache[namespace+":"+name]; ok {
		return true
	}
	// Check in other namespaces
	for key := range s.statsCache {
		if strings.HasSuffix(key, ":"+name) {
			return true
		}
	}
	return false
}

// resolveServiceNamespace finds the namespace of a service in s.statsCache, falling back to default
func (s *Store) resolveServiceNamespace(defaultNamespace string, serviceName string) string {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	// Check in defaultNamespace
	if _, ok := s.statsCache[defaultNamespace+":"+serviceName]; ok {
		return defaultNamespace
	}
	// Check in other namespaces
	for key := range s.statsCache {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 && parts[1] == serviceName {
			return parts[0]
		}
	}
	return defaultNamespace
}
