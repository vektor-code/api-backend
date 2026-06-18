package store

import (
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

// GetServiceMap returns service dependency graph for a namespace
func (s *Store) GetServiceMap(namespace string) (*models.ServiceMapData, error) {
	s.statsMu.RLock()
	s.tracesMu.RLock()
	defer s.statsMu.RUnlock()
	defer s.tracesMu.RUnlock()

	data := &models.ServiceMapData{Namespace: namespace}
	edgeMap := make(map[string]*models.ServiceEdge)
	infraNodes := make(map[string]*models.ServiceStats)

	// Scan recent traces for edges
	for _, trace := range s.recentTraces {
		if namespace != "" {
			hasNs := false
			for _, sp := range trace.Spans {
				if sp.Namespace == namespace {
					hasNs = true
					break
				}
			}
			if !hasNs {
				continue
			}
		}
		
		spanMap := make(map[string]*models.Span)
		parentSet := make(map[string]bool)
		for _, sp := range trace.Spans {
			s.enrichSpanMetadata(sp)
			spanMap[sp.SpanID] = sp
			if sp.ParentSpanID != "" {
				parentSet[sp.ParentSpanID] = true
			}
		}

		for _, sp := range trace.Spans {
			s.enrichSpanMetadata(sp)
			// Check for external database or messaging infrastructure calls, or uninstrumented client calls (e.g. Vault, MinIO, external HTTP APIs)
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
					// Check if this represents a client call to a microservice rather than an infra node
					if isClient && !hasDb && !hasMsg && s.isMicroservice(sp.Namespace, baseName) {
						if targetNamespace == "" {
							targetNamespace = s.resolveServiceNamespace(sp.Namespace, baseName)
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
					infra.P50Ms = (infra.P50Ms*float64(infra.RequestCount-1) + sp.DurationMs) / float64(infra.RequestCount)
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

	for key, svc := range s.statsCache {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if namespace == "" || parts[0] == namespace {
			data.Nodes = append(data.Nodes, models.ServiceStats(*svc))
		}
	}

	// Append infrastructure nodes
	for _, infra := range infraNodes {
		if namespace == "" || infra.Namespace == namespace {
			if infra.RequestCount > 0 {
				infra.ErrorRate = float64(infra.ErrorCount) / float64(infra.RequestCount) * 100
			}
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
		errRate := 0.0
		if internetCalls > 0 {
			errRate = float64(internetErrors) / float64(internetCalls) * 100.0
		}
		data.Nodes = append(data.Nodes, models.ServiceStats{
			ServiceName:  "Internet",
			Namespace:    namespace,
			RequestCount: internetCalls,
			ErrorCount:   internetErrors,
			ErrorRate:    errRate,
			P50Ms:        0,
			P95Ms:        0,
			P99Ms:        0,
		})
	}

	for _, edge := range edgeMap {
		data.Edges = append(data.Edges, *edge)
	}

	return data, nil
}

// isMicroservice checks if a service name represents an instrumented service rather than infrastructure
func (s *Store) isMicroservice(namespace string, name string) bool {
	nameLower := strings.ToLower(name)
	if strings.HasSuffix(nameLower, "-backend") || strings.HasSuffix(nameLower, "-frontend") || nameLower == "ingress-nginx" || nameLower == "gateway" {
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
