package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/minio/minio-go/v7"
)

// GetTrace retrieves a trace by ID
func (s *Store) GetTrace(traceID string) (*models.Trace, error) {
	s.tracesMu.RLock()
	if trace, ok := s.recentTraces[traceID]; ok {
		s.tracesMu.RUnlock()
		return trace, nil
	}
	s.tracesMu.RUnlock()

	// In a full implementation, we would query MinIO using prefix `traces/` and filter by traceID.
	// For this scope, if it's not in our recent in-memory cache, we try to load it from S3.
	// We list all objects containing the traceID.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var spans []*models.Span
	
	// Note: Without an index, finding traces efficiently in S3 requires listing many objects.
	// We'll use the MinIO ListObjects recursive search but it can be slow on large buckets.
	// For production, we'd use MinIO metadata indexing or a separate DB for search.
	for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    "traces/",
		Recursive: true,
	}) {
		if obj.Err != nil {
			continue
		}
		if strings.Contains(obj.Key, "/"+traceID+"/") {
			sp, err := s.getSpanObject(ctx, obj.Key)
			if err == nil {
				s.enrichSpanMetadata(sp)
				spans = append(spans, sp)
			}
		}
	}

	if len(spans) == 0 {
		return nil, fmt.Errorf("trace not found: %s", traceID)
	}

	return buildTrace(traceID, spans), nil
}

// SearchTraces performs a filtered search over recent traces
func (s *Store) SearchTraces(q *models.SearchQuery) ([]*models.TraceListItem, error) {
	if q.Limit == 0 {
		q.Limit = 50
	}
	
	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	var items []*models.TraceListItem
	for _, trace := range s.recentTraces {
		if q.Cluster != "" {
			hasCluster := false
			for _, sp := range trace.Spans {
				if sp.Cluster == q.Cluster {
					hasCluster = true
					break
				}
			}
			if !hasCluster {
				continue
			}
		}
		if q.Namespace != "" {
			hasNs := false
			for _, sp := range trace.Spans {
				if sp.Namespace == q.Namespace {
					hasNs = true
					break
				}
			}
			if !hasNs {
				continue
			}
		}
		if q.ServiceName != "" {
			matched := false
			targetSvc := strings.ToLower(q.ServiceName)
			
			// Parse system name if it contains details in parentheses, e.g. "postgresql (users_db)"
			baseSys := targetSvc
			dbName := ""
			if idx := strings.Index(targetSvc, "("); idx != -1 {
				baseSys = strings.TrimSpace(targetSvc[:idx])
				if endIdx := strings.Index(targetSvc, ")"); endIdx != -1 && endIdx > idx {
					dbName = strings.TrimSpace(targetSvc[idx+1 : endIdx])
				}
			}

			for _, sp := range trace.Spans {
				// Match span service name
				if strings.ToLower(sp.ServiceName) == targetSvc {
					matched = true
					break
				}
				// Match database system and optionally db.name
				if dbSys, ok := sp.Attributes["db.system"]; ok && strings.ToLower(dbSys) == baseSys {
					if dbName == "" {
						matched = true
						break
					}
					// If dbName is specified, also verify db.name matches
					if dbN, ok2 := sp.Attributes["db.name"]; ok2 && strings.ToLower(dbN) == dbName {
						matched = true
						break
					}
				}
				// Match messaging system
				if msgSys, ok := sp.Attributes["messaging.system"]; ok && strings.ToLower(msgSys) == baseSys {
					matched = true
					break
				}
				// Match 3rd party tool
				if is3rd, tool := s.isThirdPartySpan(sp); is3rd && strings.ToLower(tool) == baseSys {
					matched = true
					break
				}
			}
			// Fallback: match raw root service name
			if !matched && strings.ToLower(trace.ServiceName) == targetSvc {
				matched = true
			}
			if !matched {
				continue
			}
		}
		if q.HasError != nil && *q.HasError != trace.HasError {
			continue
		}
		if q.TraceID != "" && !strings.Contains(trace.TraceID, q.TraceID) {
			continue
		}
		if q.Operation != "" && (trace.RootSpan == nil || !strings.Contains(strings.ToLower(trace.RootSpan.Name), strings.ToLower(q.Operation))) {
			continue
		}
		if q.MinSpans > 0 && trace.SpanCount < q.MinSpans {
			continue
		}
		if q.MinDurationMs > 0 && trace.DurationMs < q.MinDurationMs {
			continue
		}
		if q.MaxDurationMs > 0 && trace.DurationMs > q.MaxDurationMs {
			continue
		}
		if !q.StartTime.IsZero() && trace.StartTime.Before(q.StartTime) {
			continue
		}
		if !q.EndTime.IsZero() && trace.StartTime.After(q.EndTime) {
			continue
		}

		// Collect unique services and 3rd-party tools involved in this trace
		svcSet := make(map[string]bool)
		toolSet := make(map[string]bool)
		for _, sp := range trace.Spans {
			svcSet[sp.ServiceName] = true
			if is3rd, tool := s.isThirdPartySpan(sp); is3rd {
				toolSet[tool] = true
			}
			if dbSys := sp.Attributes["db.system"]; dbSys != "" {
				toolSet[dbSys] = true
			}
			if msgSys := sp.Attributes["messaging.system"]; msgSys != "" {
				toolSet[msgSys] = true
			}
		}
		var svcs []string
		for svc := range svcSet {
			svcs = append(svcs, svc)
		}
		var tools []string
		for tool := range toolSet {
			tools = append(tools, tool)
		}

		var errType string
		var errSummary string
		if trace.HasError {
			// Find the first error span details
			for _, sp := range trace.Spans {
				if sp.Status == models.SpanStatusError {
					dbSys := sp.Attributes["db.system"]
					msgSys := sp.Attributes["messaging.system"]
					
					if dbSys != "" {
						errType = dbSys
					} else if msgSys != "" {
						errType = msgSys
					} else if sp.Attributes["http.url"] != "" || sp.Attributes["http.method"] != "" {
						errType = "http"
					} else {
						errType = "app"
					}
					
					msg := sp.Attributes["error.message"]
					if msg == "" {
						msg = sp.Attributes["exception.message"]
					}
					if msg == "" {
						msg = sp.Attributes["status.message"]
					}
					if msg == "" && len(sp.Events) > 0 {
						for _, ev := range sp.Events {
							if ev.Attributes != nil {
								if evMsg := ev.Attributes["exception.message"]; evMsg != "" {
									msg = evMsg
									break
								}
							}
						}
					}
					if msg == "" {
						msg = "Unknown error"
					}
					errSummary = msg
					break
				}
			}
		}

		item := &models.TraceListItem{
			TraceID:         trace.TraceID,
			ServiceName:     trace.ServiceName,
			Namespace:       trace.Namespace,
			Cluster:         trace.Cluster,
			StartTime:       trace.StartTime,
			DurationMs:      trace.DurationMs,
			SpanCount:       trace.SpanCount,
			HasError:        trace.HasError,
			Services:        svcs,
			ThirdPartyTools: tools,
			ErrorType:       errType,
			ErrorSummary:    errSummary,
		}
		if trace.RootSpan != nil {
			item.RootName = trace.RootSpan.Name
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].StartTime.After(items[j].StartTime)
	})

	if len(items) > q.Limit {
		items = items[:q.Limit]
	}

	return items, nil
}
