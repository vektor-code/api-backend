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
	// ClickHouse mode: always fetch from ClickHouse — the in-memory cache
	// holds only a recent window and could return a partial (incomplete) trace.
	if s.chMode {
		return s.chGetTrace(traceID)
	}

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
// SearchEndpoints returns a stable per-endpoint aggregation over the query
// window (root service + root operation). In ClickHouse mode it aggregates the
// whole window server-side; otherwise it falls back to aggregating the matching
// in-memory trace list items.
func (s *Store) SearchEndpoints(q *models.SearchQuery) ([]*models.EndpointStat, error) {
	if s.chMode {
		return s.chAggregateEndpoints(q)
	}

	lq := *q
	lq.Limit = 2000
	lq.Offset = 0
	items, err := s.SearchTraces(&lq)
	if err != nil {
		return nil, err
	}

	groups := make(map[string]*models.EndpointStat)
	durs := make(map[string][]float64)
	for _, it := range items {
		key := it.ServiceName + "\x00" + it.RootName
		g := groups[key]
		if g == nil {
			g = &models.EndpointStat{ServiceName: it.ServiceName, OperationName: it.RootName}
			groups[key] = g
		}
		g.Count++
		if it.HasError {
			g.ErrorCount++
		}
		durs[key] = append(durs[key], it.DurationMs)
	}

	out := make([]*models.EndpointStat, 0, len(groups))
	for key, g := range groups {
		ds := durs[key]
		sort.Float64s(ds)
		var sum float64
		for _, d := range ds {
			sum += d
		}
		if len(ds) > 0 {
			g.AvgDurationMs = sum / float64(len(ds))
			g.P95DurationMs = ds[int(float64(len(ds)-1)*0.95)]
		}
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AvgDurationMs*float64(out[i].Count) > out[j].AvgDurationMs*float64(out[j].Count)
	})
	return out, nil
}

func (s *Store) SearchTraces(q *models.SearchQuery) ([]*models.TraceListItem, error) {
	if q.Limit == 0 {
		q.Limit = 50
	}

	// ClickHouse mode: search the full retention window server-side.
	if s.chMode {
		return s.chSearchTraces(q)
	}

	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	// Phase 1: Collect candidate trace IDs with lightweight filtering.
	// Stop early once we have enough candidates (3x limit for sort headroom).
	maxCandidates := q.Limit * 3
	if maxCandidates < 200 {
		maxCandidates = 200
	}

	type candidate struct {
		traceID string
		start   time.Time
	}
	candidates := make([]candidate, 0, maxCandidates)

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

		candidates = append(candidates, candidate{trace.TraceID, trace.StartTime})

		// Early termination: we have enough candidates for sorting+pagination
		if len(candidates) >= maxCandidates {
			break
		}
	}

	// Phase 2: Sort candidates by start time (newest first) and take the page
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].start.After(candidates[j].start)
	})
	if len(candidates) > q.Limit {
		candidates = candidates[:q.Limit]
	}

	// Phase 3: Build full TraceListItems only for the final page
	items := make([]*models.TraceListItem, 0, len(candidates))
	for _, c := range candidates {
		trace := s.recentTraces[c.traceID]
		if trace == nil {
			continue
		}
		items = append(items, s.buildTraceListItem(trace))
	}

	return items, nil
}

// buildTraceListItem assembles the list-view summary of a trace, including
// involved services, third-party tools, error details and the service flow.
func (s *Store) buildTraceListItem(trace *models.Trace) *models.TraceListItem {
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
					msg = sp.Error
				}
				// Add HTTP context so list views show e.g. "GET http://x -> 404 Not Found"
				if code := sp.Attributes["http.response.status_code"]; code == "" {
					if code = sp.Attributes["http.status_code"]; code != "" && !strings.Contains(msg, code) {
						msg = strings.TrimSpace(fmt.Sprintf("HTTP %s %s", code, msg))
					}
				} else if !strings.Contains(msg, code) {
					msg = strings.TrimSpace(fmt.Sprintf("HTTP %s %s", code, msg))
				}
				if msg == "" {
					msg = "Unknown error"
				}
				errSummary = msg
				break
			}
		}
	}

	// Build ordered service flow by walking the span tree from root
	serviceFlow := buildServiceFlow(trace.Spans)

	// Collect all namespaces the trace crosses, ordered by span start time,
	// so multi-namespace flows are visible in list views.
	nsSeen := make(map[string]bool)
	var namespaces []string
	sortedSpans := make([]*models.Span, len(trace.Spans))
	copy(sortedSpans, trace.Spans)
	sort.Slice(sortedSpans, func(i, j int) bool { return sortedSpans[i].StartTime.Before(sortedSpans[j].StartTime) })
	for _, sp := range sortedSpans {
		if sp.Namespace != "" && !nsSeen[sp.Namespace] {
			nsSeen[sp.Namespace] = true
			namespaces = append(namespaces, sp.Namespace)
		}
	}

	item := &models.TraceListItem{
		TraceID:         trace.TraceID,
		ServiceName:     trace.ServiceName,
		Namespace:       trace.Namespace,
		Namespaces:      namespaces,
		Cluster:         trace.Cluster,
		StartTime:       trace.StartTime,
		DurationMs:      trace.DurationMs,
		SpanCount:       trace.SpanCount,
		HasError:        trace.HasError,
		Services:        svcs,
		ThirdPartyTools: tools,
		ServiceFlow:     serviceFlow,
		ErrorType:       errType,
		ErrorSummary:    errSummary,
	}
	if trace.RootSpan != nil {
		item.RootName = trace.RootSpan.Name
	}
	return item
}

// buildServiceFlow walks the span tree from root to leaves and returns
// an ordered list of unique service names representing the request flow.
func buildServiceFlow(spans []*models.Span) []string {
	if len(spans) == 0 {
		return nil
	}

	// Build parent->children index. Spans whose parent was never captured
	// (uninstrumented hop, sampling, cross-namespace gap) are treated as
	// roots too, so their subtree still appears in the flow.
	idSet := make(map[string]bool, len(spans))
	for _, sp := range spans {
		idSet[sp.SpanID] = true
	}
	childrenMap := make(map[string][]*models.Span)
	var roots []*models.Span
	for _, sp := range spans {
		if isRootParentID(sp.ParentSpanID) || !idSet[sp.ParentSpanID] {
			roots = append(roots, sp)
		} else {
			childrenMap[sp.ParentSpanID] = append(childrenMap[sp.ParentSpanID], sp)
		}
	}

	// If no explicit root found, use the earliest span
	if len(roots) == 0 {
		earliestIdx := 0
		for i, sp := range spans {
			if sp.StartTime.Before(spans[earliestIdx].StartTime) {
				earliestIdx = i
			}
		}
		roots = []*models.Span{spans[earliestIdx]}
	}

	// BFS traversal collecting unique service names in order
	seen := make(map[string]bool)
	var flow []string
	queue := make([]*models.Span, 0, len(spans))
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].StartTime.Before(roots[j].StartTime)
	})
	queue = append(queue, roots...)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if !seen[current.ServiceName] {
			seen[current.ServiceName] = true
			flow = append(flow, current.ServiceName)
		}

		children := childrenMap[current.SpanID]
		sort.Slice(children, func(i, j int) bool {
			return children[i].StartTime.Before(children[j].StartTime)
		})
		queue = append(queue, children...)
	}

	return flow
}
