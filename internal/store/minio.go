package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is the MinIO-backed trace storage engine
type Store struct {
	client     *minio.Client
	bucketName string
	
	// In-memory stats cache and recent trace index for fast dashboard rendering
	statsCache map[string]*models.ServiceStats
	statsMu    sync.RWMutex

	recentTraces map[string]*models.Trace
	tracesMu     sync.RWMutex
}



// New creates a new MinIO store
func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio init: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("make bucket: %w", err)
		}
	}

	s := &Store{
		client:       client,
		bucketName:   bucket,
		statsCache:   make(map[string]*models.ServiceStats),
		recentTraces: make(map[string]*models.Trace),
	}

	go s.runGC()
	return s, nil
}

// Close shuts down the store
func (s *Store) Close() error {
	return nil // MinIO client doesn't need explicit close
}

// SaveSpan writes a span to MinIO
func (s *Store) SaveSpan(span *models.Span) error {
	data, err := json.Marshal(span)
	if err != nil {
		return err
	}

	// S3 path: traces/namespace/service/traceID/spanID.json
	objectName := fmt.Sprintf("traces/%s/%s/%s/%s.json", span.Namespace, span.ServiceName, span.TraceID, span.SpanID)
	
	// Fire and forget upload to avoid blocking the ingestion path
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = s.client.PutObject(ctx, s.bucketName, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
	}()

	s.updateStats(span)
	s.updateRecentTraces(span)
	return nil
}

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
				spans = append(spans, sp)
			}
		}
	}

	if len(spans) == 0 {
		return nil, fmt.Errorf("trace not found: %s", traceID)
	}

	return buildTrace(traceID, spans), nil
}

func (s *Store) getSpanObject(ctx context.Context, key string) (*models.Span, error) {
	obj, err := s.client.GetObject(ctx, s.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}

	var span models.Span
	if err := json.Unmarshal(data, &span); err != nil {
		return nil, err
	}
	return &span, nil
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
		if q.Namespace != "" && trace.Namespace != q.Namespace {
			continue
		}
		if q.ServiceName != "" && trace.ServiceName != q.ServiceName {
			continue
		}
		if q.HasError != nil && *q.HasError != trace.HasError {
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

		item := &models.TraceListItem{
			TraceID:     trace.TraceID,
			ServiceName: trace.ServiceName,
			Namespace:   trace.Namespace,
			StartTime:   trace.StartTime,
			DurationMs:  trace.DurationMs,
			SpanCount:   trace.SpanCount,
			HasError:    trace.HasError,
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

// GetNamespaceStats returns namespace-level statistics
func (s *Store) GetNamespaceStats() ([]*models.NamespaceStats, error) {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	nsMap := make(map[string]*models.NamespaceStats)
	for key, svc := range s.statsCache {
		ns := strings.Split(key, ":")[0]
		if nsMap[ns] == nil {
			nsMap[ns] = &models.NamespaceStats{Namespace: ns}
		}
		ns_stat := nsMap[ns]
		ns_stat.Services = append(ns_stat.Services, models.ServiceStats(*svc))
		ns_stat.TraceCount += svc.RequestCount
		ns_stat.ErrorCount += svc.ErrorCount
		if svc.LastSeen.After(ns_stat.LastActivity) {
			ns_stat.LastActivity = svc.LastSeen
		}
	}

	result := make([]*models.NamespaceStats, 0, len(nsMap))
	for _, ns := range nsMap {
		if ns.TraceCount > 0 {
			ns.ErrorRate = float64(ns.ErrorCount) / float64(ns.TraceCount) * 100
		}
		result = append(result, ns)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Namespace < result[j].Namespace
	})
	return result, nil
}

// GetServiceMap returns service dependency graph for a namespace
func (s *Store) GetServiceMap(namespace string) (*models.ServiceMapData, error) {
	s.statsMu.RLock()
	s.tracesMu.RLock()
	defer s.statsMu.RUnlock()
	defer s.tracesMu.RUnlock()

	data := &models.ServiceMapData{Namespace: namespace}
	edgeMap := make(map[string]*models.ServiceEdge)

	// Scan recent traces for edges
	for _, trace := range s.recentTraces {
		if namespace != "" && trace.Namespace != namespace {
			continue
		}
		
		spanMap := make(map[string]*models.Span)
		for _, sp := range trace.Spans {
			spanMap[sp.SpanID] = sp
		}
		for _, sp := range trace.Spans {
			if sp.ParentSpanID == "" || sp.ParentSpanID == "0" || sp.ParentSpanID == "0000000000000000" {
				// Trace entry point from external traffic
				edgeKey := "Internet->" + sp.ServiceName
				e, ok := edgeMap[edgeKey]
				if !ok {
					e = &models.ServiceEdge{
						Source: "Internet",
						Target: sp.ServiceName,
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
				edgeKey := "Internet->" + sp.ServiceName
				e, ok := edgeMap[edgeKey]
				if !ok {
					e = &models.ServiceEdge{
						Source: "Internet",
						Target: sp.ServiceName,
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
			edgeKey := parent.ServiceName + "->" + sp.ServiceName
			e, ok := edgeMap[edgeKey]
			if !ok {
				e = &models.ServiceEdge{
					Source: parent.ServiceName,
					Target: sp.ServiceName,
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

// updateStats maintains in-memory service statistics
func (s *Store) updateStats(span *models.Span) {
	key := span.Namespace + ":" + span.ServiceName
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	stat, ok := s.statsCache[key]
	if !ok {
		stat = &models.ServiceStats{
			ServiceName: span.ServiceName,
			Namespace:   span.Namespace,
		}
		s.statsCache[key] = stat
	}

	stat.RequestCount++
	if span.Status == models.SpanStatusError {
		stat.ErrorCount++
	}
	if stat.RequestCount > 0 {
		stat.ErrorRate = float64(stat.ErrorCount) / float64(stat.RequestCount) * 100
	}
	stat.LastSeen = time.Now()

	n := float64(stat.RequestCount)
	stat.P50Ms = (stat.P50Ms*(n-1) + span.DurationMs) / n
}

func (s *Store) updateRecentTraces(span *models.Span) {
	s.tracesMu.Lock()
	defer s.tracesMu.Unlock()

	trace, ok := s.recentTraces[span.TraceID]
	if !ok {
		trace = &models.Trace{
			TraceID: span.TraceID,
		}
		s.recentTraces[span.TraceID] = trace
	}
	
	// Add span if not exists
	exists := false
	for _, sp := range trace.Spans {
		if sp.SpanID == span.SpanID {
			exists = true
			break
		}
	}
	if !exists {
		trace.Spans = append(trace.Spans, span)
		// Rebuild trace
		updatedTrace := buildTrace(trace.TraceID, trace.Spans)
		s.recentTraces[span.TraceID] = updatedTrace
	}
}

// runGC cleans up old in-memory traces to prevent OOM
func (s *Store) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.tracesMu.Lock()
		cutoff := time.Now().Add(-1 * time.Hour)
		for id, trace := range s.recentTraces {
			if trace.StartTime.Before(cutoff) {
				delete(s.recentTraces, id)
			}
		}
		s.tracesMu.Unlock()
	}
}

// buildTrace assembles a Trace from raw spans
func buildTrace(traceID string, spans []*models.Span) *models.Trace {
	trace := &models.Trace{
		TraceID:   traceID,
		Spans:     spans,
		SpanCount: len(spans),
	}

	var rootSpan *models.Span
	var minStart, maxEnd time.Time
	hasError := false

	for _, sp := range spans {
		if sp.ParentSpanID == "" {
			rootSpan = sp
		}
		if minStart.IsZero() || sp.StartTime.Before(minStart) {
			minStart = sp.StartTime
		}
		if maxEnd.IsZero() || sp.EndTime.After(maxEnd) {
			maxEnd = sp.EndTime
		}
		if sp.Status == models.SpanStatusError {
			hasError = true
		}
	}

	trace.RootSpan = rootSpan
	trace.StartTime = minStart
	trace.EndTime = maxEnd
	trace.HasError = hasError
	trace.DurationMs = float64(maxEnd.Sub(minStart).Microseconds()) / 1000.0

	if rootSpan != nil {
		trace.Namespace = rootSpan.Namespace
		trace.ServiceName = rootSpan.ServiceName
	} else if len(spans) > 0 {
		trace.Namespace = spans[0].Namespace
		trace.ServiceName = spans[0].ServiceName
	}

	return trace
}
