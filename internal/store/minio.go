package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
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

	// Local caches (collected by this pod instance only)
	localStats   map[string]*models.ServiceStats
	localTraces  map[string]*models.Trace
	localMu      sync.RWMutex
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
		localStats:   make(map[string]*models.ServiceStats),
		localTraces:  make(map[string]*models.Trace),
	}

	go s.runGC()
	go s.runSync()
	return s, nil
}

// Close shuts down the store
func (s *Store) Close() error {
	return nil // MinIO client doesn't need explicit close
}

// SaveSpan writes a span to MinIO
func (s *Store) SaveSpan(span *models.Span) error {
	s.enrichSpanMetadata(span)

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

	s.updateLocalStats(span)
	s.updateLocalRecentTraces(span)
	return nil
}

// enrichSpanMetadata fills in missing metadata like db.system for uninstrumented databases
func (s *Store) enrichSpanMetadata(span *models.Span) {
	if span == nil {
		return
	}
	if span.Attributes == nil {
		span.Attributes = make(map[string]string)
	}

	// Only process CLIENT spans
	isClient := span.Kind == models.SpanKindClient || span.Kind == "CLIENT"
	if !isClient {
		return
	}

	// 1. If db.system is already set and not empty, we are done
	if dbSys, ok := span.Attributes["db.system"]; ok && dbSys != "" {
		return
	}

	// 2. Identify target addresses, service name, and ports
	peerName := strings.ToLower(span.Attributes["net.peer.name"])
	if peerName == "" {
		peerName = strings.ToLower(span.Attributes["server.address"])
	}
	if peerName == "" {
		peerName = strings.ToLower(span.Attributes["peer.service"])
	}
	if peerName == "" {
		peerName = strings.ToLower(span.Attributes["net.peer.ip"])
	}
	if peerName == "" {
		peerName = strings.ToLower(span.Attributes["network.peer.address"])
	}

	portStr := span.Attributes["server.port"]
	if portStr == "" {
		portStr = span.Attributes["net.peer.port"]
	}
	if portStr == "" {
		portStr = span.Attributes["peer.port"]
	}

	inferredSystem := ""
	isDb := false
	isMsg := false

	// 3. Port-based inference
	if portStr != "" {
		switch portStr {
		case "5432", "5433":
			inferredSystem = "postgresql"
			isDb = true
		case "3306", "33060":
			inferredSystem = "mysql"
			isDb = true
		case "6379":
			inferredSystem = "redis"
			isDb = true
		case "27017", "27018":
			inferredSystem = "mongodb"
			isDb = true
		case "9092":
			inferredSystem = "kafka"
			isMsg = true
		case "5672", "15672":
			inferredSystem = "rabbitmq"
			isMsg = true
		case "1433":
			inferredSystem = "mssql"
			isDb = true
		case "1521":
			inferredSystem = "oracle"
			isDb = true
		case "9200", "9300":
			inferredSystem = "elasticsearch"
			isDb = true
		}
	}

	// Helper to match database substrings
	checkSubstrings := func(str string) (string, bool, bool) {
		if str == "" {
			return "", false, false
		}
		if strings.Contains(str, "postgres") || (strings.Contains(str, "pg") && !strings.Contains(str, "png") && !strings.Contains(str, "page")) {
			return "postgresql", true, false
		}
		if strings.Contains(str, "redis") {
			return "redis", true, false
		}
		if strings.Contains(str, "mysql") {
			return "mysql", true, false
		}
		if strings.Contains(str, "mongo") {
			return "mongodb", true, false
		}
		if strings.Contains(str, "oracle") {
			return "oracle", true, false
		}
		if strings.Contains(str, "mssql") || strings.Contains(str, "sqlserver") {
			return "mssql", true, false
		}
		if strings.Contains(str, "kafka") {
			return "kafka", false, true
		}
		if strings.Contains(str, "rabbitmq") || strings.Contains(str, "amqp") {
			return "rabbitmq", false, true
		}
		if strings.Contains(str, "elasticsearch") || strings.Contains(str, "elastic") {
			return "elasticsearch", true, false
		}
		if strings.Contains(str, "db") || strings.Contains(str, "database") || strings.Contains(str, "sql") {
			return "database", true, false
		}
		return "", false, false
	}

	// 4. Substring-based inference from peer name / service / span name
	if inferredSystem == "" {
		if sys, db, msg := checkSubstrings(strings.ToLower(span.Name)); sys != "" {
			inferredSystem = sys
			isDb = db
			isMsg = msg
		}
	}
	if inferredSystem == "" && peerName != "" {
		if sys, db, msg := checkSubstrings(peerName); sys != "" {
			inferredSystem = sys
			isDb = db
			isMsg = msg
		}
	}

	// 5. Special case: explicitly check database IP addresses like the user's "10.254.5.30"
	if inferredSystem == "" && (peerName == "10.254.5.30" || strings.Contains(peerName, "10.254.5.30")) {
		inferredSystem = "database"
		isDb = true
	}

	// 6. Explicit check if database attributes (like db.statement or db.name) exist
	_, hasDbName := span.Attributes["db.name"]
	_, hasDbStmt := span.Attributes["db.statement"]
	if (hasDbName || hasDbStmt) && inferredSystem == "" {
		inferredSystem = "database"
		isDb = true
	}

	// 7. Apply the inferred attributes
	if isDb && inferredSystem != "" {
		span.Attributes["db.system"] = inferredSystem
	} else if isMsg && inferredSystem != "" {
		span.Attributes["messaging.system"] = inferredSystem
	}
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
	s.enrichSpanMetadata(&span)
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
		if q.ServiceName != "" && trace.ServiceName != q.ServiceName {
			continue
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

		// Collect unique services involved in this trace
		svcSet := make(map[string]bool)
		for _, sp := range trace.Spans {
			svcSet[sp.ServiceName] = true
		}
		var svcs []string
		for svc := range svcSet {
			svcs = append(svcs, svc)
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
			TraceID:      trace.TraceID,
			ServiceName:  trace.ServiceName,
			Namespace:    trace.Namespace,
			StartTime:    trace.StartTime,
			DurationMs:   trace.DurationMs,
			SpanCount:    trace.SpanCount,
			HasError:     trace.HasError,
			Services:     svcs,
			ErrorType:    errType,
			ErrorSummary: errSummary,
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

// runGC cleans up old in-memory local traces to prevent OOM
func (s *Store) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.localMu.Lock()
		cutoff := time.Now().Add(-1 * time.Hour)
		for id, trace := range s.localTraces {
			if trace.StartTime.Before(cutoff) {
				delete(s.localTraces, id)
			}
		}
		s.localMu.Unlock()
	}
}

// PodState represents the cache snapshot of a single replica pod
type PodState struct {
	Stats        map[string]*models.ServiceStats `json:"stats"`
	RecentTraces map[string]*models.Trace        `json:"recentTraces"`
	UpdatedAt    time.Time                       `json:"updatedAt"`
}

// TracePodInfo represents pod metadata extracted from ingested trace spans.
// This enables pod visibility for remote clusters without direct K8s API access.
type TracePodInfo struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	NodeName    string            `json:"nodeName"`
	ServiceName string            `json:"serviceName"`
	Labels      map[string]string `json:"labels,omitempty"`
	LastSeen    time.Time         `json:"lastSeen"`
}

// GetTracePodsByNamespace scans recent traces and extracts unique pod metadata
// from span resource attributes (k8s.pod.name, k8s.node.name).
// This provides pod discovery for namespaces on remote clusters that send
// OTEL traces but are not reachable via the local K8s API watcher.
func (s *Store) GetTracePodsByNamespace(namespace string) []TracePodInfo {
	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	podMap := make(map[string]*TracePodInfo)
	for _, trace := range s.recentTraces {
		for _, sp := range trace.Spans {
			if sp.PodName == "" {
				continue
			}
			if namespace != "" && sp.Namespace != namespace {
				continue
			}
			key := sp.Namespace + "/" + sp.PodName
			if existing, ok := podMap[key]; !ok || sp.StartTime.After(existing.LastSeen) {
				podMap[key] = &TracePodInfo{
					Name:        sp.PodName,
					Namespace:   sp.Namespace,
					NodeName:    sp.NodeName,
					ServiceName: sp.ServiceName,
					Labels: map[string]string{
						"app":     sp.ServiceName,
						"version": "v1.0",
					},
					LastSeen: sp.StartTime,
				}
			}
		}
	}

	result := make([]TracePodInfo, 0, len(podMap))
	for _, p := range podMap {
		result = append(result, *p)
	}
	return result
}


// runSync runs the background replication loop
func (s *Store) runSync() {
	// Sync immediately on startup
	s.syncState()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncState()
	}
}

// syncState handles serialization of local state and merging from other active replicas
func (s *Store) syncState() {
	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Write this pod's local state to MinIO
	s.localMu.RLock()
	state := PodState{
		Stats:        make(map[string]*models.ServiceStats),
		RecentTraces: make(map[string]*models.Trace),
		UpdatedAt:    time.Now(),
	}
	for k, v := range s.localStats {
		state.Stats[k] = v
	}
	for k, v := range s.localTraces {
		state.RecentTraces[k] = v
	}
	s.localMu.RUnlock()

	data, err := json.Marshal(state)
	if err == nil {
		objectName := fmt.Sprintf("state/pod-%s.json", hn)
		_, _ = s.client.PutObject(ctx, s.bucketName, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
	}

	// 2. Scan and merge states from all active replicas
	newStatsCache := make(map[string]*models.ServiceStats)
	newRecentTraces := make(map[string]*models.Trace)

	for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    "state/",
		Recursive: false,
	}) {
		if obj.Err != nil {
			continue
		}

		// Delete state files that haven't been updated for over an hour (dead pods)
		if obj.LastModified.Before(time.Now().Add(-1 * time.Hour)) {
			_ = s.client.RemoveObject(ctx, s.bucketName, obj.Key, minio.RemoveObjectOptions{})
			continue
		}

		// Download and parse replica state
		objReader, err := s.client.GetObject(ctx, s.bucketName, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		
		var pState PodState
		dec := json.NewDecoder(objReader)
		err = dec.Decode(&pState)
		objReader.Close()
		if err != nil {
			continue
		}

		// Merge stats
		for k, svc := range pState.Stats {
			existing, ok := newStatsCache[k]
			if !ok {
				existing = &models.ServiceStats{
					ServiceName:      svc.ServiceName,
					Namespace:        svc.Namespace,
					IsInfrastructure: svc.IsInfrastructure,
				}
				newStatsCache[k] = existing
			}
			totalReq := existing.RequestCount + svc.RequestCount
			if totalReq > 0 {
				existing.P50Ms = (existing.P50Ms*float64(existing.RequestCount) + svc.P50Ms*float64(svc.RequestCount)) / float64(totalReq)
				existing.P95Ms = (existing.P95Ms*float64(existing.RequestCount) + svc.P95Ms*float64(svc.RequestCount)) / float64(totalReq)
				existing.P99Ms = (existing.P99Ms*float64(existing.RequestCount) + svc.P99Ms*float64(svc.RequestCount)) / float64(totalReq)
			}
			existing.RequestCount = totalReq
			existing.ErrorCount += svc.ErrorCount
			if svc.LastSeen.After(existing.LastSeen) {
				existing.LastSeen = svc.LastSeen
			}
			if totalReq > 0 {
				existing.ErrorRate = float64(existing.ErrorCount) / float64(totalReq) * 100
			}
		}

		// Merge traces
		for k, t := range pState.RecentTraces {
			existing, ok := newRecentTraces[k]
			if !ok {
				newRecentTraces[k] = t
				continue
			}
			spanMap := make(map[string]*models.Span)
			for _, sp := range existing.Spans {
				spanMap[sp.SpanID] = sp
			}
			for _, sp := range t.Spans {
				spanMap[sp.SpanID] = sp
			}
			var combinedSpans []*models.Span
			for _, sp := range spanMap {
				combinedSpans = append(combinedSpans, sp)
			}
			newRecentTraces[k] = buildTrace(t.TraceID, combinedSpans)
		}
	}

	// Hot-swap global caches
	s.statsMu.Lock()
	s.statsCache = newStatsCache
	s.statsMu.Unlock()

	s.tracesMu.Lock()
	s.recentTraces = newRecentTraces
	s.tracesMu.Unlock()
}

func (s *Store) updateLocalStats(span *models.Span) {
	key := span.Namespace + ":" + span.ServiceName
	s.localMu.Lock()
	defer s.localMu.Unlock()

	stat, ok := s.localStats[key]
	if !ok {
		stat = &models.ServiceStats{
			ServiceName: span.ServiceName,
			Namespace:   span.Namespace,
		}
		s.localStats[key] = stat
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

func (s *Store) updateLocalRecentTraces(span *models.Span) {
	s.localMu.Lock()
	defer s.localMu.Unlock()

	trace, ok := s.localTraces[span.TraceID]
	if !ok {
		trace = &models.Trace{
			TraceID: span.TraceID,
		}
		s.localTraces[span.TraceID] = trace
	}
	
	exists := false
	for _, sp := range trace.Spans {
		if sp.SpanID == span.SpanID {
			exists = true
			break
		}
	}
	if !exists {
		trace.Spans = append(trace.Spans, span)
		updatedTrace := buildTrace(trace.TraceID, trace.Spans)
		s.localTraces[span.TraceID] = updatedTrace
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

// GetRecentSpans returns all spans from in-memory traces, optionally filtered by namespace
func (s *Store) GetRecentSpans(namespace string) []*models.Span {
	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	var spans []*models.Span
	for _, trace := range s.recentTraces {
		if namespace != "" && trace.Namespace != namespace {
			continue
		}
		spans = append(spans, trace.Spans...)
	}
	return spans
}

// getInfraNodeName constructs a specific and unique identifier for database/queue target nodes
func getInfraNodeName(span *models.Span, baseName string) string {
	if strings.ToLower(baseName) == "dns" {
		return "DNS"
	}

	dbName := span.Attributes["db.name"]
	msgDest := span.Attributes["messaging.destination"]
	if msgDest == "" {
		msgDest = span.Attributes["messaging.destination.name"]
	}
	if msgDest == "" {
		msgDest = span.Attributes["messaging.destination_name"]
	}
	if msgDest == "" {
		msgDest = span.Attributes["messaging.dest"]
	}

	peerName := span.Attributes["net.peer.name"]
	if peerName == "" {
		peerName = span.Attributes["server.address"]
	}
	if peerName == "" {
		peerName = span.Attributes["peer.service"]
	}
	if peerName == "" {
		peerName = span.Attributes["net.peer.ip"]
	}
	if peerName == "" {
		peerName = span.Attributes["network.peer.address"]
	}

	resourceName := dbName
	if resourceName == "" {
		resourceName = msgDest
	}

	resource := ""
	if peerName != "" && resourceName != "" {
		resource = peerName + "/" + resourceName
	} else if resourceName != "" {
		resource = resourceName
	} else if peerName != "" {
		resource = peerName
	} else {
		resource = span.ServiceName
	}

	return strings.ToLower(baseName) + " (" + resource + ")"
}

// getClientDependencyName parses target hostname/identity from HTTP/gRPC client spans
func getClientDependencyName(span *models.Span) string {
	if peer := span.Attributes["peer.service"]; peer != "" {
		return peer
	}
	
	if strings.Contains(strings.ToLower(span.Name), "vault") {
		return "vault"
	}
	if urlStr := span.Attributes["http.url"]; urlStr != "" && strings.Contains(strings.ToLower(urlStr), "vault") {
		return "vault"
	}

	if strings.Contains(strings.ToLower(span.Name), "dns") {
		return "dns"
	}

	host := span.Attributes["server.address"]
	if host == "" {
		host = span.Attributes["net.peer.name"]
	}
	if host == "" {
		host = span.Attributes["http.host"]
	}
	
	if host != "" {
		if idx := strings.Index(host, ":"); idx != -1 {
			return host[:idx]
		}
		return host
	}

	if urlStr := span.Attributes["http.url"]; urlStr != "" {
		if idx := strings.Index(urlStr, "://"); idx != -1 {
			rem := urlStr[idx+3:]
			if endIdx := strings.IndexAny(rem, ":/"); endIdx != -1 {
				return rem[:endIdx]
			}
			return rem
		}
	}

	if span.Name != "" {
		return span.Name
	}

	return "external"
}

// parseK8sServiceAndNamespace extracts the clean microservice name and namespace from a target address
func (s *Store) parseK8sServiceAndNamespace(host string) (string, string) {
	if host == "" {
		return "", ""
	}
	host = strings.ToLower(host)
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}
	if idx := strings.Index(host, "://"); idx != -1 {
		host = host[idx+3:]
	}
	if idx := strings.Index(host, "/"); idx != -1 {
		host = host[:idx]
	}

	if net.ParseIP(host) != nil {
		return host, ""
	}

	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return "", ""
	}

	first := parts[0]
	// If the service name ends with our standard suffixes, extract it and attempt to parse the namespace
	if strings.HasSuffix(first, "-backend") || strings.HasSuffix(first, "-frontend") || first == "gateway" || first == "ingress-nginx" {
		if len(parts) >= 2 {
			return first, parts[1]
		}
		return first, ""
	}

	// Case 1: service.namespace.svc.cluster.local or service.namespace.svc
	if len(parts) >= 3 && parts[2] == "svc" {
		return parts[0], parts[1]
	}

	// Case 2: service.namespace where namespace is a known namespace
	if len(parts) == 2 {
		ns := parts[1]
		s.statsMu.RLock()
		hasNs := false
		for key := range s.statsCache {
			if strings.HasPrefix(key, ns+":") {
				hasNs = true
				break
			}
		}
		s.statsMu.RUnlock()
		if hasNs {
			return parts[0], ns
		}
	}

	// Case 3: check if any subsequent part matches a known namespace
	if len(parts) > 2 {
		s.statsMu.RLock()
		defer s.statsMu.RUnlock()
		for i := 1; i < len(parts); i++ {
			ns := parts[i]
			for key := range s.statsCache {
				if strings.HasPrefix(key, ns+":") {
					return parts[0], ns
				}
			}
		}
	}

	return parts[0], ""
}

