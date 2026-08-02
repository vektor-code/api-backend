package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/shared/connstr"
	"github.com/kubetrace/shared/spanenrich"
)

// SaveSpan enriches a span and hands it to the active storage pipeline:
// ClickHouse mode publishes to Kafka (consumed by ingestor-backend into
// ClickHouse); legacy mode writes per-span JSON to MinIO and maintains the
// in-memory replication caches.
func (s *Store) SaveSpan(span *models.Span) error {
	s.enrichSpanMetadata(span)

	if s.chMode {
		if s.producer != nil {
			s.producer.enqueue(span)
		}
	} else {
		data, err := json.Marshal(span)
		if err != nil {
			return err
		}

		// S3 path: traces/namespace/service/traceID/spanID.json
		objectName := fmt.Sprintf("traces/%s/%s/%s/%s.json", span.Namespace, span.ServiceName, span.TraceID, span.SpanID)

		// Queue the upload task with drop-on-overflow logic to protect S3/MinIO disk I/O and prevent CPU iowait
		task := uploadTask{
			objectName:  objectName,
			data:        data,
			contentType: "application/json",
		}
		select {
		case s.uploadChan <- task:
		default:
			// Queue is full; discard S3 upload to protect system stability
		}

		s.updateLocalStats(span)
		s.updateLocalRecentTraces(span)
	}

	if span.Cluster != "" {
		s.clustersMu.Lock()
		s.detectedClusters[span.Cluster] = true
		s.clustersMu.Unlock()
	}
	return nil
}

// enrichSpanMetadata attaches the Kubernetes context only this service has, then
// hands the span to the shared enrichment pipeline.
//
// The classification that used to live here — several hundred lines of port
// tables, substring tests and SQL-dialect sniffing — was one of four
// independent copies, and it iterated the attribute map, so its answer varied
// with Go's map ordering. It now lives in github.com/kubetrace/shared, which
// the ingestor runs too.
func (s *Store) enrichSpanMetadata(span *models.Span) {
	if span == nil {
		return
	}
	if span.Attributes == nil {
		span.Attributes = make(map[string]string)
	}

	s.applyPodDatabaseHints(span)

	// The reported-pod fallback resolves a database name that the
	// instrumentation left blank or generic, using what the agent observed in
	// the pod's environment.
	if dbName := s.resolveDatabaseName(span); dbName != "" {
		span.Attributes["db.name"] = dbName
	}

	result := s.enricher.Enrich(span.Attributes, span.Name, string(span.Kind))

	if is3rdParty, toolName := s.isThirdPartySpan(span, result); is3rdParty {
		span.Attributes["external.service"] = toolName
		span.Attributes["external.service.is3rdparty"] = "true"
	}
}

// applyPodDatabaseHints copies the database coordinates the agent discovered in
// a pod's environment onto spans emitted by that pod. This is the one piece of
// enrichment the ingestor cannot do, because it has no Kubernetes client.
func (s *Store) applyPodDatabaseHints(span *models.Span) {
	if span.PodName == "" || span.Namespace == "" {
		return
	}
	for _, pod := range s.GetReportedPods(span.Namespace) {
		if pod.Name != span.PodName {
			continue
		}
		if span.Attributes["db.name"] == "" && pod.DatabaseName != "" {
			span.Attributes["db.name"] = pod.DatabaseName
		}
		if span.Attributes["net.peer.name"] == "" && span.Attributes["server.address"] == "" && pod.DatabaseHost != "" {
			span.Attributes["server.address"] = pod.DatabaseHost
		}
		if span.Attributes["net.peer.port"] == "" && span.Attributes["server.port"] == "" && pod.DatabasePort != "" {
			span.Attributes["server.port"] = pod.DatabasePort
		}
		return
	}
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
			Cluster:     span.Cluster,
		}
		s.statsCache[key] = stat
	}
	if stat.Cluster == "" && span.Cluster != "" {
		stat.Cluster = span.Cluster
	}
	if (stat.Language == "" || stat.Language == "unknown") && span.Attributes != nil {
		if lang := DetectLanguageFromSpan(span); lang != "" {
			stat.Language = lang
		}
	}

	stat.RequestCount++
	if span.Status == models.SpanStatusError {
		stat.ErrorCount++
	}
	stat.LastSeen = time.Now()
	updateLatencyEstimates(stat, span.DurationMs)
	finalizeServiceStats(stat)
}

// DetectLanguageFromSpan extracts runtime languages dynamically from span metrics and library telemetry metadata
func DetectLanguageFromSpan(span *models.Span) string {
	if span == nil || span.Attributes == nil {
		return ""
	}

	// 1. Standard telemetry SDK language resource attributes
	if lang, ok := span.Attributes["telemetry.sdk.language"]; ok && lang != "" {
		return cleanLanguage(lang)
	}

	// 2. Process runtime name (e.g. openjdk, go, node)
	if rt, ok := span.Attributes["process.runtime.name"]; ok && rt != "" {
		return cleanLanguage(rt)
	}

	// 3. OTel Scope / Instrumentation Library Name
	if scope, ok := span.Attributes["otel.library.name"]; ok && scope != "" {
		sLower := strings.ToLower(scope)
		if strings.Contains(sLower, "java") {
			return "java"
		}
		if strings.Contains(sLower, "node") || strings.Contains(sLower, "express") || strings.Contains(sLower, "nextjs") || strings.Contains(sLower, "hapi") || strings.Contains(sLower, "koa") || strings.Contains(sLower, "js") {
			return "nodejs"
		}
		if strings.Contains(sLower, "python") || strings.Contains(sLower, "flask") || strings.Contains(sLower, "django") || strings.Contains(sLower, "fastapi") {
			return "python"
		}
		if strings.Contains(sLower, "go.opentelemetry") || strings.Contains(sLower, "otel/go") {
			return "go"
		}
		if strings.Contains(sLower, "dotnet") || strings.Contains(sLower, "aspnet") || strings.Contains(sLower, "microsoft") {
			return "dotnet"
		}
		if strings.Contains(sLower, "php") {
			return "php"
		}
	}

	// 4. Attribute Key and Value heuristics (nested framework libraries)
	for k, v := range span.Attributes {
		kLower := strings.ToLower(k)
		vLower := strings.ToLower(v)

		// Java specific attributes
		if strings.HasPrefix(kLower, "java.") || strings.Contains(kLower, "jvm.") || strings.Contains(vLower, "spring-boot") || strings.Contains(vLower, "hibernate") {
			return "java"
		}
		// Node.js specific attributes
		if strings.HasPrefix(kLower, "nodejs.") || strings.Contains(kLower, "express.") || strings.Contains(kLower, "javascript.") {
			return "nodejs"
		}
		// Python specific attributes
		if strings.Contains(kLower, "python.") || strings.Contains(vLower, "wsgi") || strings.Contains(vLower, "django") || strings.Contains(vLower, "flask") {
			return "python"
		}
		// Go specific attributes
		if strings.Contains(kLower, "go.runtime") || strings.Contains(kLower, "goroutine") {
			return "go"
		}
		// Dotnet specific attributes
		if strings.HasPrefix(kLower, "dotnet.") || strings.Contains(kLower, "aspnetcore.") || strings.Contains(vLower, "microsoft.aspnetcore") {
			return "dotnet"
		}
		// PHP specific attributes
		if strings.HasPrefix(kLower, "php.") || strings.Contains(vLower, "laravel") || strings.Contains(vLower, "symfony") {
			return "php"
		}
	}

	// 5. Name heuristics as fallback
	sName := strings.ToLower(span.ServiceName)
	if strings.Contains(sName, "java") || strings.Contains(sName, "spring") || strings.Contains(sName, "boot") {
		return "java"
	}
	if strings.Contains(sName, "node") || strings.Contains(sName, "express") || strings.Contains(sName, "javascript") || strings.Contains(sName, "typescript") {
		return "nodejs"
	}
	if strings.Contains(sName, "python") || strings.Contains(sName, "django") || strings.Contains(sName, "flask") || strings.Contains(sName, "fastapi") {
		return "python"
	}
	if strings.Contains(sName, "golang") || strings.Contains(sName, "go-") || strings.HasSuffix(sName, "-go") {
		return "go"
	}
	if strings.Contains(sName, "dotnet") || strings.Contains(sName, "csharp") || strings.Contains(sName, "aspnet") {
		return "dotnet"
	}
	if strings.Contains(sName, "php") || strings.Contains(sName, "laravel") || strings.Contains(sName, "symfony") {
		return "php"
	}

	return ""
}

func cleanLanguage(lang string) string {
	l := strings.ToLower(lang)
	if strings.Contains(l, "java") {
		return "java"
	}
	if strings.Contains(l, "node") || strings.Contains(l, "js") || strings.Contains(l, "javascript") || strings.Contains(l, "typescript") {
		return "nodejs"
	}
	if strings.Contains(l, "python") || strings.Contains(l, "cpython") {
		return "python"
	}
	if strings.Contains(l, "go") || strings.Contains(l, "golang") {
		return "go"
	}
	if strings.Contains(l, "dotnet") || strings.Contains(l, "c#") || strings.Contains(l, "csharp") {
		return "dotnet"
	}
	if strings.Contains(l, "php") {
		return "php"
	}
	return l
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

func (s *Store) updateLocalStats(span *models.Span) {
	key := span.Namespace + ":" + span.ServiceName
	s.localMu.Lock()
	defer s.localMu.Unlock()

	stat, ok := s.localStats[key]
	if !ok {
		stat = &models.ServiceStats{
			ServiceName: span.ServiceName,
			Namespace:   span.Namespace,
			Cluster:     span.Cluster,
		}
		s.localStats[key] = stat
	}
	if stat.Cluster == "" && span.Cluster != "" {
		stat.Cluster = span.Cluster
	}

	stat.RequestCount++
	if span.Status == models.SpanStatusError {
		stat.ErrorCount++
	}
	stat.LastSeen = time.Now()
	updateLatencyEstimates(stat, span.DurationMs)
	finalizeServiceStats(stat)
}

func (s *Store) updateLocalRecentTraces(span *models.Span) {
	s.localMu.Lock()
	defer s.localMu.Unlock()

	trace, ok := s.localTraces[span.TraceID]
	if !ok {
		// Cap in-memory traces map to protect container from OOM under high telemetry throughput
		if len(s.localTraces) >= s.maxTraces {
			for id := range s.localTraces {
				delete(s.localTraces, id)
				break
			}
		}
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

// isRootParentID returns true if the parent span ID indicates this is a root span.
// OTLP encodes a missing parent as zero bytes; fmt.Sprintf("%x") turns that into "0".
func isRootParentID(id string) bool {
	if id == "" {
		return true
	}
	for _, c := range id {
		if c != '0' {
			return false
		}
	}
	return true
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
	cluster := ""

	for _, sp := range spans {
		if isRootParentID(sp.ParentSpanID) {
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
		if sp.Cluster != "" {
			cluster = sp.Cluster
		}
	}

	// A trace whose true root was never stored is still worth showing. That
	// happens routinely: a browser propagates a traceparent without exporting
	// its own span, or the caller sits in a namespace that ingest drops. The
	// entry span — the earliest orphan, preferring an inbound SERVER span —
	// stands in, so the trace lists under a real operation name instead of a
	// blank row.
	if rootSpan == nil {
		rootSpan = entrySpan(spans)
		trace.Partial = rootSpan != nil
	}

	trace.RootSpan = rootSpan
	trace.StartTime = minStart
	trace.EndTime = maxEnd
	trace.HasError = hasError
	trace.DurationMs = float64(maxEnd.Sub(minStart).Microseconds()) / 1000.0
	trace.Cluster = cluster

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
	activeMap := s.GetApplicationActivationMap()

	s.tracesMu.RLock()
	defer s.tracesMu.RUnlock()

	var spans []*models.Span
	for _, trace := range s.recentTraces {
		for _, sp := range trace.Spans {
			if namespace != "" && sp.Namespace != namespace {
				continue
			}
			if enabled, exists := activeMap[sp.Namespace+":"+sp.ServiceName]; exists && !enabled {
				continue
			}
			spans = append(spans, sp)
		}
	}
	return spans
}

// resolveDatabaseName recovers the database a span targeted when the
// instrumentation did not name it, working from the newer and older attribute
// spellings, then the connection string, then the statement.
//
// The previous version ended with a hardcoded correction for one specific
// truncated database name at one specific site. Site-specific facts belong in
// the reported-pod data or the dependency ruleset, not in a compiled-in
// string replacement.
func (s *Store) resolveDatabaseName(span *models.Span) string {
	if span == nil || span.Attributes == nil {
		return ""
	}
	attrs := span.Attributes

	dbName := firstAttr(attrs, "db.name", "db.namespace", "db.instance")

	if dbName == "" {
		for _, key := range []string{"db.connection_string", "db.url", "db.dsn"} {
			if conn := attrs[key]; conn != "" {
				if info := connstr.Parse(conn); info.Database != "" {
					dbName = info.Database
					break
				}
			}
		}
	}

	if dbName == "" {
		if stmt := firstAttr(attrs, "db.statement", "db.query.text"); stmt != "" {
			dbName = databaseFromUseStatement(stmt)
		}
	}

	dbName = strings.Trim(strings.TrimSpace(strings.ToLower(dbName)), "'\"` ")

	// A blank or engine-default name tells us nothing; prefer what the agent
	// observed in the pod environment for this service.
	if dbName == "" || dbName == "unknown" || dbName == "postgres" || dbName == "postgresql" {
		if reported := s.GetReportedDatabaseForService(span.Namespace, span.ServiceName); reported != "" {
			return reported
		}
	}
	return dbName
}

// databaseFromUseStatement reads the target of a USE statement.
func databaseFromUseStatement(stmt string) string {
	fields := strings.Fields(strings.TrimSpace(stmt))
	if len(fields) < 2 || !strings.EqualFold(fields[0], "use") {
		return ""
	}
	return strings.Trim(fields[1], `;"'`)
}

// isThirdPartySpan reports whether a client span left the cluster for a
// third-party API. Spans already identified as a database, cache or broker are
// infrastructure, not third-party tools.
//
// Host matching, the internal DNS suffixes and the private network ranges all
// come from the dependency ruleset; only the "is this one of our own services"
// test lives here, because only the store knows the service inventory.
func (s *Store) isThirdPartySpan(span *models.Span, dep spanenrich.Result) (bool, string) {
	if span.Kind != models.SpanKindClient && span.Kind != "CLIENT" {
		return false, ""
	}
	if dep.System != "" {
		return false, ""
	}
	if span.Attributes["db.system"] != "" || span.Attributes["messaging.system"] != "" {
		return false, ""
	}

	host := firstAttr(span.Attributes, "server.address", "net.peer.name", "http.host")
	if host == "" {
		host = hostFromURL(firstAttr(span.Attributes, "http.url", "url.full"))
	}
	if host == "" {
		return false, ""
	}

	toolName, ok := s.enricher.ClassifyExternal(host)
	if !ok {
		return false, ""
	}

	// A host that matches one of our own service names is internal traffic
	// reached through an external-looking address.
	cleanHost := strings.ToLower(host)
	if idx := strings.Index(cleanHost, ":"); idx != -1 {
		cleanHost = cleanHost[:idx]
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	for key := range s.statsCache {
		if parts := strings.SplitN(key, ":", 2); len(parts) == 2 && parts[1] == cleanHost {
			return false, ""
		}
	}

	return true, toolName
}

func hostFromURL(rawURL string) string {
	idx := strings.Index(rawURL, "://")
	if idx == -1 {
		return ""
	}
	rest := rawURL[idx+3:]
	if at := strings.LastIndex(rest, "@"); at != -1 {
		rest = rest[at+1:]
	}
	if end := strings.IndexAny(rest, ":/"); end != -1 {
		return rest[:end]
	}
	return rest
}

func firstAttr(attrs map[string]string, keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(attrs[key]); v != "" {
			return v
		}
	}
	return ""
}

// thirdPartyToolFor reads the third-party attribution recorded at ingest.
// Read paths must not re-derive it: the write path already decided, and a
// second opinion here is how the two used to disagree.
func thirdPartyToolFor(span *models.Span) (string, bool) {
	if span == nil || span.Attributes == nil {
		return "", false
	}
	if span.Attributes["external.service.is3rdparty"] != "true" {
		return "", false
	}
	name := span.Attributes["external.service"]
	return name, name != ""
}

// entrySpan picks the span that best represents where a partial trace was
// entered: among spans whose parent is absent from the trace, the earliest
// inbound one, falling back to the earliest of any kind.
func entrySpan(spans []*models.Span) *models.Span {
	if len(spans) == 0 {
		return nil
	}

	present := make(map[string]struct{}, len(spans))
	for _, sp := range spans {
		present[sp.SpanID] = struct{}{}
	}

	var bestServer, bestAny *models.Span
	for _, sp := range spans {
		if _, hasParent := present[sp.ParentSpanID]; hasParent {
			continue // not an orphan; its parent is in this trace
		}
		if bestAny == nil || sp.StartTime.Before(bestAny.StartTime) {
			bestAny = sp
		}
		if sp.Kind == models.SpanKindServer || sp.Kind == "SERVER" {
			if bestServer == nil || sp.StartTime.Before(bestServer.StartTime) {
				bestServer = sp
			}
		}
	}

	if bestServer != nil {
		return bestServer
	}
	if bestAny != nil {
		return bestAny
	}
	// Every span claims a parent inside the trace (a cycle, or duplicated ids).
	// Fall back to the earliest span so the trace still has an identity.
	earliest := spans[0]
	for _, sp := range spans[1:] {
		if sp.StartTime.Before(earliest.StartTime) {
			earliest = sp
		}
	}
	return earliest
}
