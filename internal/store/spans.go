package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
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

// enrichSpanMetadata fills in missing metadata like db.system for uninstrumented databases
func (s *Store) enrichSpanMetadata(span *models.Span) {
	if span == nil {
		return
	}
	if span.Attributes != nil && span.Attributes["__vektor_enriched__"] == "true" {
		return
	}
	if span.Attributes == nil {
		span.Attributes = make(map[string]string)
	}
	span.Attributes["__vektor_enriched__"] = "true"

	// Try to resolve/extract database metadata from Kubernetes pod details
	if span.PodName != "" && span.Namespace != "" {
		pods := s.GetReportedPods(span.Namespace)
		for _, pod := range pods {
			if pod.Name == span.PodName {
				if span.Attributes["db.name"] == "" && pod.DatabaseName != "" {
					span.Attributes["db.name"] = pod.DatabaseName
				}
				if span.Attributes["net.peer.name"] == "" && span.Attributes["server.address"] == "" && pod.DatabaseHost != "" {
					span.Attributes["server.address"] = pod.DatabaseHost
				}
				if span.Attributes["net.peer.port"] == "" && span.Attributes["server.port"] == "" && pod.DatabasePort != "" {
					span.Attributes["server.port"] = pod.DatabasePort
				}
				break
			}
		}
	}

	// Try to resolve/extract database name if missing or generic
	if dbName := parseDbNameFromAttributes(s, span); dbName != "" {
		span.Attributes["db.name"] = dbName
	}

	// Only process CLIENT, PRODUCER, and CONSUMER spans
	isValidKind := span.Kind == models.SpanKindClient || span.Kind == "CLIENT" ||
		span.Kind == models.SpanKindProducer || span.Kind == "PRODUCER" ||
		span.Kind == models.SpanKindConsumer || span.Kind == "CONSUMER"
	if !isValidKind {
		return
	}

	// 1. Identify target addresses, service name, and ports
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

	// 2. Scan all span attributes to find explicit or implicit hints
	for k, v := range span.Attributes {
		valLower := strings.ToLower(v)
		keyLower := strings.ToLower(k)

		if keyLower == "db.system" && valLower != "" && valLower != "unknown" {
			inferredSystem = valLower
			isDb = true
			break
		}
		if keyLower == "messaging.system" && valLower != "" && valLower != "unknown" {
			if valLower == "message_bus" {
				inferredSystem = "rabbitmq"
			} else {
				inferredSystem = valLower
			}
			isMsg = true
			break
		}

		// Check for connection string or identifier patterns
		if strings.Contains(valLower, "redis://") || strings.Contains(valLower, "redis-") || valLower == "redis" {
			inferredSystem = "redis"
			isDb = true
		}
		if strings.Contains(valLower, "kafka") || strings.Contains(valLower, "broker-") {
			inferredSystem = "kafka"
			isMsg = true
		}
		if strings.Contains(valLower, "rabbitmq") || strings.Contains(valLower, "amqp://") || strings.Contains(valLower, "amqps://") {
			inferredSystem = "rabbitmq"
			isMsg = true
		}
		if strings.Contains(valLower, "minio") || strings.Contains(valLower, "s3.amazonaws") {
			inferredSystem = "minio"
			isDb = true
		}
		if strings.Contains(valLower, "apm") {
			inferredSystem = "apm"
			isDb = true
		}
		if inferredSystem == "" && strings.Contains(valLower, "vault") {
			inferredSystem = "vault"
			isDb = true
		}
		if strings.Contains(valLower, "clickhouse") {
			inferredSystem = "clickhouse"
			isDb = true
		}
		if strings.Contains(valLower, "liquibase") {
			inferredSystem = "liquibase"
			isDb = true
		}
		if strings.Contains(valLower, "nginx") {
			inferredSystem = "nginx"
			isDb = true
		}
		if strings.Contains(valLower, "kong") {
			inferredSystem = "kong"
			isDb = true
		}
	}

	// 2b. Hostname-based disambiguation — resolves conflicts where different
	// services share the same port (e.g. APM Server and Vault both use 8200).
	if inferredSystem == "" && peerName != "" {
		if strings.Contains(peerName, "apm") {
			inferredSystem = "apm"
			isDb = true
		}
	}

	// 3. Port-based inference (including SSL / custom ports)
	if inferredSystem == "" && portStr != "" {
		switch portStr {
		case "5432", "5433":
			inferredSystem = "postgresql"
			isDb = true
		case "3306", "33060":
			inferredSystem = "mysql"
			isDb = true
		case "6379", "6380":
			inferredSystem = "redis"
			isDb = true
		case "27017", "27018":
			inferredSystem = "mongodb"
			isDb = true
		case "9092", "9093", "9094", "29092", "39092":
			inferredSystem = "kafka"
			isMsg = true
		case "5671", "5672", "15672", "15671":
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
		case "8200", "8201":
			// Disambiguate: APM servers also commonly use port 8200
			if strings.Contains(peerName, "apm") {
				inferredSystem = "apm"
			} else {
				inferredSystem = "vault"
			}
			isDb = true
		case "9000", "9001":
			inferredSystem = "minio"
			isDb = true
		case "8123", "9440":
			inferredSystem = "clickhouse"
			isDb = true
		case "8000", "8443", "8001", "8444":
			inferredSystem = "kong"
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
		if strings.Contains(str, "minio") || strings.Contains(str, "s3") {
			return "minio", true, false
		}
		if strings.Contains(str, "apm") {
			return "apm", true, false
		}
		if strings.Contains(str, "vault") {
			return "vault", true, false
		}
		if strings.Contains(str, "clickhouse") {
			return "clickhouse", true, false
		}
		if strings.Contains(str, "liquibase") {
			return "liquibase", true, false
		}
		if strings.Contains(str, "nginx") {
			return "nginx", true, false
		}
		if strings.Contains(str, "kong") {
			return "kong", true, false
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

	// 5. Redis Command Name checks (when span name is exactly a command like GET/SET)
	if inferredSystem == "" {
		spanNameLower := strings.ToLower(span.Name)
		redisCmds := map[string]bool{
			"get": true, "set": true, "del": true, "keys": true, "ping": true, "exists": true,
			"hget": true, "hset": true, "hdel": true, "hgetall": true, "sadd": true, "srem": true,
			"lpush": true, "rpop": true, "incr": true, "decr": true, "expire": true, "ttl": true,
		}
		if redisCmds[spanNameLower] {
			// Corroborate with port, peer name, or database tags
			if portStr == "6379" || portStr == "6380" || strings.Contains(peerName, "redis") || strings.Contains(peerName, "cache") || span.Attributes["db.name"] != "" {
				inferredSystem = "redis"
				isDb = true
			}
		}
	}

	// 6. Generic Messaging destination heuristics
	if inferredSystem == "" {
		_, hasMsgDest := span.Attributes["messaging.destination"]
		if !hasMsgDest {
			_, hasMsgDest = span.Attributes["messaging.destination.name"]
		}
		if !hasMsgDest {
			_, hasMsgDest = span.Attributes["messaging.destination_name"]
		}
		if hasMsgDest {
			// Refine based on broker ports or host names
			if portStr == "9092" || portStr == "9093" || portStr == "9094" || strings.Contains(peerName, "kafka") {
				inferredSystem = "kafka"
				isMsg = true
			} else if portStr == "5672" || portStr == "5671" || portStr == "15672" || strings.Contains(peerName, "rabbit") || strings.Contains(peerName, "amqp") {
				inferredSystem = "rabbitmq"
				isMsg = true
			} else {
				inferredSystem = "rabbitmq"
				isMsg = true
			}
		}
	}

	// 7. Special case: explicitly check database IP addresses like the user's "10.254.5.30"
	if inferredSystem == "" && (peerName == "10.254.5.30" || strings.Contains(peerName, "10.254.5.30")) {
		inferredSystem = "database"
		isDb = true
	}

	// 8. Explicit check if database attributes (like db.statement or db.name) exist
	dbStmt := span.Attributes["db.statement"]
	_, hasDbName := span.Attributes["db.name"]
	if (hasDbName || dbStmt != "") && (inferredSystem == "" || inferredSystem == "database") {
		isDb = true

		// Attempt to refine generic "database" system into a specific brand using SQL syntax analysis
		if dbStmt != "" {
			q := strings.ToLower(dbStmt)

			// postgresql patterns (type casts, double quotes around table/column names, postgres functions, pg client prefixes)
			hasPgCast := strings.Contains(dbStmt, "::")
			hasDoubleQuote := strings.Contains(dbStmt, `"`) && !strings.Contains(dbStmt, "`")
			hasPgFunc := strings.Contains(q, "now()") || strings.Contains(q, "string_agg(") || strings.Contains(q, "coalesce(")
			hasPgParams := strings.Contains(dbStmt, "$1") || strings.Contains(dbStmt, "$2")

			if hasPgCast || hasPgParams || (hasDoubleQuote && (hasPgFunc || strings.Contains(q, "select ") || strings.Contains(q, "insert ") || strings.Contains(q, "update "))) {
				inferredSystem = "postgresql"
			} else if strings.Contains(dbStmt, "`") {
				inferredSystem = "mysql"
			} else if strings.Contains(dbStmt, "[") && strings.Contains(dbStmt, "]") && strings.Contains(q, "select") {
				inferredSystem = "mssql"
			} else if strings.Contains(q, " rownum") || strings.Contains(q, "sysdate") || strings.Contains(q, "nvl(") {
				inferredSystem = "oracle"
			} else if inferredSystem == "" {
				inferredSystem = "database"
			}
		} else if inferredSystem == "" {
			inferredSystem = "database"
		}
	}

	// 9. Apply the inferred attributes
	if isDb && inferredSystem != "" {
		span.Attributes["db.system"] = inferredSystem
	} else if isMsg && inferredSystem != "" {
		span.Attributes["messaging.system"] = inferredSystem
	}

	// 10. Detect if it is a 3rd-party external call
	if is3rdParty, toolName := s.isThirdPartySpan(span); is3rdParty {
		span.Attributes["external.service"] = toolName
		span.Attributes["external.service.is3rdparty"] = "true"
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

// parseDbNameFromAttributes resolves/extracts db name from OpenTelemetry tags
func parseDbNameFromAttributes(s *Store, span *models.Span) string {
	if span == nil || span.Attributes == nil {
		return ""
	}
	attrs := span.Attributes

	// 1. If db.name already exists and is non-empty, use it
	dbName := attrs["db.name"]

	// 2. Try db.instance (older semantic conventions)
	if dbName == "" {
		dbName = attrs["db.instance"]
	}

	// 3. Try db.namespace (newer semantic conventions)
	if dbName == "" {
		dbName = attrs["db.namespace"]
	}

	// 4. Try parsing from db.connection_string, db.url, or db.dsn
	if dbName == "" {
		connKeys := []string{"db.connection_string", "db.url", "db.dsn"}
		for _, key := range connKeys {
			if connStr, ok := attrs[key]; ok && connStr != "" {
				if parsed := extractDbNameFromConnStr(connStr); parsed != "" {
					dbName = parsed
					break
				}
			}
		}
	}

	// 5. Try parsing from db.statement (e.g. USE statement)
	if dbName == "" {
		if stmt, ok := attrs["db.statement"]; ok && stmt != "" {
			if parsed := extractDbNameFromSQL(stmt); parsed != "" {
				dbName = parsed
			}
		}
	}

	// Normalize database name
	dbName = strings.TrimSpace(strings.ToLower(dbName))
	dbName = strings.Trim(dbName, "'\"` ")

	// Correct any truncated names
	if strings.HasPrefix(dbName, "rmis_project_backend_d") {
		dbName = "rmis_project_backend_dev"
	}

	// Apply namespace/service fallbacks dynamically from reported pods config
	if dbName == "" || dbName == "unknown" || dbName == "postgres" || dbName == "postgresql" {
		if reportedDb := s.GetReportedDatabaseForService(span.Namespace, span.ServiceName); reportedDb != "" {
			dbName = reportedDb
		}
	}

	return dbName
}

// extractDbNameFromConnStr parses host URL and DSN/PDO formats to extract schema names
func extractDbNameFromConnStr(connStr string) string {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return ""
	}

	// Case A: standard URL scheme (e.g. postgresql://user:pass@host:port/dbname?query=...)
	if strings.Contains(connStr, "://") {
		parts := strings.SplitN(connStr, "://", 2)
		if len(parts) == 2 {
			rem := parts[1]
			atIdx := strings.LastIndex(rem, "@")
			hostPart := rem
			if atIdx != -1 {
				hostPart = rem[atIdx+1:]
			}

			slashIdx := strings.Index(hostPart, "/")
			if slashIdx != -1 {
				dbPart := hostPart[slashIdx+1:]
				if qIdx := strings.Index(dbPart, "?"); qIdx != -1 {
					dbPart = dbPart[:qIdx]
				}
				if hashIdx := strings.Index(dbPart, "#"); hashIdx != -1 {
					dbPart = dbPart[:hashIdx]
				}
				dbPart = strings.TrimSpace(dbPart)
				if dbPart != "" {
					return dbPart
				}
			}
		}
	}

	// Case B: DSN format (key=value pairs separated by semicolons, spaces, or commas)
	// Example: mysql:host=10.254.5.30;dbname=emuhasibatliq_dev
	normalized := connStr
	normalized = strings.ReplaceAll(normalized, ";", " ")
	normalized = strings.ReplaceAll(normalized, ",", " ")

	if colonIdx := strings.Index(normalized, ":"); colonIdx != -1 && !strings.Contains(normalized[:colonIdx], "=") {
		normalized = normalized[colonIdx+1:]
	}

	words := strings.Fields(normalized)
	for _, word := range words {
		if strings.Contains(word, "=") {
			kv := strings.SplitN(word, "=", 2)
			if len(kv) == 2 {
				k := strings.ToLower(strings.TrimSpace(kv[0]))
				v := strings.TrimSpace(kv[1])
				v = strings.Trim(v, `"'`)
				if k == "dbname" || k == "database" || k == "databasename" || k == "initial catalog" {
					if v != "" {
						return v
					}
				}
			}
		}
	}

	return ""
}

// extractDbNameFromSQL extracts database name from statements like USE
func extractDbNameFromSQL(stmt string) string {
	q := strings.ToLower(strings.TrimSpace(stmt))
	if strings.HasPrefix(q, "use ") {
		parts := strings.Fields(q)
		if len(parts) >= 2 {
			db := strings.Trim(parts[1], `;"'`)
			return db
		}
	}
	return ""
}

// isThirdPartySpan checks if a client span is calling a 3rd-party external tool/API
func (s *Store) isThirdPartySpan(span *models.Span) (bool, string) {
	if span.Kind != models.SpanKindClient && span.Kind != "CLIENT" {
		return false, ""
	}
	// If it has a db.system or messaging.system, it's database/infra, not a 3rd-party tool
	if span.Attributes["db.system"] != "" || span.Attributes["messaging.system"] != "" {
		return false, ""
	}

	host := span.Attributes["server.address"]
	if host == "" {
		host = span.Attributes["net.peer.name"]
	}
	if host == "" {
		host = span.Attributes["http.host"]
	}
	if host == "" {
		if urlStr := span.Attributes["http.url"]; urlStr != "" {
			if idx := strings.Index(urlStr, "://"); idx != -1 {
				rem := urlStr[idx+3:]
				if endIdx := strings.IndexAny(rem, ":/"); endIdx != -1 {
					host = rem[:endIdx]
				} else {
					host = rem
				}
			}
		}
	}

	if host == "" {
		return false, ""
	}

	hostLower := strings.ToLower(host)
	cleanHost := hostLower
	if idx := strings.Index(cleanHost, ":"); idx != -1 {
		cleanHost = cleanHost[:idx]
	}

	// Skip localhost, local IP addresses, and private cluster ranges
	if cleanHost == "localhost" || cleanHost == "127.0.0.1" || strings.HasPrefix(cleanHost, "10.") || strings.HasPrefix(cleanHost, "192.168.") || strings.HasPrefix(cleanHost, "172.") {
		return false, ""
	}

	if strings.HasSuffix(cleanHost, ".local") || strings.HasSuffix(cleanHost, ".svc") || strings.Contains(cleanHost, ".svc.cluster") || strings.HasSuffix(cleanHost, ".internal") {
		return false, ""
	}

	// Check if this host matches an internal microservice name
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	for key := range s.statsCache {
		parts := strings.Split(key, ":")
		if len(parts) == 2 && parts[1] == cleanHost {
			return false, ""
		}
	}

	hasDot := strings.Contains(cleanHost, ".")

	// Determine 3rd-party tool name
	toolName := ""
	if strings.Contains(cleanHost, "stripe") {
		toolName = "Stripe"
	} else if strings.Contains(cleanHost, "paypal") {
		toolName = "PayPal"
	} else if strings.Contains(cleanHost, "openai") {
		toolName = "OpenAI"
	} else if strings.Contains(cleanHost, "anthropic") {
		toolName = "Anthropic"
	} else if strings.Contains(cleanHost, "twilio") {
		toolName = "Twilio"
	} else if strings.Contains(cleanHost, "sendgrid") {
		toolName = "SendGrid"
	} else if strings.Contains(cleanHost, "mailgun") {
		toolName = "Mailgun"
	} else if strings.Contains(cleanHost, "sentry") {
		toolName = "Sentry"
	} else if strings.Contains(cleanHost, "github") {
		toolName = "GitHub"
	} else if strings.Contains(cleanHost, "slack") {
		toolName = "Slack"
	} else if strings.Contains(cleanHost, "discord") {
		toolName = "Discord"
	} else if strings.Contains(cleanHost, "auth0") {
		toolName = "Auth0"
	} else if strings.Contains(cleanHost, "okta") {
		toolName = "Okta"
	} else if strings.Contains(cleanHost, "mygov") {
		toolName = "MyGov"
	} else if strings.Contains(cleanHost, "egov") {
		toolName = "e-Gov"
	} else if strings.Contains(cleanHost, "google") || strings.Contains(cleanHost, "googleapis") {
		toolName = "Google API"
	} else if strings.Contains(cleanHost, "facebook") {
		toolName = "Facebook API"
	} else {
		if !hasDot {
			return false, ""
		}
		toolName = cleanHost
	}

	return true, toolName
}
