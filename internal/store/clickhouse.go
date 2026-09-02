package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/shared/httproute"
)

const (
	chTimeLayout = "2006-01-02 15:04:05.999999"

	// Window of spans loaded into the in-memory caches that back the
	// service map, recent-spans and pod-discovery endpoints.
	chRefreshWindow    = 15 * time.Minute
	chRefreshSpanLimit = 20000

	// Floor for the per-namespace share of that budget. A plain
	// "newest 20000 spans" cap allocates the cache by traffic volume, so a
	// busy namespace crowds quiet ones out entirely — at 88k spans per window
	// a low-volume namespace received well under 1% of the cache and rendered
	// as empty in the service map. Every namespace now gets its own slice.
	chRefreshMinPerNamespace = 250

	// Window used for service statistics aggregation.
	chStatsWindow = time.Hour

	// When retention is unlimited, keep default queries bounded unless the UI
	// sends an explicit startTime. This keeps "forever" storage cheap to operate.
	chDefaultForeverQueryHours = 168
)

var chHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

// chQuery executes a SQL query against ClickHouse and returns rows decoded
// from FORMAT JSON output.
func (s *Store) chQuery(ctx context.Context, query string) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", s.chURL, strings.NewReader(query+" FORMAT JSON"))
	if err != nil {
		return nil, err
	}
	if req.URL.User != nil {
		pass, _ := req.URL.User.Password()
		req.SetBasicAuth(req.URL.User.Username(), pass)
	}

	resp, err := chHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("clickhouse status %d: %s", resp.StatusCode, string(body))
	}

	var out struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("clickhouse decode: %w", err)
	}
	return out.Data, nil
}

// chExec runs a ClickHouse statement that returns no result set (DDL such as
// ALTER TABLE ... MODIFY TTL). Unlike chQuery it does not append FORMAT JSON.
func (s *Store) chExec(ctx context.Context, query string) error {
	req, err := http.NewRequestWithContext(ctx, "POST", s.chURL, strings.NewReader(query))
	if err != nil {
		return err
	}
	if req.URL.User != nil {
		pass, _ := req.URL.User.Password()
		req.SetBasicAuth(req.URL.User.Username(), pass)
	}

	resp, err := chHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse status %d: %s", resp.StatusCode, string(body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// coldVolumeExists reports whether the tiered storage policy with a MinIO-backed
// 'cold' volume is configured on the ClickHouse server. When it is, retention
// TTL moves aged parts to MinIO instead of deleting them; when it is not (e.g.
// the storage config has not been deployed yet), we degrade to a plain delete
// TTL so retention still works.
func (s *Store) coldVolumeExists(ctx context.Context) bool {
	rows, err := s.chQuery(ctx, "SELECT count() AS c FROM system.storage_policies WHERE policy_name = 'tiered' AND volume_name = 'cold'")
	if err != nil || len(rows) == 0 {
		return false
	}
	return chInt(rows[0]["c"]) > 0
}

// spansTTLExpr builds the TTL clause implementing tiered storage. Parts move to
// the MinIO-backed cold volume after hotHours; they are deleted only after
// retentionHours (0 = keep forever, so old traces stay queryable on MinIO
// indefinitely). When the cold volume is absent, the move clause is omitted.
func spansTTLExpr(hotHours, retentionHours int, hasCold bool) string {
	if hotHours <= 0 {
		hotHours = defaultHotHours
	}
	var parts []string
	if hasCold {
		parts = append(parts, fmt.Sprintf("toDateTime(timestamp) + toIntervalHour(%d) TO VOLUME 'cold'", hotHours))
	}
	if retentionHours > 0 {
		if retentionHours < hotHours {
			retentionHours = hotHours
		}
		parts = append(parts, fmt.Sprintf("toDateTime(timestamp) + toIntervalHour(%d) DELETE", retentionHours))
	}
	return strings.Join(parts, ", ")
}

// ApplyClickHouseTTL reconciles the spans table TTL with the configured
// retention. This is the control that actually governs what the trace page can
// read: recent spans stay on the fast hot disk, older parts are moved to MinIO,
// and nothing is deleted until the retention horizon (0 = never). The admin
// "Storage & Retention" setting drives it. No-op when not in ClickHouse mode.
func (s *Store) ApplyClickHouseTTL(retentionHours int) error {
	if !s.chMode {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hasCold := s.coldVolumeExists(ctx)
	ttl := spansTTLExpr(s.chHotHours, retentionHours, hasCold)
	if ttl == "" {
		// No cold tier and unlimited retention: drop any existing TTL so
		// ClickHouse keeps every span.
		if err := s.chExec(ctx, "ALTER TABLE kubetrace.spans REMOVE TTL"); err != nil {
			return fmt.Errorf("remove clickhouse ttl: %w", err)
		}
		log.Printf("[clickhouse] retention TTL removed (keep forever, no cold tier)")
		return nil
	}
	if err := s.chExec(ctx, "ALTER TABLE kubetrace.spans MODIFY TTL "+ttl); err != nil {
		return fmt.Errorf("apply clickhouse ttl: %w", err)
	}
	log.Printf("[clickhouse] retention reconciled: hot=%dh cold=%v retention=%dh", s.chHotHours, hasCold, retentionHours)
	return nil
}

// chEscape escapes a string literal for safe inlining into a ClickHouse query.
func chEscape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return v
}

// chSpanErrorSQL matches OTel ERROR spans and HTTP ≥ 500 responses even when
// status_code is UNSET (common with auto-instrumentation). Does not treat 4xx
// alone as errors so Explorer is not flooded by client 404 noise.
func chSpanErrorSQL() string {
	return `(status_code = 'ERROR' OR toInt64OrZero(tags['http.response.status_code']) >= 500 OR toInt64OrZero(tags['http.status_code']) >= 500)`
}

func chTraceIDPredicate(traceID string) string {
	traceID = strings.TrimSpace(traceID)
	if len(traceID) == 32 && isHexString(traceID) {
		return fmt.Sprintf("trace_id = '%s'", chEscape(strings.ToLower(traceID)))
	}
	return fmt.Sprintf("positionCaseInsensitive(trace_id, '%s') > 0", chEscape(traceID))
}

const chRootSpanPredicate = "parent_span_id = '' OR match(parent_span_id, '^0+$')"

// chEndpointRootPredicate is the root-span predicate plus transaction identity
// eligibility. Database CLIENT/PRODUCER roots and malformed HTTP SERVER spans
// stay in the trace; they must not mint Top Transactions identity. Duration
// aggregation is unchanged.
func chEndpointRootPredicate() string {
	return "((" + chRootSpanPredicate + ") AND " + httproute.CHTransactionIdentityEligible("kind", "tags", "dep_kind", "operation_name") + ")"
}

func chIncomingSpanPredicate() string {
	return httproute.CHRequestIdentityEligible("kind", "tags", "operation_name")
}

// chAnyIfTransactionExpr picks the earliest incoming SERVER/CONSUMER span
// anywhere in the trace (Datadog/Elastic behavior). If the trace has none, it
// falls back to an eligible INTERNAL root such as a cron job.
func chAnyIfTransactionExpr(valueExpr string) string {
	incoming := chIncomingSpanPredicate()
	fallback := chEndpointRootPredicate()
	return fmt.Sprintf("if(countIf(%s) > 0, argMinIf(%s, timestamp, %s), argMinIf(%s, timestamp, %s))", incoming, valueExpr, incoming, valueExpr, fallback)
}

func chRootTransactionNameExpr() string {
	return chAnyIfTransactionExpr("if(transaction_name != '', transaction_name, operation_name)")
}

func chOperationHaving(operation string) string {
	return fmt.Sprintf("positionCaseInsensitive(%s, '%s') > 0", chRootTransactionNameExpr(), chEscape(operation))
}

func (s *Store) chQueryBounds(q *models.SearchQuery, now time.Time) (time.Time, time.Time) {
	end := q.EndTime
	if end.IsZero() {
		end = now.Add(5 * time.Minute)
	}

	start := q.StartTime
	retentionHours := s.GetRetentionHours()
	if retentionHours > 0 {
		cutoff := now.Add(-time.Duration(retentionHours) * time.Hour)
		if start.IsZero() || start.Before(cutoff) {
			start = cutoff
		}
	} else if start.IsZero() {
		hours := getEnvInt("KUBETRACE_DEFAULT_QUERY_HOURS", chDefaultForeverQueryHours)
		if hours <= 0 {
			hours = chDefaultForeverQueryHours
		}
		start = now.Add(-time.Duration(hours) * time.Hour)
	}

	if start.After(end) {
		start = end
	}
	return start.UTC(), end.UTC()
}

func isHexString(value string) bool {
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return value != ""
}

func chString(v any) string {
	s, _ := v.(string)
	return s
}

func chFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}

func chInt(v any) int64 {
	return int64(chFloat(v))
}

func chTime(v any) time.Time {
	s, ok := v.(string)
	if !ok {
		return time.Time{}
	}
	t, err := time.ParseInLocation(chTimeLayout, s, time.UTC)
	if err != nil {
		return time.Time{}
	}
	return t
}

func chTags(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]string{}
	}
	tags := make(map[string]string, len(m))
	for k, val := range m {
		tags[k] = chString(val)
	}
	return tags
}

// chRowToSpan converts a ClickHouse row into a models.Span.
func chRowToSpan(row map[string]any) *models.Span {
	start := chTime(row["timestamp"])
	durNs := chInt(row["duration_ns"])

	span := &models.Span{
		TraceID:      chString(row["trace_id"]),
		SpanID:       chString(row["span_id"]),
		ParentSpanID: chString(row["parent_span_id"]),
		Name:         chString(row["operation_name"]),
		ServiceName:  chString(row["service_name"]),
		Namespace:    chString(row["namespace"]),
		Cluster:      chString(row["cluster"]),
		PodName:      chString(row["pod_name"]),
		NodeName:     chString(row["node_name"]),
		StartTime:    start,
		EndTime:      start.Add(time.Duration(durNs)),
		DurationMs:   float64(durNs) / 1e6,
		Status:       models.SpanStatus(chString(row["status_code"])),
		Kind:         models.SpanKind(chString(row["kind"])),
		Attributes:   chTags(row["tags"]),
		Error:        chString(row["status_message"]),
	}

	if evJSON := chString(row["events"]); evJSON != "" {
		var events []models.SpanEvent
		if err := json.Unmarshal([]byte(evJSON), &events); err == nil {
			span.Events = events
		}
	}
	promoteExceptionFromEvents(span)
	if raw := span.Attributes["otel.span.links"]; raw != "" {
		var links []models.SpanLink
		if err := json.Unmarshal([]byte(raw), &links); err == nil {
			span.Links = links
		}
	}

	if span.Namespace == "" {
		span.Namespace = span.Attributes["k8s.namespace.name"]
	}
	if span.Kind == "" {
		span.Kind = models.SpanKindInternal
	}
	if txn := chString(row["transaction_name"]); txn != "" {
		if span.Attributes == nil {
			span.Attributes = map[string]string{}
		}
		if span.Attributes["crnet.apm.transaction"] == "" {
			span.Attributes["crnet.apm.transaction"] = txn
		}
	}
	if sys := chString(row["dep_system"]); sys != "" {
		if span.Attributes == nil {
			span.Attributes = map[string]string{}
		}
		if span.Attributes["crnet.apm.dependency.system"] == "" {
			span.Attributes["crnet.apm.dependency.system"] = sys
		}
	}
	if kind := chString(row["dep_kind"]); kind != "" {
		if span.Attributes == nil {
			span.Attributes = map[string]string{}
		}
		if span.Attributes["crnet.apm.dependency.kind"] == "" {
			span.Attributes["crnet.apm.dependency.kind"] = kind
		}
	}
	return span
}

// promoteExceptionFromEvents lifts exception event fields onto the span so
// drawers and analyzers see a message even when status_message was empty
// (common with Python OTel auto-instrumentation).
func promoteExceptionFromEvents(span *models.Span) {
	if span == nil || len(span.Events) == 0 {
		return
	}
	var exc *models.SpanEvent
	for i := range span.Events {
		name := strings.ToLower(strings.TrimSpace(span.Events[i].Name))
		if name == "exception" || name == "error" {
			exc = &span.Events[i]
			break
		}
	}
	if exc == nil || exc.Attributes == nil {
		return
	}
	if span.Attributes == nil {
		span.Attributes = map[string]string{}
	}
	for _, k := range []string{"exception.type", "exception.message", "exception.stacktrace", "exception.escaped"} {
		if v := strings.TrimSpace(exc.Attributes[k]); v != "" && span.Attributes[k] == "" {
			span.Attributes[k] = v
		}
	}
	if msg := strings.TrimSpace(exc.Attributes["exception.message"]); msg == "" {
		msg = strings.TrimSpace(exc.Attributes["message"])
		if msg != "" && span.Attributes["exception.message"] == "" {
			span.Attributes["exception.message"] = msg
		}
	}
	if strings.TrimSpace(span.Error) == "" {
		if msg := strings.TrimSpace(span.Attributes["exception.message"]); msg != "" {
			span.Error = redactSecretFragments(msg)
		}
	}
	if span.Status != models.SpanStatusError {
		span.Status = models.SpanStatusError
	}
}

// redactSecretFragments strips common credential patterns from display/error text.
func redactSecretFragments(s string) string {
	lower := strings.ToLower(s)
	if !strings.Contains(lower, "bearer ") && !strings.Contains(lower, "api-key") &&
		!strings.Contains(lower, "api_key") && !strings.Contains(lower, "authorization:") {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		l := strings.ToLower(line)
		if strings.Contains(l, "bearer ") || strings.Contains(l, "api-key") ||
			strings.Contains(l, "api_key") || strings.Contains(l, "authorization:") {
			lines[i] = "[redacted]"
		}
	}
	return strings.Join(lines, "\n")
}

const chSpanColumns = "timestamp, trace_id, span_id, parent_span_id, service_name, operation_name, duration_ns, status_code, status_message, tags, namespace, cluster, kind, pod_name, node_name, events, transaction_name, dep_system, dep_kind, sample_weight"

// runClickHouseRefresh replaces the MinIO gossip loop in ClickHouse mode:
// it periodically rebuilds the in-memory statsCache and recentTraces from
// ClickHouse so the service map, recent-spans and pod-discovery endpoints
// keep working — with every replica reading identical data.
func (s *Store) runClickHouseRefresh() {
	s.refreshFromClickHouse()
	_ = s.LoadDisabledNamespaces()
	_ = s.LoadConfiguredNamespaces()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.refreshFromClickHouse()
		_ = s.LoadDisabledNamespaces()
		_ = s.LoadConfiguredNamespaces()
	}
}

func (s *Store) refreshFromClickHouse() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// 1. Service statistics with real percentiles.
	eligible := httproute.CHRequestIdentityEligible("kind", "tags", "operation_name")
	statsQuery := fmt.Sprintf(`SELECT
		namespace, service_name,
		any(cluster) AS cluster,
		countIf(%[1]s) AS request_count,
		countIf((` + chSpanErrorSQL() + `) AND (%[1]s)) AS error_count,
		quantileIf(0.5)(duration_ns, %[1]s) / 1e6 AS p50,
		quantileIf(0.95)(duration_ns, %[1]s) / 1e6 AS p95,
		quantileIf(0.99)(duration_ns, %[1]s) / 1e6 AS p99,
		max(timestamp) AS last_seen,
		if(
			anyIf(tags['telemetry.sdk.language'], tags['telemetry.sdk.language'] NOT IN ('', 'unknown', 'auto')) != '',
			anyIf(tags['telemetry.sdk.language'], tags['telemetry.sdk.language'] NOT IN ('', 'unknown', 'auto')),
			anyIf(tags['process.runtime.name'], tags['process.runtime.name'] NOT IN ('', 'unknown', 'auto'))
		) AS sdk_lang
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %[2]d SECOND
	GROUP BY namespace, service_name`, eligible, int(chStatsWindow.Seconds()))

	statsRows, err := s.chQuery(ctx, statsQuery)
	if err != nil {
		log.Printf("[clickhouse] stats refresh failed: %v", err)
		return
	}

	newStats := make(map[string]*models.ServiceStats, len(statsRows))
	for _, row := range statsRows {
		ns := chString(row["namespace"])
		svc := chString(row["service_name"])
		reqCount := chInt(row["request_count"])
		errCount := chInt(row["error_count"])
		stat := &models.ServiceStats{
			ServiceName:  svc,
			Namespace:    ns,
			Cluster:      chString(row["cluster"]),
			RequestCount: reqCount,
			ErrorCount:   errCount,
			P50Ms:        chFloat(row["p50"]),
			P95Ms:        chFloat(row["p95"]),
			P99Ms:        chFloat(row["p99"]),
			LastSeen:     chTime(row["last_seen"]),
		}
		if lang := chString(row["sdk_lang"]); lang != "" {
			stat.Language = cleanLanguage(lang)
		}
		if reqCount <= 0 || math.IsNaN(stat.P50Ms) || math.IsNaN(stat.P95Ms) || math.IsNaN(stat.P99Ms) {
			stat.P50Ms = 0
			stat.P95Ms = 0
			stat.P99Ms = 0
		}
		finalizeServiceStats(stat)
		newStats[ns+":"+svc] = stat

		if stat.Cluster != "" {
			s.clustersMu.Lock()
			s.detectedClusters[stat.Cluster] = true
			s.clustersMu.Unlock()
		}
	}

	// 2. Recent spans for the service map / pod discovery caches (bounded).
	//
	// The budget is divided between the namespaces actually reporting, using
	// LIMIT ... BY, so the cache represents every namespace rather than
	// whichever few produce the most traffic. namespaceCount comes from the
	// stats query above, which has no cap.
	perNamespace := chRefreshSpanLimit
	if namespaceCount := countNamespaces(newStats); namespaceCount > 1 {
		perNamespace = chRefreshSpanLimit / namespaceCount
		if perNamespace < chRefreshMinPerNamespace {
			perNamespace = chRefreshMinPerNamespace
		}
	}

	spansQuery := fmt.Sprintf(`SELECT %s
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %d SECOND
	ORDER BY timestamp DESC
	LIMIT %d BY namespace
	LIMIT %d`, chSpanColumns, int(chRefreshWindow.Seconds()), perNamespace, chRefreshSpanLimit*2)

	spanRows, err := s.chQuery(ctx, spansQuery)
	if err != nil {
		log.Printf("[clickhouse] recent spans refresh failed: %v", err)
		return
	}

	spansByTrace := make(map[string][]*models.Span)
	for _, row := range spanRows {
		sp := chRowToSpan(row)
		if sp.TraceID == "" {
			continue
		}
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
	}
	newTraces := make(map[string]*models.Trace, len(spansByTrace))
	for id, spans := range spansByTrace {
		newTraces[id] = buildTrace(id, spans)
	}

	// 3. Hot-swap caches.
	s.statsMu.Lock()
	s.statsCache = newStats
	s.statsMu.Unlock()

	s.tracesMu.Lock()
	s.recentTraces = newTraces
	s.tracesMu.Unlock()

	s.InvalidateServiceMapCache()
	s.rebuildPrecomputedStats()
}

// chSearchTraces implements SearchTraces on top of ClickHouse: a trace-level
// aggregation query selects matching trace IDs, then the spans of the final
// page are fetched to build the full list items.
func (s *Store) chSearchTraces(q *models.SearchQuery) ([]*models.TraceListItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	start, end := s.chQueryBounds(q, time.Now())

	where := []string{
		fmt.Sprintf("timestamp >= toDateTime64('%s', 6)", start.UTC().Format(chTimeLayout)),
		fmt.Sprintf("timestamp <= toDateTime64('%s', 6)", end.UTC().Format(chTimeLayout)),
	}
	if q.TraceID != "" {
		where = append(where, chTraceIDPredicate(q.TraceID))
	}

	var having []string
	if q.Namespace != "" {
		having = append(having, fmt.Sprintf("countIf(namespace = '%s') > 0", chEscape(q.Namespace)))
	}
	if q.Cluster != "" {
		having = append(having, fmt.Sprintf("countIf(cluster = '%s') > 0", chEscape(q.Cluster)))
	}
	if q.ServiceName != "" {
		target := strings.ToLower(q.ServiceName)
		baseSys := target
		dbName := ""
		if idx := strings.Index(target, "("); idx != -1 {
			baseSys = strings.TrimSpace(target[:idx])
			if endIdx := strings.Index(target, ")"); endIdx != -1 && endIdx > idx {
				dbName = strings.TrimSpace(target[idx+1 : endIdx])
			}
		}
		base := chEscape(baseSys)
		match := []string{
			fmt.Sprintf("lowerUTF8(service_name) = '%s'", base),
			fmt.Sprintf("lowerUTF8(tags['messaging.system']) = '%s'", base),
			fmt.Sprintf("lowerUTF8(tags['external.service']) = '%s'", base),
		}
		if dbName != "" {
			match = append(match, fmt.Sprintf("(lowerUTF8(tags['db.system']) = '%s' AND lowerUTF8(tags['db.name']) = '%s')", base, chEscape(dbName)))
		} else {
			match = append(match, fmt.Sprintf("lowerUTF8(tags['db.system']) = '%s'", base))
		}
		having = append(having, fmt.Sprintf("countIf(%s) > 0", strings.Join(match, " OR ")))
	}
	if q.HasError != nil {
		if *q.HasError {
			having = append(having, "countIf("+chSpanErrorSQL()+") > 0")
		} else {
			having = append(having, "countIf("+chSpanErrorSQL()+") = 0")
		}
	}
	if q.Operation != "" {
		having = append(having, chOperationHaving(q.Operation))
	}
	if q.TraceID == "" {
		having = append(having, fmt.Sprintf("countIf(%s) > 0", httproute.CHTransactionIdentityEligible("kind", "tags", "dep_kind", "operation_name")))
	}
	if q.MinSpans > 0 {
		having = append(having, fmt.Sprintf("count() >= %d", q.MinSpans))
	}
	if q.MinDurationMs > 0 {
		having = append(having, fmt.Sprintf("(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) >= %d", int64(q.MinDurationMs)))
	}
	if q.MaxDurationMs > 0 {
		having = append(having, fmt.Sprintf("(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) <= %d", int64(q.MaxDurationMs)))
	}
	if q.TraceID == "" {
		having = append(having, chRoleHaving(q)...)
	}

	limit := q.Limit
	if limit == 0 {
		limit = 50
	}

	idQuery := fmt.Sprintf(`SELECT trace_id, min(timestamp) AS trace_start
	FROM kubetrace.spans
	WHERE %s
	GROUP BY trace_id`, strings.Join(where, " AND "))
	if len(having) > 0 {
		idQuery += "\n\tHAVING " + strings.Join(having, " AND ")
	}
	idQuery += fmt.Sprintf("\n\tORDER BY trace_start DESC\n\tLIMIT %d OFFSET %d", limit, q.Offset)

	idRows, err := s.chQuery(ctx, idQuery)
	if err != nil {
		return nil, err
	}
	if len(idRows) == 0 {
		return []*models.TraceListItem{}, nil
	}

	orderedIDs := make([]string, 0, len(idRows))
	quoted := make([]string, 0, len(idRows))
	for _, row := range idRows {
		id := chString(row["trace_id"])
		orderedIDs = append(orderedIDs, id)
		quoted = append(quoted, "'"+chEscape(id)+"'")
	}

	// Fetch the spans of the selected traces, bounded to the query window plus
	// a small edge cushion so long root/child timing skew is still represented.
	spansQuery := fmt.Sprintf(`SELECT %s
	FROM kubetrace.spans
	WHERE trace_id IN (%s)
		AND timestamp >= toDateTime64('%s', 6) - INTERVAL 10 MINUTE
		AND timestamp <= toDateTime64('%s', 6) + INTERVAL 10 MINUTE
	LIMIT 100000`, chSpanColumns, strings.Join(quoted, ","), start.UTC().Format(chTimeLayout), end.UTC().Format(chTimeLayout))

	spanRows, err := s.chQuery(ctx, spansQuery)
	if err != nil {
		return nil, err
	}

	spansByTrace := make(map[string][]*models.Span, len(orderedIDs))
	for _, row := range spanRows {
		sp := chRowToSpan(row)
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
	}

	items := make([]*models.TraceListItem, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		spans := spansByTrace[id]
		if len(spans) == 0 {
			continue
		}
		items = append(items, s.buildTraceListItem(buildTrace(id, spans)))
	}
	return items, nil
}

// chEndpointFilters builds the trace-level WHERE and HAVING clauses shared by
// the trace search and the endpoint aggregation, so both apply identical
// filtering semantics.
func (s *Store) chEndpointFilters(q *models.SearchQuery) (where []string, having []string, start time.Time, end time.Time) {
	start, end = s.chQueryBounds(q, time.Now())

	where = []string{
		fmt.Sprintf("timestamp >= toDateTime64('%s', 6)", start.UTC().Format(chTimeLayout)),
		fmt.Sprintf("timestamp <= toDateTime64('%s', 6)", end.UTC().Format(chTimeLayout)),
	}
	if q.TraceID != "" {
		where = append(where, chTraceIDPredicate(q.TraceID))
	}

	if q.Namespace != "" {
		having = append(having, fmt.Sprintf("countIf(namespace = '%s') > 0", chEscape(q.Namespace)))
	}
	if q.Cluster != "" {
		having = append(having, fmt.Sprintf("countIf(cluster = '%s') > 0", chEscape(q.Cluster)))
	}
	if q.ServiceName != "" {
		target := strings.ToLower(q.ServiceName)
		baseSys := target
		dbName := ""
		if idx := strings.Index(target, "("); idx != -1 {
			baseSys = strings.TrimSpace(target[:idx])
			if endIdx := strings.Index(target, ")"); endIdx != -1 && endIdx > idx {
				dbName = strings.TrimSpace(target[idx+1 : endIdx])
			}
		}
		base := chEscape(baseSys)
		match := []string{
			fmt.Sprintf("lowerUTF8(service_name) = '%s'", base),
			fmt.Sprintf("lowerUTF8(tags['messaging.system']) = '%s'", base),
			fmt.Sprintf("lowerUTF8(tags['external.service']) = '%s'", base),
		}
		if dbName != "" {
			match = append(match, fmt.Sprintf("(lowerUTF8(tags['db.system']) = '%s' AND lowerUTF8(tags['db.name']) = '%s')", base, chEscape(dbName)))
		} else {
			match = append(match, fmt.Sprintf("lowerUTF8(tags['db.system']) = '%s'", base))
		}
		having = append(having, fmt.Sprintf("countIf(%s) > 0", strings.Join(match, " OR ")))
	}
	if q.HasError != nil {
		if *q.HasError {
			having = append(having, "countIf("+chSpanErrorSQL()+") > 0")
		} else {
			having = append(having, "countIf("+chSpanErrorSQL()+") = 0")
		}
	}
	if q.Operation != "" {
		having = append(having, chOperationHaving(q.Operation))
	}
	if q.MinSpans > 0 {
		having = append(having, fmt.Sprintf("count() >= %d", q.MinSpans))
	}
	if q.MinDurationMs > 0 {
		having = append(having, fmt.Sprintf("(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) >= %d", int64(q.MinDurationMs)))
	}
	if q.MaxDurationMs > 0 {
		having = append(having, fmt.Sprintf("(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) <= %d", int64(q.MaxDurationMs)))
	}
	if q.TraceID == "" {
		having = append(having, chRoleHaving(q)...)
	}
	return where, having, start, end
}

func chRoleHaving(q *models.SearchQuery) []string {
	if q == nil {
		return nil
	}
	var having []string
	probe := httproute.CHProbeSpan("tags", "operation_name")
	stream := httproute.CHStreamSpan("tags", "operation_name")
	if q.ExcludeProbes {
		having = append(having, fmt.Sprintf("(countIf(upperUTF8(kind) = 'SERVER' AND NOT %s) > 0 OR countIf(upperUTF8(kind) = 'SERVER') = 0)", probe))
	}
	if q.ExcludeStreams {
		having = append(having, fmt.Sprintf("(countIf(upperUTF8(kind) = 'SERVER' AND NOT %s) > 0 OR countIf(upperUTF8(kind) = 'SERVER') = 0)", stream))
	}
	if q.HasBody != nil {
		body := "(tags['http.request.body'] != '' OR tags['http.response.body'] != '')"
		if *q.HasBody {
			having = append(having, "countIf("+body+") > 0")
		} else {
			having = append(having, "countIf("+body+") = 0")
		}
	}
	if q.HttpMethod != "" {
		having = append(having, fmt.Sprintf(
			"countIf(upperUTF8(if(tags['http.request.method'] != '', tags['http.request.method'], tags['http.method'])) = '%s') > 0",
			chEscape(q.HttpMethod),
		))
	}
	return having
}

// chAggregateEndpoints returns stable per-endpoint aggregates over the whole
// query window. It first reduces each trace to its root service/operation,
// duration and error flag, then groups those by (root service, root operation).
// Because it aggregates the full window server-side, results don't jitter
// between refreshes and old endpoints keep showing until they age out.
func (s *Store) chAggregateEndpoints(q *models.SearchQuery) ([]*models.EndpointStat, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	where, having, _, _ := s.chEndpointFilters(q)

	// Exclude spans from namespaces that have been disabled in the Namespace
	// Manager, so their endpoints never surface in the aggregation.
	s.disabledNamespacesMu.RLock()
	if len(s.disabledNamespaces) > 0 {
		quoted := make([]string, 0, len(s.disabledNamespaces))
		for ns := range s.disabledNamespaces {
			quoted = append(quoted, "'"+chEscape(ns)+"'")
		}
		where = append(where, fmt.Sprintf("namespace NOT IN (%s)", strings.Join(quoted, ",")))
	}
	s.disabledNamespacesMu.RUnlock()

	limit := q.Limit
	if limit <= 0 {
		limit = 500
	}

	// transaction_name is the aggregation key, falling back to operation_name
	// for spans written before that column existed. Grouping on the raw span
	// name merged every endpoint of a service that names its server spans
	// after the bare HTTP method into one row.
	inner := fmt.Sprintf(`SELECT
		%s AS root_service,
		%s AS root_namespace,
		%s AS root_op,
		(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) AS dur_ms,
		countIf(`+chSpanErrorSQL()+`) > 0 AS has_error,
		maxIf(sample_weight, %s) AS weight
	FROM kubetrace.spans
	WHERE %s
	GROUP BY trace_id`, chAnyIfTransactionExpr("service_name"), chAnyIfTransactionExpr("namespace"), chAnyIfTransactionExpr("if(transaction_name != '', transaction_name, operation_name)"), chRootSpanPredicate, strings.Join(where, " AND "))
	if len(having) > 0 {
		inner += "\n\tHAVING " + strings.Join(having, " AND ")
	}

	// Counts are weighted by the sampling factor, so they describe traffic
	// rather than what happened to survive sampling. Latency percentiles are
	// unweighted: each stored trace is one observation of the distribution.
	query := fmt.Sprintf(`SELECT
		root_service AS service_name,
		root_namespace AS namespace,
		root_op AS operation_name,
		toInt64(round(sum(greatest(weight, 1)))) AS cnt,
		toInt64(round(sumIf(greatest(weight, 1), has_error))) AS err_cnt,
		avg(dur_ms) AS avg_ms,
		quantile(0.95)(dur_ms) AS p95_ms,
		count() AS sampled_cnt
	FROM (%s)
	WHERE root_service != '' AND root_op != '' AND root_op != '-'
	GROUP BY service_name, namespace, operation_name
	ORDER BY (avg_ms * cnt) DESC
	LIMIT %d`, inner, limit)

	rows, err := s.chQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	out := make([]*models.EndpointStat, 0, len(rows))
	for _, row := range rows {
		op := chString(row["operation_name"])
		if strings.TrimSpace(op) == "" || op == "-" {
			continue
		}
		out = append(out, &models.EndpointStat{
			ServiceName:   chString(row["service_name"]),
			Namespace:     chString(row["namespace"]),
			OperationName: op,
			Count:         chInt(row["cnt"]),
			ErrorCount:    chInt(row["err_cnt"]),
			AvgDurationMs: chFloat(row["avg_ms"]),
			P95DurationMs: chFloat(row["p95_ms"]),
			SampledCount:  chInt(row["sampled_cnt"]),
		})
	}
	return out, nil
}

// chGetTrace loads a full trace by exact ID from ClickHouse.
func (s *Store) chGetTrace(traceID string) (*models.Trace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	query := fmt.Sprintf(`SELECT %s
	FROM kubetrace.spans
	WHERE trace_id = '%s'
	LIMIT 50000`, chSpanColumns, chEscape(traceID))

	rows, err := s.chQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("trace not found: %s", traceID)
	}

	spans := make([]*models.Span, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		sp := chRowToSpan(row)
		// ReplacingMergeTree deduplicates asynchronously; drop duplicates here.
		if seen[sp.SpanID] {
			continue
		}
		seen[sp.SpanID] = true
		spans = append(spans, sp)
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].StartTime.Before(spans[j].StartTime) })

	return buildTrace(traceID, spans), nil
}

// chStringSlice converts a ClickHouse array column into a string slice.
func chStringSlice(v any) []string {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s := chString(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// countNamespaces counts the distinct namespaces present in a stats map keyed
// by "namespace:service".
func countNamespaces(stats map[string]*models.ServiceStats) int {
	seen := make(map[string]struct{}, len(stats))
	for _, stat := range stats {
		seen[stat.Namespace] = struct{}{}
	}
	return len(seen)
}
