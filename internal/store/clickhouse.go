package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

const (
	chTimeLayout = "2006-01-02 15:04:05.999999"

	// Window of spans loaded into the in-memory caches that back the
	// service map, recent-spans and pod-discovery endpoints.
	chRefreshWindow    = 15 * time.Minute
	chRefreshSpanLimit = 20000

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

func chTraceIDPredicate(traceID string) string {
	traceID = strings.TrimSpace(traceID)
	if len(traceID) == 32 && isHexString(traceID) {
		return fmt.Sprintf("trace_id = '%s'", chEscape(strings.ToLower(traceID)))
	}
	return fmt.Sprintf("positionCaseInsensitive(trace_id, '%s') > 0", chEscape(traceID))
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

	if span.Namespace == "" {
		span.Namespace = span.Attributes["k8s.namespace.name"]
	}
	if span.Kind == "" {
		span.Kind = models.SpanKindInternal
	}
	return span
}

const chSpanColumns = "timestamp, trace_id, span_id, parent_span_id, service_name, operation_name, duration_ns, status_code, status_message, tags, namespace, cluster, kind, pod_name, node_name, events"

// runClickHouseRefresh replaces the MinIO gossip loop in ClickHouse mode:
// it periodically rebuilds the in-memory statsCache and recentTraces from
// ClickHouse so the service map, recent-spans and pod-discovery endpoints
// keep working — with every replica reading identical data.
func (s *Store) runClickHouseRefresh() {
	s.refreshFromClickHouse()
	_ = s.LoadDisabledNamespaces()
	_ = s.LoadConfiguredNamespaces()

	ticker := time.NewTicker(15 * time.Second)
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
	statsQuery := fmt.Sprintf(`SELECT
		namespace, service_name,
		any(cluster) AS cluster,
		count() AS request_count,
		countIf(status_code = 'ERROR') AS error_count,
		quantile(0.5)(duration_ns) / 1e6 AS p50,
		quantile(0.95)(duration_ns) / 1e6 AS p95,
		quantile(0.99)(duration_ns) / 1e6 AS p99,
		max(timestamp) AS last_seen,
		anyIf(tags['telemetry.sdk.language'], tags['telemetry.sdk.language'] != '') AS sdk_lang
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %d SECOND
	GROUP BY namespace, service_name`, int(chStatsWindow.Seconds()))

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
		finalizeServiceStats(stat)
		newStats[ns+":"+svc] = stat

		if stat.Cluster != "" {
			s.clustersMu.Lock()
			s.detectedClusters[stat.Cluster] = true
			s.clustersMu.Unlock()
		}
	}

	// 2. Recent spans for the service map / pod discovery caches (bounded).
	spansQuery := fmt.Sprintf(`SELECT %s
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %d SECOND
	ORDER BY timestamp DESC
	LIMIT %d`, chSpanColumns, int(chRefreshWindow.Seconds()), chRefreshSpanLimit)

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
			having = append(having, "countIf(status_code = 'ERROR') > 0")
		} else {
			having = append(having, "countIf(status_code = 'ERROR') = 0")
		}
	}
	if q.Operation != "" {
		having = append(having, fmt.Sprintf(
			"positionCaseInsensitive(anyIf(operation_name, parent_span_id = '' OR match(parent_span_id, '^0+$')), '%s') > 0",
			chEscape(q.Operation)))
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
			having = append(having, "countIf(status_code = 'ERROR') > 0")
		} else {
			having = append(having, "countIf(status_code = 'ERROR') = 0")
		}
	}
	if q.Operation != "" {
		having = append(having, fmt.Sprintf(
			"positionCaseInsensitive(anyIf(operation_name, parent_span_id = '' OR match(parent_span_id, '^0+$')), '%s') > 0",
			chEscape(q.Operation)))
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
	return where, having, start, end
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

	const rootPred = "parent_span_id = '' OR match(parent_span_id, '^0+$')"
	inner := fmt.Sprintf(`SELECT
		anyIf(service_name, %s) AS root_service,
		anyIf(operation_name, %s) AS root_op,
		(max(toUnixTimestamp64Milli(timestamp) + intDiv(duration_ns, 1000000)) - min(toUnixTimestamp64Milli(timestamp))) AS dur_ms,
		countIf(status_code = 'ERROR') > 0 AS has_error
	FROM kubetrace.spans
	WHERE %s
	GROUP BY trace_id`, rootPred, rootPred, strings.Join(where, " AND "))
	if len(having) > 0 {
		inner += "\n\tHAVING " + strings.Join(having, " AND ")
	}

	query := fmt.Sprintf(`SELECT
		root_service AS service_name,
		root_op AS operation_name,
		count() AS cnt,
		countIf(has_error) AS err_cnt,
		avg(dur_ms) AS avg_ms,
		quantile(0.95)(dur_ms) AS p95_ms
	FROM (%s)
	WHERE root_service != ''
	GROUP BY service_name, operation_name
	ORDER BY (avg_ms * cnt) DESC
	LIMIT %d`, inner, limit)

	rows, err := s.chQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	out := make([]*models.EndpointStat, 0, len(rows))
	for _, row := range rows {
		out = append(out, &models.EndpointStat{
			ServiceName:   chString(row["service_name"]),
			OperationName: chString(row["operation_name"]),
			Count:         chInt(row["cnt"]),
			ErrorCount:    chInt(row["err_cnt"]),
			AvgDurationMs: chFloat(row["avg_ms"]),
			P95DurationMs: chFloat(row["p95_ms"]),
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
