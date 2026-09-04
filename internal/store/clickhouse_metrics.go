package store

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/shared/httproute"
)

func (s *Store) chLoadServiceStats(ctx context.Context) (map[string]*models.ServiceStats, bool, error) {
	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		namespace, service_name,
		any(cluster) AS cluster,
		toInt64(round(sum(count))) AS request_count,
		toInt64(round(sum(error_count))) AS error_count,
		quantileMerge(0.5)(duration_ns_p50) / 1e6 AS p50,
		quantileMerge(0.95)(duration_ns_p95) / 1e6 AS p95,
		quantileMerge(0.99)(duration_ns_p99) / 1e6 AS p99,
		max(last_seen) AS last_seen,
		anyLast(sdk_lang) AS sdk_lang
	FROM kubetrace.span_metrics
	WHERE bucket > now() - INTERVAL %d SECOND
		AND is_request = 1
	GROUP BY namespace, service_name`, int(chStatsWindow.Seconds())))
	if err == nil && len(rows) > 0 {
		return s.statsFromRows(rows), true, nil
	}

	eligible := httproute.CHRequestIdentityEligible("kind", "tags", "operation_name")
	raw, rawErr := s.chQuery(ctx, fmt.Sprintf(`SELECT
		namespace, service_name,
		any(cluster) AS cluster,
		countIf(%[1]s) AS request_count,
		countIf((`+chSpanErrorSQL()+`) AND (%[1]s)) AS error_count,
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
	GROUP BY namespace, service_name`, eligible, int(chStatsWindow.Seconds())))
	if rawErr != nil {
		if err != nil {
			return nil, false, err
		}
		return nil, false, rawErr
	}
	return s.statsFromRows(raw), false, nil
}

func (s *Store) statsFromRows(rows []map[string]any) map[string]*models.ServiceStats {
	newStats := make(map[string]*models.ServiceStats, len(rows))
	for _, row := range rows {
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
	return newStats
}

func (s *Store) chLoadTracePods(ctx context.Context) ([]TracePodInfo, error) {
	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		namespace,
		pod_name,
		any(node_name) AS node_name,
		any(service_name) AS service_name,
		max(timestamp) AS last_seen
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %d SECOND
		AND pod_name != ''
	GROUP BY namespace, pod_name`, int(chRefreshWindow.Seconds())))
	if err != nil {
		return nil, err
	}
	out := make([]TracePodInfo, 0, len(rows))
	for _, row := range rows {
		pod := chString(row["pod_name"])
		ns := chString(row["namespace"])
		if pod == "" {
			continue
		}
		out = append(out, TracePodInfo{
			Name:        pod,
			Namespace:   ns,
			NodeName:    chString(row["node_name"]),
			ServiceName: chString(row["service_name"]),
			Labels: map[string]string{
				"app":     chString(row["service_name"]),
				"version": "v1.0",
			},
			LastSeen: chTime(row["last_seen"]),
		})
	}
	return out, nil
}

func (s *Store) chLoadRecentTraces(ctx context.Context, stats map[string]*models.ServiceStats) map[string]*models.Trace {
	perNamespace := chRefreshSpanLimit
	if namespaceCount := countNamespaces(stats); namespaceCount > 1 {
		perNamespace = chRefreshSpanLimit / namespaceCount
		if perNamespace < chRefreshMinPerNamespace {
			perNamespace = chRefreshMinPerNamespace
		}
	}
	spanRows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT %s
	FROM kubetrace.spans
	WHERE timestamp > now64(6) - INTERVAL %d SECOND
	ORDER BY timestamp DESC
	LIMIT %d BY namespace
	LIMIT %d`, chSpanColumns, int(chRefreshWindow.Seconds()), perNamespace, chRefreshSpanLimit*2))
	if err != nil {
		log.Printf("[clickhouse] recent spans refresh failed: %v", err)
		return map[string]*models.Trace{}
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
	return newTraces
}

func (s *Store) chTimeseriesFromMetrics(result *TimeseriesData, bucketIdx map[int64]int, namespace string, allowedNs []string, from time.Time, step int) (*TimeseriesData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	where := []string{fmt.Sprintf("bucket >= toDateTime('%s')", from.Format("2006-01-02 15:04:05"))}
	if namespace != "" {
		where = append(where, fmt.Sprintf("namespace = '%s'", chEscape(namespace)))
	} else if allowedNs != nil {
		quoted := make([]string, 0, len(allowedNs))
		for _, ns := range allowedNs {
			quoted = append(quoted, "'"+chEscape(ns)+"'")
		}
		if len(quoted) == 0 {
			return result, nil
		}
		where = append(where, "namespace IN ("+strings.Join(quoted, ",")+")")
	}
	cond := strings.Join(where, " AND ")

	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		toUnixTimestamp(toStartOfInterval(bucket, INTERVAL %d SECOND)) AS b,
		toInt64(round(sum(count))) AS spans,
		toInt64(round(sum(error_count))) AS errors,
		sum(duration_ns_sum) / nullIf(sum(count), 0) / 1e6 AS avg_ms,
		quantileMerge(0.99)(duration_ns_p99) / 1e6 AS p99_ms,
		toInt64(round(sumIf(count, is_db = 1))) AS db_calls,
		sumIf(duration_ns_sum, is_db = 1) / nullIf(sumIf(count, is_db = 1), 0) / 1e6 AS db_avg_ms
	FROM kubetrace.span_metrics WHERE %s GROUP BY b ORDER BY b`, step, cond))
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	for _, row := range rows {
		if idx, ok := bucketIdx[chInt(row["b"])]; ok {
			bkt := &result.Buckets[idx]
			bkt.Spans = chInt(row["spans"])
			bkt.Errors = chInt(row["errors"])
			bkt.AvgMs = chFloat(row["avg_ms"])
			bkt.P99Ms = chFloat(row["p99_ms"])
			bkt.DbCalls = chInt(row["db_calls"])
			bkt.DbAvgMs = chFloat(row["db_avg_ms"])
		}
	}

	svcRows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		service_name, any(namespace) AS ns,
		toUnixTimestamp(toStartOfInterval(bucket, INTERVAL %d SECOND)) AS b,
		toInt64(round(sum(count))) AS spans,
		toInt64(round(sum(error_count))) AS errors
	FROM kubetrace.span_metrics WHERE %s GROUP BY service_name, b`, step, cond))
	if err != nil {
		return result, nil
	}
	fillTimeseriesHeatmap(result, bucketIdx, svcRows)
	return result, nil
}
