package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/shared/spanenrich"
)

// dbMetricsMaxRows bounds the result set. Query shapes are already collapsed by
// fingerprint, so a healthy system produces far fewer rows than this.
const dbMetricsMaxRows = 500

// dbMetricsMaxSampleErrors is how many distinct error messages are kept per
// query shape.
const dbMetricsMaxSampleErrors = 5

// GetDatabaseQueryMetrics aggregates database spans by query shape.
//
// In ClickHouse mode this is a single GROUP BY over the requested window. The
// previous implementation scanned the in-memory recent-traces cache, which in
// ClickHouse mode holds at most a 15-minute, 20 000-span slice, and it grouped
// by the raw statement text — so every distinct literal became its own row and
// the numbers described a truncated sample rather than the window asked for.
func (s *Store) GetDatabaseQueryMetrics(namespace string, allowedNs []string, windowMinutes int) ([]*models.DatabaseQueryMetric, error) {
	if windowMinutes <= 0 {
		windowMinutes = 60
	}
	if s.chMode {
		return s.chDatabaseQueryMetrics(namespace, allowedNs, windowMinutes)
	}
	return s.memDatabaseQueryMetrics(namespace, allowedNs), nil
}

func (s *Store) chDatabaseQueryMetrics(namespace string, allowedNs []string, windowMinutes int) ([]*models.DatabaseQueryMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	from := time.Now().UTC().Add(-time.Duration(windowMinutes) * time.Minute)

	where := []string{
		fmt.Sprintf("timestamp >= toDateTime64('%s', 6)", from.Format(chTimeLayout)),
		// Only stateful data stores. Gateways and secret stores are dependencies
		// too, but they are not database queries and do not belong here.
		"dep_kind IN ('database', 'cache')",
	}
	if namespace != "" {
		where = append(where, fmt.Sprintf("namespace = '%s'", chEscape(namespace)))
	} else if allowedNs != nil {
		if len(allowedNs) == 0 {
			return nil, nil
		}
		quoted := make([]string, 0, len(allowedNs))
		for _, ns := range allowedNs {
			quoted = append(quoted, "'"+chEscape(ns)+"'")
		}
		where = append(where, "namespace IN ("+strings.Join(quoted, ",")+")")
	}

	// Rows are ordered by total time, not by average: a 3 ms query run 100 000
	// times costs more than a 400 ms query run twice, and only total time
	// surfaces that.
	query := fmt.Sprintf(`SELECT
		db_query_fingerprint AS fingerprint,
		service_name,
		any(namespace) AS ns,
		any(dep_system) AS system,
		any(db_query_summary) AS summary,
		any(db_operation) AS operation,
		any(db_collection) AS collection,
		any(db_namespace) AS db_name,
		any(tags['db.statement']) AS sample_query,
		count() AS calls,
		countIf(status_code = 'ERROR') AS errors,
		avg(duration_ns) / 1e6 AS avg_ms,
		quantile(0.95)(duration_ns) / 1e6 AS p95_ms,
		quantile(0.99)(duration_ns) / 1e6 AS p99_ms,
		max(duration_ns) / 1e6 AS max_ms,
		sum(duration_ns) / 1e6 AS total_ms,
		arraySlice(arrayDistinct(groupArrayIf(64)(status_message, status_code = 'ERROR' AND status_message != '')), 1, %d) AS recent_errors
	FROM kubetrace.spans
	WHERE %s
	GROUP BY db_query_fingerprint, service_name
	ORDER BY total_ms DESC
	LIMIT %d`, dbMetricsMaxSampleErrors, strings.Join(where, " AND "), dbMetricsMaxRows)

	rows, err := s.chQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	out := make([]*models.DatabaseQueryMetric, 0, len(rows))
	for _, row := range rows {
		calls := chInt(row["calls"])
		if calls == 0 {
			continue
		}
		errors := chInt(row["errors"])

		metric := &models.DatabaseQueryMetric{
			Fingerprint:     chString(row["fingerprint"]),
			Query:           chString(row["sample_query"]),
			Summary:         chString(row["summary"]),
			System:          chString(row["system"]),
			Operation:       chString(row["operation"]),
			Collection:      chString(row["collection"]),
			DatabaseName:    chString(row["db_name"]),
			Service:         chString(row["service_name"]),
			Namespace:       chString(row["ns"]),
			CallCount:       calls,
			ErrorCount:      errors,
			ErrorRate:       percentage(errors, calls),
			AvgDurationMs:   chFloat(row["avg_ms"]),
			P95DurationMs:   chFloat(row["p95_ms"]),
			P99DurationMs:   chFloat(row["p99_ms"]),
			MaxDurationMs:   chFloat(row["max_ms"]),
			TotalDurationMs: chFloat(row["total_ms"]),
			RecentErrors:    chStringSlice(row["recent_errors"]),
		}
		if metric.Query == "" {
			metric.Query = metric.Summary
		}
		if s.IsNamespaceDisabled(metric.Namespace) {
			continue
		}
		out = append(out, metric)
	}
	return out, nil
}

// memDatabaseQueryMetrics serves the legacy MinIO mode from the in-memory
// cache. It groups on the fingerprint recorded at enrichment, so it collapses
// query shapes the same way the ClickHouse path does.
func (s *Store) memDatabaseQueryMetrics(namespace string, allowedNs []string) []*models.DatabaseQueryMetric {
	type key struct {
		fingerprint string
		service     string
	}
	agg := make(map[key]*models.DatabaseQueryMetric)
	durations := make(map[key][]float64)

	for _, span := range s.GetRecentSpans(namespace) {
		if !nsAllowedIn(allowedNs, span.Namespace) || s.IsNamespaceDisabled(span.Namespace) {
			continue
		}
		if span.Attributes == nil {
			continue
		}
		kind := span.Attributes[spanenrich.TagKind]
		if kind != "database" && kind != "cache" {
			continue
		}

		fingerprint := span.Attributes[spanenrich.TagFingerprint]
		summary := span.Attributes["db.query.summary"]
		if fingerprint == "" {
			// No statement was captured; group by summary so the row still
			// aggregates rather than fragmenting per span.
			fingerprint = summary
		}
		if fingerprint == "" {
			fingerprint = span.Name
		}

		k := key{fingerprint: fingerprint, service: span.ServiceName}
		metric, ok := agg[k]
		if !ok {
			metric = &models.DatabaseQueryMetric{
				Fingerprint:  fingerprint,
				Query:        firstAttr(span.Attributes, "db.statement", "db.query.text"),
				Summary:      summary,
				System:       span.Attributes["db.system"],
				Operation:    span.Attributes["db.operation"],
				Collection:   span.Attributes["db.collection.name"],
				DatabaseName: span.Attributes["db.name"],
				Service:      span.ServiceName,
				Namespace:    span.Namespace,
			}
			if metric.Query == "" {
				metric.Query = summary
			}
			agg[k] = metric
		}

		metric.CallCount++
		metric.TotalDurationMs += span.DurationMs
		if span.DurationMs > metric.MaxDurationMs {
			metric.MaxDurationMs = span.DurationMs
		}
		durations[k] = append(durations[k], span.DurationMs)

		if span.Status == models.SpanStatusError {
			metric.ErrorCount++
			msg := span.Error
			if msg == "" {
				msg = span.Attributes["error.message"]
			}
			if msg != "" && len(metric.RecentErrors) < dbMetricsMaxSampleErrors && !containsString(metric.RecentErrors, msg) {
				metric.RecentErrors = append(metric.RecentErrors, msg)
			}
		}
	}

	out := make([]*models.DatabaseQueryMetric, 0, len(agg))
	for k, metric := range agg {
		metric.AvgDurationMs = metric.TotalDurationMs / float64(metric.CallCount)
		metric.ErrorRate = percentage(metric.ErrorCount, metric.CallCount)
		metric.P95DurationMs = quantileOf(durations[k], 0.95)
		metric.P99DurationMs = quantileOf(durations[k], 0.99)
		out = append(out, metric)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TotalDurationMs > out[j].TotalDurationMs })
	if len(out) > dbMetricsMaxRows {
		out = out[:dbMetricsMaxRows]
	}
	return out
}

// quantileOf returns the nearest-rank quantile of a duration sample.
func quantileOf(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func percentage(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(part) / float64(total)) * 100.0
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// nsAllowedIn reports whether a namespace is visible. A nil list means no
// restriction; an empty non-nil list means nothing is visible.
func nsAllowedIn(allowed []string, ns string) bool {
	if allowed == nil {
		return true
	}
	for _, a := range allowed {
		if a == ns {
			return true
		}
	}
	return false
}
