package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

// TimeseriesBucket is one interval of aggregated telemetry for dashboards.
type TimeseriesBucket struct {
	Time    time.Time `json:"time"`
	Label   string    `json:"label"`
	Spans   int64     `json:"spans"`
	Errors  int64     `json:"errors"`
	AvgMs   float64   `json:"avgMs"`
	P99Ms   float64   `json:"p99Ms"`
	DbCalls int64     `json:"dbCalls"`
	DbAvgMs float64   `json:"dbAvgMs"`
}

// ServiceErrorSeries carries per-bucket error counts for one service (heatmap).
type ServiceErrorSeries struct {
	Service   string  `json:"service"`
	Namespace string  `json:"namespace"`
	Errors    []int64 `json:"errors"`
	Spans     []int64 `json:"spans"`
}

// TimeseriesData is the dashboard chart payload.
type TimeseriesData struct {
	Buckets       []TimeseriesBucket   `json:"buckets"`
	ServiceErrors []ServiceErrorSeries `json:"serviceErrors"`
	WindowMinutes int                  `json:"windowMinutes"`
}

const (
	tsBucketCount   = 12
	tsMaxHeatmapSvc = 8
)

// GetTimeseries returns real per-interval aggregates for the dashboard.
// namespace filters to one namespace; allowedNs (non-nil) restricts to a
// user's visible namespaces.
func (s *Store) GetTimeseries(namespace string, allowedNs []string, windowMinutes int) (*TimeseriesData, error) {
	if windowMinutes <= 0 {
		windowMinutes = 60
	}
	step := (windowMinutes * 60) / tsBucketCount
	from := time.Now().UTC().Add(-time.Duration(windowMinutes) * time.Minute).Truncate(time.Duration(step) * time.Second)

	// Pre-build the bucket skeleton so charts always render a full axis.
	result := &TimeseriesData{WindowMinutes: windowMinutes}
	bucketIdx := make(map[int64]int, tsBucketCount)
	for i := 0; i < tsBucketCount; i++ {
		t := from.Add(time.Duration(i*step) * time.Second)
		bucketIdx[t.Unix()] = i
		result.Buckets = append(result.Buckets, TimeseriesBucket{Time: t, Label: t.Local().Format("15:04")})
	}

	if s.chMode {
		return s.chTimeseries(result, bucketIdx, namespace, allowedNs, from, step)
	}
	return s.memTimeseries(result, bucketIdx, namespace, allowedNs, from, step)
}

func (s *Store) chTimeseries(result *TimeseriesData, bucketIdx map[int64]int, namespace string, allowedNs []string, from time.Time, step int) (*TimeseriesData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	where := []string{fmt.Sprintf("timestamp >= toDateTime64('%s', 6)", from.Format(chTimeLayout))}
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

	// 1. Global buckets
	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL %d SECOND)) AS b,
		count() AS spans,
		countIf(status_code = 'ERROR') AS errors,
		avg(duration_ns) / 1e6 AS avg_ms,
		quantile(0.99)(duration_ns) / 1e6 AS p99_ms,
		countIf(tags['db.system'] != '') AS db_calls,
		coalesce(avgIf(duration_ns, tags['db.system'] != '') / 1e6, 0) AS db_avg_ms
	FROM kubetrace.spans WHERE %s GROUP BY b ORDER BY b`, step, cond))
	if err != nil {
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

	// 2. Per-service buckets for the error heatmap (top services by volume)
	svcRows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		service_name, any(namespace) AS ns,
		toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL %d SECOND)) AS b,
		count() AS spans,
		countIf(status_code = 'ERROR') AS errors
	FROM kubetrace.spans WHERE %s GROUP BY service_name, b`, step, cond))
	if err != nil {
		return result, nil // heatmap is optional; charts already have data
	}

	type svcAgg struct {
		ns          string
		spans       []int64
		errors      []int64
		total       int64
		totalErrors int64
	}
	svcMap := make(map[string]*svcAgg)
	for _, row := range svcRows {
		name := chString(row["service_name"])
		agg, ok := svcMap[name]
		if !ok {
			agg = &svcAgg{ns: chString(row["ns"]), spans: make([]int64, tsBucketCount), errors: make([]int64, tsBucketCount)}
			svcMap[name] = agg
		}
		if idx, ok := bucketIdx[chInt(row["b"])]; ok {
			agg.spans[idx] += chInt(row["spans"])
			agg.errors[idx] += chInt(row["errors"])
			agg.total += chInt(row["spans"])
			agg.totalErrors += chInt(row["errors"])
		}
	}

	type ranked struct {
		name string
		agg  *svcAgg
	}
	var all []ranked
	for name, agg := range svcMap {
		all = append(all, ranked{name, agg})
	}
	// Services with errors first, then by traffic volume.
	sort.Slice(all, func(i, j int) bool {
		if (all[i].agg.totalErrors > 0) != (all[j].agg.totalErrors > 0) {
			return all[i].agg.totalErrors > 0
		}
		if all[i].agg.totalErrors != all[j].agg.totalErrors {
			return all[i].agg.totalErrors > all[j].agg.totalErrors
		}
		return all[i].agg.total > all[j].agg.total
	})
	if len(all) > tsMaxHeatmapSvc {
		all = all[:tsMaxHeatmapSvc]
	}
	for _, r := range all {
		result.ServiceErrors = append(result.ServiceErrors, ServiceErrorSeries{
			Service:   r.name,
			Namespace: r.agg.ns,
			Errors:    r.agg.errors,
			Spans:     r.agg.spans,
		})
	}
	return result, nil
}

// memTimeseries computes the same aggregates from the in-memory recent traces
// (legacy/demo mode without ClickHouse).
func (s *Store) memTimeseries(result *TimeseriesData, bucketIdx map[int64]int, namespace string, allowedNs []string, from time.Time, step int) (*TimeseriesData, error) {
	allowed := map[string]bool{}
	for _, ns := range allowedNs {
		allowed[ns] = true
	}

	type svcAgg struct {
		ns          string
		spans       []int64
		errors      []int64
		total       int64
		totalErrors int64
	}
	svcMap := make(map[string]*svcAgg)

	durSums := make([]float64, tsBucketCount)
	dbDurSums := make([]float64, tsBucketCount)
	var durations [][]float64 = make([][]float64, tsBucketCount)

	s.tracesMu.RLock()
	for _, trace := range s.recentTraces {
		for _, sp := range trace.Spans {
			if namespace != "" && sp.Namespace != namespace {
				continue
			}
			if namespace == "" && allowedNs != nil && !allowed[sp.Namespace] {
				continue
			}
			bucket := sp.StartTime.UTC().Truncate(time.Duration(step) * time.Second).Unix()
			idx, ok := bucketIdx[bucket]
			if !ok {
				continue
			}
			bkt := &result.Buckets[idx]
			bkt.Spans++
			isErr := sp.Status == models.SpanStatusError
			if isErr {
				bkt.Errors++
			}
			durSums[idx] += sp.DurationMs
			durations[idx] = append(durations[idx], sp.DurationMs)
			if sp.Attributes["db.system"] != "" {
				bkt.DbCalls++
				dbDurSums[idx] += sp.DurationMs
			}

			agg, ok := svcMap[sp.ServiceName]
			if !ok {
				agg = &svcAgg{ns: sp.Namespace, spans: make([]int64, tsBucketCount), errors: make([]int64, tsBucketCount)}
				svcMap[sp.ServiceName] = agg
			}
			agg.spans[idx]++
			agg.total++
			if isErr {
				agg.errors[idx]++
				agg.totalErrors++
			}
		}
	}
	s.tracesMu.RUnlock()

	for i := range result.Buckets {
		bkt := &result.Buckets[i]
		if bkt.Spans > 0 {
			bkt.AvgMs = durSums[i] / float64(bkt.Spans)
		}
		if bkt.DbCalls > 0 {
			bkt.DbAvgMs = dbDurSums[i] / float64(bkt.DbCalls)
		}
		if n := len(durations[i]); n > 0 {
			sort.Float64s(durations[i])
			bkt.P99Ms = durations[i][(n*99)/100%n]
		}
	}

	type ranked struct {
		name string
		agg  *svcAgg
	}
	var all []ranked
	for name, agg := range svcMap {
		all = append(all, ranked{name, agg})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].agg.totalErrors != all[j].agg.totalErrors {
			return all[i].agg.totalErrors > all[j].agg.totalErrors
		}
		return all[i].agg.total > all[j].agg.total
	})
	if len(all) > tsMaxHeatmapSvc {
		all = all[:tsMaxHeatmapSvc]
	}
	for _, r := range all {
		result.ServiceErrors = append(result.ServiceErrors, ServiceErrorSeries{
			Service:   r.name,
			Namespace: r.agg.ns,
			Errors:    r.agg.errors,
			Spans:     r.agg.spans,
		})
	}
	return result, nil
}
