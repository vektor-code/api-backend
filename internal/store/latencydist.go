package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/spantree"
)

// LatencyBucket is one column of the request-latency histogram.
type LatencyBucket struct {
	Label string `json:"label"`
	// UpperMs is the exclusive upper bound of the bucket in milliseconds; 0 for
	// the final open-ended bucket.
	UpperMs float64 `json:"upperMs"`
	Count   int64   `json:"count"`
}

// LatencyDistribution is the request-duration histogram for the dashboard,
// computed over root spans (one observation per request), with the percentile
// markers overlaid on it.
type LatencyDistribution struct {
	Buckets       []LatencyBucket `json:"buckets"`
	P50Ms         float64         `json:"p50Ms"`
	P95Ms         float64         `json:"p95Ms"`
	P99Ms         float64         `json:"p99Ms"`
	Total         int64           `json:"total"`
	WindowMinutes int             `json:"windowMinutes"`
}

// latencyEdges are the exclusive upper bounds (ms) of each histogram bucket; the
// final bucket is open-ended (everything at or above the last edge).
var latencyEdges = []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

var latencyLabels = []string{
	"<10ms", "10–25ms", "25–50ms", "50–100ms", "100–250ms",
	"250–500ms", "500ms–1s", "1–2.5s", "2.5–5s", "5s+",
}

// GetLatencyDistribution returns the request-latency histogram over the window.
func (s *Store) GetLatencyDistribution(namespace string, allowedNs []string, windowMinutes int) (*LatencyDistribution, error) {
	if windowMinutes <= 0 {
		windowMinutes = 60
	}
	from := time.Now().UTC().Add(-time.Duration(windowMinutes) * time.Minute)

	out := &LatencyDistribution{WindowMinutes: windowMinutes}
	out.Buckets = make([]LatencyBucket, len(latencyLabels))
	for i, label := range latencyLabels {
		out.Buckets[i].Label = label
		if i < len(latencyEdges) {
			out.Buckets[i].UpperMs = latencyEdges[i]
		}
	}

	if s.chMode {
		return s.chLatencyDistribution(out, namespace, allowedNs, from)
	}
	return s.memLatencyDistribution(out, namespace, allowedNs, from)
}

func (s *Store) chLatencyDistribution(out *LatencyDistribution, namespace string, allowedNs []string, from time.Time) (*LatencyDistribution, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Root spans only: one observation per request, so the histogram describes
	// end-to-end request latency rather than every internal span.
	where := []string{
		"(parent_span_id = '' OR match(parent_span_id, '^0+$'))",
		fmt.Sprintf("timestamp >= toDateTime64('%s', 6)", from.Format(chTimeLayout)),
	}
	if namespace != "" {
		where = append(where, fmt.Sprintf("namespace = '%s'", chEscape(namespace)))
	} else if allowedNs != nil {
		quoted := make([]string, 0, len(allowedNs))
		for _, ns := range allowedNs {
			quoted = append(quoted, "'"+chEscape(ns)+"'")
		}
		if len(quoted) == 0 {
			return out, nil
		}
		where = append(where, "namespace IN ("+strings.Join(quoted, ",")+")")
	}
	cond := strings.Join(where, " AND ")

	// One pass builds every bucket (weighted by the sampling factor so counts
	// describe traffic) plus the percentile markers.
	var sel []string
	for i := range latencyLabels {
		var pred string
		switch {
		case i == 0:
			pred = fmt.Sprintf("d < %g", latencyEdges[0])
		case i == len(latencyLabels)-1:
			pred = fmt.Sprintf("d >= %g", latencyEdges[len(latencyEdges)-1])
		default:
			pred = fmt.Sprintf("d >= %g AND d < %g", latencyEdges[i-1], latencyEdges[i])
		}
		sel = append(sel, fmt.Sprintf("toInt64(round(sumIf(w, %s))) AS b%d", pred, i))
	}
	query := fmt.Sprintf(`SELECT
		%s,
		quantile(0.5)(d) AS p50, quantile(0.95)(d) AS p95, quantile(0.99)(d) AS p99,
		toInt64(round(sum(w))) AS total
	FROM (
		SELECT duration_ns / 1e6 AS d, greatest(sample_weight, 1) AS w
		FROM kubetrace.spans
		WHERE %s
	)`, strings.Join(sel, ",\n\t\t"), cond)

	rows, err := s.chQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return out, nil
	}
	row := rows[0]
	for i := range out.Buckets {
		out.Buckets[i].Count = chInt(row[fmt.Sprintf("b%d", i)])
	}
	out.P50Ms = chFloat(row["p50"])
	out.P95Ms = chFloat(row["p95"])
	out.P99Ms = chFloat(row["p99"])
	out.Total = chInt(row["total"])
	return out, nil
}

func (s *Store) memLatencyDistribution(out *LatencyDistribution, namespace string, allowedNs []string, from time.Time) (*LatencyDistribution, error) {
	allowed := map[string]bool{}
	for _, ns := range allowedNs {
		allowed[ns] = true
	}

	var samples []float64
	s.tracesMu.RLock()
	for _, trace := range s.recentTraces {
		for _, sp := range trace.Spans {
			if !isRootParent(sp.ParentSpanID) {
				continue
			}
			if sp.StartTime.UTC().Before(from) {
				continue
			}
			if namespace != "" && sp.Namespace != namespace {
				continue
			}
			if namespace == "" && allowedNs != nil && !allowed[sp.Namespace] {
				continue
			}
			d := sp.DurationMs
			samples = append(samples, d)
			out.Buckets[latencyBucketIndex(d)].Count++
			out.Total++
		}
	}
	s.tracesMu.RUnlock()

	if len(samples) > 0 {
		sort.Float64s(samples)
		out.P50Ms = percentileOf(samples, 0.50)
		out.P95Ms = percentileOf(samples, 0.95)
		out.P99Ms = percentileOf(samples, 0.99)
	}
	return out, nil
}

func latencyBucketIndex(ms float64) int {
	for i, edge := range latencyEdges {
		if ms < edge {
			return i
		}
	}
	return len(latencyLabels) - 1
}

func isRootParent(parent string) bool {
	return spantree.IsRoot(parent)
}

func percentileOf(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
