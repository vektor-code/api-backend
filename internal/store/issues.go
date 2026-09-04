package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

// ListErrorGroups returns recurring failure shapes from the ingest-time
// fingerprint table. Falls back to a bounded raw-span aggregation when the
// derived table is empty (first minutes after deploy).
func (s *Store) ListErrorGroups(namespace string, allowedNs []string, window time.Duration, limit int) ([]models.ErrorGroup, error) {
	if !s.chMode {
		return nil, nil
	}
	if window <= 0 {
		window = time.Hour
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	groups, err := s.chErrorGroupsFromTable(ctx, namespace, allowedNs, window, limit)
	if err == nil && len(groups) > 0 {
		return groups, nil
	}
	return s.chErrorGroupsFromSpans(ctx, namespace, allowedNs, window, limit)
}

func errorGroupNSFilter(namespace string, allowedNs []string) string {
	if namespace != "" {
		return fmt.Sprintf("namespace = '%s'", chEscape(namespace))
	}
	if allowedNs == nil {
		return ""
	}
	quoted := make([]string, 0, len(allowedNs))
	for _, ns := range allowedNs {
		quoted = append(quoted, "'"+chEscape(ns)+"'")
	}
	if len(quoted) == 0 {
		return "1 = 0"
	}
	return "namespace IN (" + strings.Join(quoted, ",") + ")"
}

func (s *Store) chErrorGroupsFromTable(ctx context.Context, namespace string, allowedNs []string, window time.Duration, limit int) ([]models.ErrorGroup, error) {
	where := []string{fmt.Sprintf("last_seen > now64(6) - INTERVAL %d SECOND", int(window.Seconds()))}
	if ns := errorGroupNSFilter(namespace, allowedNs); ns != "" {
		where = append(where, ns)
	}
	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		fingerprint,
		namespace,
		service_name,
		anyLast(transaction_name) AS transaction_name,
		anyLast(exception_type) AS exception_type,
		anyLast(exception_message) AS exception_message,
		anyLast(db_query_fingerprint) AS db_query_fingerprint,
		anyLast(example_trace_id) AS example_trace_id,
		toInt64(round(sum(count))) AS count,
		max(last_seen) AS last_seen
	FROM kubetrace.error_groups
	WHERE %s
	GROUP BY fingerprint, namespace, service_name
	ORDER BY count DESC
	LIMIT %d`, strings.Join(where, " AND "), limit))
	if err != nil {
		return nil, err
	}
	return rowsToErrorGroups(rows), nil
}

func (s *Store) chErrorGroupsFromSpans(ctx context.Context, namespace string, allowedNs []string, window time.Duration, limit int) ([]models.ErrorGroup, error) {
	where := []string{
		fmt.Sprintf("timestamp > now64(6) - INTERVAL %d SECOND", int(window.Seconds())),
		"error_fingerprint != ''",
	}
	if ns := errorGroupNSFilter(namespace, allowedNs); ns != "" {
		where = append(where, ns)
	}
	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		error_fingerprint AS fingerprint,
		namespace,
		service_name,
		anyLast(if(transaction_name != '', transaction_name, operation_name)) AS transaction_name,
		anyLast(exception_type) AS exception_type,
		anyLast(substringUTF8(if(exception_message != '', exception_message, status_message), 1, 400)) AS exception_message,
		anyLast(db_query_fingerprint) AS db_query_fingerprint,
		anyLast(trace_id) AS example_trace_id,
		toInt64(round(sum(greatest(sample_weight, 1)))) AS count,
		max(timestamp) AS last_seen
	FROM kubetrace.spans
	WHERE %s
	GROUP BY fingerprint, namespace, service_name
	ORDER BY count DESC
	LIMIT %d`, strings.Join(where, " AND "), limit))
	if err != nil {
		return nil, err
	}
	return rowsToErrorGroups(rows), nil
}

func rowsToErrorGroups(rows []map[string]any) []models.ErrorGroup {
	out := make([]models.ErrorGroup, 0, len(rows))
	for _, row := range rows {
		out = append(out, models.ErrorGroup{
			Fingerprint:      chString(row["fingerprint"]),
			Namespace:        chString(row["namespace"]),
			ServiceName:      chString(row["service_name"]),
			TransactionName:  chString(row["transaction_name"]),
			ExceptionType:    chString(row["exception_type"]),
			ExceptionMessage: chString(row["exception_message"]),
			DBFingerprint:    chString(row["db_query_fingerprint"]),
			ExampleTraceID:   chString(row["example_trace_id"]),
			Count:            chInt(row["count"]),
			LastSeen:         chTime(row["last_seen"]),
		})
	}
	return out
}
