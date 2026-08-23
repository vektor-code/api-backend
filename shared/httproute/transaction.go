package httproute

import (
	"fmt"
	"strings"
)

func normalizedSpanKind(kind string) string {
	return strings.ToUpper(strings.TrimSpace(kind))
}

// TransactionIdentityEligible reports whether a span may mint a transaction /
// endpoint identity. This matches Datadog, Elastic APM and New Relic:
//
//   - SERVER is an entrypoint (HTTP still requires a token-valid method)
//   - CONSUMER is an entrypoint (messaging receive)
//   - INTERNAL is an entrypoint only when it is not datastore work (cron/custom)
//   - CLIENT and PRODUCER are always dependencies, even when they are trace roots
func TransactionIdentityEligible(kind string, tags map[string]string) bool {
	switch normalizedSpanKind(kind) {
	case "CLIENT", "PRODUCER":
		return false
	case "CONSUMER":
		return true
	case "INTERNAL":
		return !isDatastoreSpan(tags)
	case "SERVER":
		return HTTPServerIdentityEligible("SERVER", tags)
	default:
		return false
	}
}

// RequestIdentityEligible reports whether a span should count as incoming
// throughput for a service. INTERNAL work can name a transaction when it is
// the root, but it is not HTTP/messaging intake.
func RequestIdentityEligible(kind string, tags map[string]string) bool {
	switch normalizedSpanKind(kind) {
	case "SERVER":
		return HTTPServerIdentityEligible("SERVER", tags)
	case "CONSUMER":
		return true
	default:
		return false
	}
}

func isDatastoreSpan(tags map[string]string) bool {
	if tags == nil {
		return false
	}
	if strings.TrimSpace(tags["db.system"]) != "" || strings.TrimSpace(tags["db.system.name"]) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(tags["crnet.apm.dependency.kind"])) {
	case "database", "cache":
		return true
	}
	thread := strings.ToLower(tags["thread.name"])
	return strings.Contains(thread, "hikari")
}

// CHTransactionIdentityEligible is the ClickHouse predicate with the same
// semantics as TransactionIdentityEligible.
func CHTransactionIdentityEligible(kindCol, tagsCol, depKindCol string) string {
	httpOK := CHHTTPServerIdentityEligible(kindCol, tagsCol)
	return fmt.Sprintf(
		"((upperUTF8(%s) = 'SERVER' AND %s) OR upperUTF8(%s) = 'CONSUMER' OR (upperUTF8(%s) = 'INTERNAL' AND %s NOT IN ('database', 'cache') AND %s['db.system'] = '' AND %s['db.system.name'] = '' AND positionCaseInsensitive(%s['thread.name'], 'hikari') = 0))",
		kindCol, httpOK, kindCol, kindCol, depKindCol, tagsCol, tagsCol, tagsCol,
	)
}

// CHRequestIdentityEligible is the ClickHouse predicate with the same
// semantics as RequestIdentityEligible.
func CHRequestIdentityEligible(kindCol, tagsCol string) string {
	httpOK := CHHTTPServerIdentityEligible(kindCol, tagsCol)
	return fmt.Sprintf(
		"((upperUTF8(%s) = 'SERVER' AND %s) OR upperUTF8(%s) = 'CONSUMER')",
		kindCol, httpOK, kindCol,
	)
}
