package httproute

import (
	"fmt"
	"strings"
)

// TagSpanRole is stamped by spanenrich so stored spans carry the same
// classification the Go helpers use. ClickHouse filters read this tag when
// present and fall back to name/path heuristics for older rows.
const TagSpanRole = "crnet.apm.span.role"

const (
	RoleProbe   = "probe"
	RoleStream  = "stream"
	RoleNetwork = "network"
	RoleJob     = "job"
)

var networkSpanNames = map[string]bool{
	"dns.lookup": true, "tcp.connect": true, "tls.connect": true,
	"connect": true, "http.connect": true, "net.http.connect": true,
	"fs": true, "fs stat": true, "fs.open": true,
}

var frameworkSpanNames = map[string]bool{
	"create nest app": true,
	"transaction.commit": true,
	"transaction.begin": true,
	"transaction.rollback": true,
	"hibernate session": true,
	"express": true,
	"middleware": true,
	"cors": true,
}

// probePathExact is the set of Kubernetes / Spring probe paths that must not
// drive service error rate or Top transactions. Datadog and Elastic exclude
// these from RED by default.
var probePathExact = map[string]bool{
	"/health": true, "/healthz": true, "/health/": true,
	"/ready": true, "/readyz": true, "/readiness": true,
	"/live": true, "/livez": true, "/liveness": true,
	"/ping": true, "/heartbeat": true,
}

// IsNetworkSpan reports whether the span is a socket/DNS/TLS primitive.
// Those spans inherit the peer address of whatever the process talks to, so
// classifying them as PostgreSQL or minting them as transactions poisons
// the database dashboard and Top transactions.
func IsNetworkSpan(spanName string) bool {
	return networkSpanNames[strings.ToLower(strings.TrimSpace(spanName))]
}

// IsFrameworkNoise reports startup, middleware, and ORM session spans that
// OpenTelemetry auto-instrumentation emits as INTERNAL roots.
func IsFrameworkNoise(spanName string) bool {
	n := strings.ToLower(strings.TrimSpace(spanName))
	if frameworkSpanNames[n] {
		return true
	}
	if strings.HasPrefix(n, "transaction.") {
		return true
	}
	return false
}

func isSQLShapedName(spanName string) bool {
	n := strings.ToUpper(strings.TrimSpace(spanName))
	for _, prefix := range []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "MERGE ", "WITH "} {
		if strings.HasPrefix(n, prefix) {
			return true
		}
	}
	return n == "SELECT" || n == "INSERT" || n == "UPDATE" || n == "DELETE"
}

func isDatastoreLayerName(spanName string) bool {
	n := strings.ToLower(spanName)
	for _, tok := range []string{"repository", "dao.", "mapper", "entitymanager", "jdbc"} {
		if strings.Contains(n, tok) {
			return true
		}
	}
	return false
}

func requestPath(spanName string, tags map[string]string) string {
	if tags != nil {
		for _, k := range []string{"http.route", "url.path", "http.target"} {
			if p := strings.TrimSpace(tags[k]); p != "" {
				if idx := strings.IndexAny(p, "?#"); idx >= 0 {
					p = p[:idx]
				}
				return pathOnly(p)
			}
		}
		if p := pathFromURL(strings.TrimSpace(tags["http.url"])); p != "" {
			return pathOnly(p)
		}
		if p := pathFromURL(strings.TrimSpace(tags["url.full"])); p != "" {
			return pathOnly(p)
		}
	}
	if looksLikePath(spanName) {
		idx := strings.Index(spanName, "/")
		return pathOnly(spanName[idx:])
	}
	return ""
}

func pathOnly(p string) string {
	if idx := strings.IndexAny(p, "?#"); idx >= 0 {
		p = p[:idx]
	}
	p = strings.TrimSpace(p)
	if len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

// IsProbe reports Kubernetes/Spring liveness and readiness hits. They are
// real SERVER spans, but counting them as user traffic makes a CrashLoop
// healthz look like a 100% application error rate.
func IsProbe(spanName string, tags map[string]string) bool {
	if tags != nil && tags[TagSpanRole] == RoleProbe {
		return true
	}
	path := strings.ToLower(requestPath(spanName, tags))
	if path == "" {
		return false
	}
	if probePathExact[path] {
		return true
	}
	if strings.HasPrefix(path, "/actuator/health") {
		return true
	}
	// "/api/health/readiness" and similar nested probe routes.
	for _, seg := range strings.Split(path, "/") {
		switch seg {
		case "healthz", "readyz", "livez", "readiness", "liveness", "heartbeat":
			return true
		}
	}
	if strings.HasSuffix(path, "/health") || strings.HasSuffix(path, "/ready") || strings.HasSuffix(path, "/live") {
		return true
	}
	return false
}

// IsStreaming reports long-lived RPC/HTTP streams. Their duration is the
// subscription lifetime, not a request latency, so they must not rank as the
// slowest endpoint.
func IsStreaming(spanName string, tags map[string]string) bool {
	if tags != nil && tags[TagSpanRole] == RoleStream {
		return true
	}
	if tags != nil {
		if strings.EqualFold(tags["rpc.grpc.streaming"], "true") {
			return true
		}
		for _, k := range []string{"rpc.method", "rpc.grpc.method"} {
			if streamingToken(tags[k]) {
				return true
			}
		}
	}
	return streamingToken(spanName)
}

func streamingToken(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Match Stream as a gRPC/method token, not "upstream" or "downstream".
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "stream") {
		return true
	}
	for _, sep := range []string{"/", ".", "_", " "} {
		if strings.Contains(lower, sep+"stream") {
			return true
		}
	}
	return strings.Contains(s, "Stream")
}

// InternalEntrypointEligible reports whether an INTERNAL span may name a
// transaction. Cron/jobs stay; DNS, TLS, ORM session, and repository methods
// do not.
func InternalEntrypointEligible(spanName string, tags map[string]string) bool {
	if isDatastoreSpan(tags) {
		return false
	}
	name := strings.TrimSpace(spanName)
	if name == "" || isBareMethod(name) {
		return false
	}
	if IsNetworkSpan(name) || IsFrameworkNoise(name) || isSQLShapedName(name) || isDatastoreLayerName(name) {
		return false
	}
	if IsProbe(name, tags) {
		return false
	}
	return true
}

// SpanRole is the derived role stored on crnet.apm.span.role.
func SpanRole(spanName, spanKind string, tags map[string]string) string {
	if IsNetworkSpan(spanName) {
		return RoleNetwork
	}
	if IsProbe(spanName, tags) {
		return RoleProbe
	}
	if IsStreaming(spanName, tags) {
		return RoleStream
	}
	if strings.EqualFold(strings.TrimSpace(spanKind), "INTERNAL") && InternalEntrypointEligible(spanName, tags) {
		return RoleJob
	}
	return ""
}

// HasHTTPRequestSignal reports whether the span carries HTTP telemetry.
func HasHTTPRequestSignal(tags map[string]string) bool {
	return hasHTTPAttrPresent(tags)
}

func chPathExpr(tagsCol, opCol string) string {
	return fmt.Sprintf(
		"if(%s['http.route'] != '', %s['http.route'], if(%s['url.path'] != '', %s['url.path'], if(%s['http.target'] != '', %s['http.target'], %s)))",
		tagsCol, tagsCol, tagsCol, tagsCol, tagsCol, tagsCol, opCol,
	)
}

// CHProbeSpan is a ClickHouse predicate that is true for probe SERVER spans.
func CHProbeSpan(tagsCol, opCol string) string {
	path := chPathExpr(tagsCol, opCol)
	return fmt.Sprintf(
		"(%s['%s'] = '%s' OR match(%s, '(?i)^/(healthz?|readyz?|livez?|ping|heartbeat|readiness|liveness)(/.*)?$') OR match(%s, '(?i)/actuator/health') OR positionCaseInsensitive(%s, '/healthz') > 0)",
		tagsCol, TagSpanRole, RoleProbe, path, path, path,
	)
}

// CHStreamSpan is a ClickHouse predicate that is true for streaming RPCs.
func CHStreamSpan(tagsCol, opCol string) string {
	return fmt.Sprintf(
		"(%s['%s'] = '%s' OR match(if(%s['rpc.method'] != '', %s['rpc.method'], %s), '(?i)(^|[/._ ])stream') OR position(%s, 'Stream') > 0)",
		tagsCol, TagSpanRole, RoleStream, tagsCol, tagsCol, opCol, opCol,
	)
}

func chInternalNoiseSQL(opCol string) string {
	return fmt.Sprintf(
		"lowerUTF8(%s) NOT IN ('dns.lookup', 'tcp.connect', 'tls.connect', 'connect', 'http.connect', 'net.http.connect', 'create nest app', 'transaction.commit', 'transaction.begin', 'transaction.rollback') AND positionCaseInsensitive(%s, 'Repository') = 0 AND positionCaseInsensitive(%s, 'EntityManager') = 0 AND match(%s, '^(?i)(select|insert|update|delete|merge|with)(\\\\s|$)') = 0",
		opCol, opCol, opCol, opCol,
	)
}
