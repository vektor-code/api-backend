// Package httproute derives a stable, low-cardinality transaction name for a
// span.
//
// "Top transactions" groups by a name, so that name has to sit in a narrow
// band: specific enough that two different endpoints do not merge, general
// enough that two calls to the same endpoint do not split.
//
// Real traffic fails at both ends. Some instrumentation names a server span
// after the templated route ("GET /users/{id}") which is exactly right; some
// names it after the raw method alone ("GET"), collapsing every endpoint in the
// service into one row; and the raw path that would fix that carries record ids
// ("/users/11", "/users/65"), which splits one endpoint into thousands.
//
// TransactionName resolves all three: it keeps a name that already carries a
// route, promotes http.route when the span name does not, and falls back to the
// request path with its variable segments replaced.
package httproute

import (
	"strings"
)

// Placeholders substituted for variable path segments.
const (
	PlaceholderID   = "{id}"
	PlaceholderUUID = "{uuid}"
	PlaceholderHash = "{hash}"
)

// maxPathSegments bounds how much of a path is kept. Deeply nested paths add
// cardinality without adding meaning to an endpoint list.
const maxPathSegments = 8

// knownMethods are the HTTP methods a bare span name might consist of.
var knownMethods = map[string]bool{
	"GET": true, "HEAD": true, "POST": true, "PUT": true, "DELETE": true,
	"CONNECT": true, "OPTIONS": true, "TRACE": true, "PATCH": true, "QUERY": true,
}

// TransactionName returns the name to aggregate a span under.
//
// tags must already have been through semconv.Normalize, so http.route,
// url.path and http.method hold resolved values. An empty return means the
// caller should keep the original span name.
func TransactionName(tags map[string]string, spanName string) string {
	if tags == nil {
		return spanName
	}

	method := strings.ToUpper(strings.TrimSpace(tags["http.method"]))
	route := strings.TrimSpace(tags["http.route"])

	// A span name that is more than a bare method already carries routing
	// information; the instrumentation knows better than we do.
	if !isBareMethod(spanName) && spanName != "" {
		// Unless it embeds raw ids, in which case normalize just those.
		if looksLikePath(spanName) {
			return normalizeNameWithPath(spanName)
		}
		return spanName
	}

	// The span name is a bare method, or empty. Recover the endpoint.
	if route != "" {
		return joinMethodPath(method, route)
	}

	path := strings.TrimSpace(tags["url.path"])
	if path == "" {
		path = strings.TrimSpace(tags["http.target"])
	}
	if path == "" {
		path = pathFromURL(strings.TrimSpace(tags["http.url"]))
	}
	if path != "" {
		return joinMethodPath(method, NormalizePath(path))
	}

	return spanName
}

// NormalizePath replaces variable segments of a request path with placeholders,
// so that /users/11 and /users/65 become the same endpoint.
//
// Segments that are already templated by the framework ({id}, :id, {*path}) are
// left alone.
func NormalizePath(path string) string {
	if path == "" {
		return ""
	}
	// Drop query and fragment; they are parameters, not the endpoint.
	if idx := strings.IndexAny(path, "?#"); idx != -1 {
		path = path[:idx]
	}
	if path == "" || path == "/" {
		return "/"
	}

	segments := strings.Split(path, "/")
	out := make([]string, 0, len(segments))
	kept := 0
	for _, segment := range segments {
		if segment == "" {
			out = append(out, segment)
			continue
		}
		if kept >= maxPathSegments {
			out = append(out, "...")
			break
		}
		kept++
		out = append(out, normalizeSegment(segment))
	}

	result := strings.Join(out, "/")
	// Preserve a leading slash lost to an empty first segment.
	if strings.HasPrefix(path, "/") && !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	// A trailing slash is not a distinct endpoint.
	if len(result) > 1 {
		result = strings.TrimSuffix(result, "/")
	}
	return result
}

// normalizeSegment classifies one path segment.
func normalizeSegment(segment string) string {
	if isTemplated(segment) {
		return segment
	}
	if isAllDigits(segment) {
		return PlaceholderID
	}
	if isUUID(segment) {
		return PlaceholderUUID
	}
	if isHexBlob(segment) {
		return PlaceholderHash
	}
	if isOpaqueIdentifier(segment) {
		return PlaceholderID
	}
	return segment
}

// isTemplated reports whether the framework already parameterized the segment,
// in any of the common spellings: {id}, {*path}, :id, <id>.
func isTemplated(segment string) bool {
	switch segment[0] {
	case '{', ':', '<', '*':
		return true
	}
	return false
}

// isOpaqueIdentifier catches record keys that are neither purely numeric nor
// hexadecimal — ULIDs, base64 tokens, slugs ending in an id.
//
// The length floor matters: without it "v2" and "me" would be swallowed, and
// the endpoint list would lose the distinctions that make it useful.
func isOpaqueIdentifier(segment string) bool {
	if len(segment) < 12 {
		return false
	}
	var digits, letters, other int
	for i := 0; i < len(segment); i++ {
		c := segment[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			letters++
		default:
			other++
		}
	}
	// Words separated by hyphens or underscores are readable route names
	// (PROCUREMENT_METHOD, functional-sections), not identifiers.
	if other > 0 && digits == 0 {
		return false
	}
	return digits > 0 && letters > 0
}

func isUUID(segment string) bool {
	if len(segment) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := segment[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !isHexDigit(c) {
			return false
		}
	}
	return true
}

func isHexBlob(segment string) bool {
	if len(segment) < 16 {
		return false
	}
	for i := 0; i < len(segment); i++ {
		if !isHexDigit(segment[i]) {
			return false
		}
	}
	return true
}

// looksLikePath reports whether a span name contains a path component that may
// need normalizing, e.g. "GET /api/users/11".
func looksLikePath(spanName string) bool {
	return strings.Contains(spanName, "/")
}

// normalizeNameWithPath normalizes the path portion of an existing span name
// while leaving any leading method or label intact.
func normalizeNameWithPath(spanName string) string {
	idx := strings.Index(spanName, "/")
	if idx < 0 {
		return spanName
	}
	prefix := spanName[:idx]
	path := spanName[idx:]
	// Only touch names shaped like "<label> <path>"; a bare path has no prefix.
	if prefix != "" && !strings.HasSuffix(prefix, " ") {
		return spanName
	}
	return prefix + NormalizePath(path)
}

// isBareMethod reports whether a span name is nothing but an HTTP method.
func isBareMethod(spanName string) bool {
	return knownMethods[strings.ToUpper(strings.TrimSpace(spanName))]
}

func joinMethodPath(method, path string) string {
	if method == "" {
		return path
	}
	if path == "" {
		return method
	}
	return method + " " + path
}

// pathFromURL extracts the path component of an absolute URL.
func pathFromURL(rawURL string) string {
	idx := strings.Index(rawURL, "://")
	if idx == -1 {
		return ""
	}
	rest := rawURL[idx+3:]
	slash := strings.Index(rest, "/")
	if slash == -1 {
		return "/"
	}
	return rest[slash:]
}

func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
