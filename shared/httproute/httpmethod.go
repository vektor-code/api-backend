package httproute

import (
	"fmt"
	"strings"
)

// HTTPMethodTokenPattern is the regular expression for a single HTTP method
// token (RFC 9110 tchar). IsValidHTTPMethod and ClickHouse match() must stay
// equivalent — see TestHTTPMethodTokenParity.
const HTTPMethodTokenPattern = `^[!#$%&'*+\-.^_` + "`" + `|~0-9A-Za-z]+$`

// httpSignalAttrKeys are attributes that mean the span carries HTTP telemetry.
// Presence of any non-empty value (including whitespace-only) is an HTTP signal.
var httpSignalAttrKeys = []string{
	"http.method", "http.request.method",
	"http.route", "url.path", "http.target",
	"http.url", "url.full",
}

// IsValidHTTPMethod reports whether method is a single HTTP token.
// Empty strings, whitespace, commas, slashes, colons, controls, and other
// non-tchar bytes are rejected. Custom tokens such as PURGE remain valid.
// The value is not trimmed and is not compared to an allowlist.
func IsValidHTTPMethod(method string) bool {
	if method == "" {
		return false
	}
	for i := 0; i < len(method); i++ {
		if !isHTTPTchar(method[i]) {
			return false
		}
	}
	return true
}

func isHTTPTchar(c byte) bool {
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// HTTPMethodFromTags returns the raw HTTP method attribute, preferring the
// stable key. The value is not trimmed or canonicalized.
func HTTPMethodFromTags(tags map[string]string) string {
	if tags == nil {
		return ""
	}
	for _, k := range []string{"http.request.method", "http.method"} {
		if tags[k] != "" {
			return tags[k]
		}
	}
	return ""
}

func hasHTTPAttrPresent(tags map[string]string) bool {
	if tags == nil {
		return false
	}
	for _, k := range httpSignalAttrKeys {
		if tags[k] != "" {
			return true
		}
	}
	return false
}

// HTTPServerIdentityEligible reports whether a span may mint a backend HTTP
// transaction / endpoint identity.
//
// Non-SERVER kinds are always eligible (CLIENT dependencies and CONSUMER
// transactions are out of scope). SERVER spans without HTTP attributes keep
// existing non-HTTP (RPC) behavior. SERVER spans with HTTP attributes require
// a token-valid method.
func HTTPServerIdentityEligible(kind string, tags map[string]string) bool {
	if !strings.EqualFold(strings.TrimSpace(kind), "SERVER") {
		return true
	}
	if !hasHTTPAttrPresent(tags) {
		return true
	}
	return IsValidHTTPMethod(HTTPMethodFromTags(tags))
}

// CHHTTPServerIdentityEligible is the ClickHouse predicate with the same
// semantics as HTTPServerIdentityEligible, using columns kindCol and tagsCol.
func CHHTTPServerIdentityEligible(kindCol, tagsCol string) string {
	noHTTP := make([]string, 0, len(httpSignalAttrKeys))
	for _, k := range httpSignalAttrKeys {
		noHTTP = append(noHTTP, fmt.Sprintf("%s['%s'] = ''", tagsCol, k))
	}
	methodExpr := fmt.Sprintf(
		"if(%s['http.request.method'] != '', %s['http.request.method'], %s['http.method'])",
		tagsCol, tagsCol, tagsCol,
	)
	pattern := strings.ReplaceAll(HTTPMethodTokenPattern, `'`, `\'`)
	return fmt.Sprintf(
		"(upperUTF8(%s) != 'SERVER' OR (%s) OR match(%s, '%s'))",
		kindCol,
		strings.Join(noHTTP, " AND "),
		methodExpr,
		pattern,
	)
}
