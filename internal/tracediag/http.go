package tracediag

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/spantree"
	"github.com/kubetrace/shared/httproute"
)

func attr(sp *models.Span, keys ...string) string {
	if sp == nil || sp.Attributes == nil {
		return ""
	}
	for _, k := range keys {
		if v := strings.TrimSpace(sp.Attributes[k]); v != "" {
			return v
		}
	}
	return ""
}

func attrPresent(sp *models.Span, keys ...string) bool {
	if sp == nil || sp.Attributes == nil {
		return false
	}
	for _, k := range keys {
		if _, ok := sp.Attributes[k]; ok {
			return true
		}
	}
	return false
}

func httpMethodRaw(sp *models.Span) (string, bool) {
	if sp == nil || sp.Attributes == nil {
		return "", false
	}
	for _, k := range []string{"http.request.method", "http.method"} {
		if v, ok := sp.Attributes[k]; ok {
			return v, true
		}
	}
	return "", false
}

func httpStatus(sp *models.Span) (int, bool) {
	raw := attr(sp, "http.response.status_code", "http.status_code", "http.status")
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, true
	}
	return n, true
}

func httpPath(sp *models.Span) string {
	if p := attr(sp, "url.path", "http.target", "http.route"); p != "" {
		if i := strings.IndexAny(p, "?#"); i >= 0 {
			p = p[:i]
		}
		return p
	}
	full := attr(sp, "url.full", "http.url")
	if full == "" {
		return ""
	}
	if u, err := url.Parse(full); err == nil && u.Path != "" {
		return u.Path
	}
	if i := strings.Index(full, "://"); i >= 0 {
		rest := full[i+3:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			path := rest[slash:]
			if q := strings.IndexAny(path, "?#"); q >= 0 {
				path = path[:q]
			}
			return path
		}
	}
	return ""
}

func httpURL(sp *models.Span) string {
	return attr(sp, "url.full", "http.url")
}

func libraryName(sp *models.Span) string {
	return attr(sp, "otel.library.name", "otel.scope.name", "library.name")
}

func looksLikeHTTPName(name string) bool {
	n := strings.ToUpper(strings.TrimSpace(name))
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"} {
		if n == m || strings.HasPrefix(n, m+" ") || strings.HasPrefix(n, m+"/") {
			return true
		}
	}
	return strings.Contains(name, "/http") || strings.Contains(name, "HTTP ")
}

func isHTTPLibrary(sp *models.Span) bool {
	lib := strings.ToLower(libraryName(sp))
	if lib == "" {
		return false
	}
	return containsAny(lib,
		"/http", "instrumentation-http", "instrumentation.http", "net/http",
		"tomcat", "servlet", "jetty", "undertow",
		"spring-web", "spring-webmvc", "spring-webflux",
		"okhttp", "apache-httpclient", "apache-httpasyncclient", "java-http-client",
		"reactor-netty",
		"aspnetcore", "aspnetcore.mvc",
		"flask", "django", "fastapi", "starlette", "aiohttp", "httpx", "urllib3", "wsgi", "asgi",
		"express", "fastify", "koa", "hapi", "undici", "nextjs", "nestjs",
		"rack", "sinatra", "faraday", "net::http", "action_pack",
		"laravel", "symfony", "guzzle", "php.auto",
	) || strings.HasSuffix(lib, ".http") || strings.Contains(lib, "requests")
}

func isHTTPSpan(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	if attrPresent(sp,
		"http.request.method", "http.method",
		"http.response.status_code", "http.status_code", "http.status",
		"url.path", "http.target", "http.route", "http.url", "url.full",
		"http.host", "http.scheme", "url.scheme",
	) {
		return true
	}
	if isHTTPLibrary(sp) {
		return true
	}
	if (sp.Kind == models.SpanKindServer || sp.Kind == models.SpanKindClient) && looksLikeHTTPName(sp.Name) {
		return true
	}
	return false
}

func validHTTPMethod(sp *models.Span) bool {
	raw, present := httpMethodRaw(sp)
	if !present || raw == "" {
		return false
	}
	return httproute.IsValidHTTPMethod(raw)
}

func validHTTPPath(sp *models.Span) bool {
	p := httpPath(sp)
	if p != "" {
		return true
	}
	return httpURL(sp) != ""
}

func validHTTPStatus(code int) bool {
	return code >= 100 && code <= 599
}

func errorText(sp *models.Span) string {
	if sp == nil {
		return ""
	}
	parts := []string{sp.Error}
	for _, k := range []string{"exception.type", "error.type", "exception.message", "error.message", "error.msg", "status.message", "message"} {
		if v := attr(sp, k); v != "" {
			parts = append(parts, v)
		}
	}
	if sp.Events != nil {
		for _, ev := range sp.Events {
			if ev.Attributes == nil {
				continue
			}
			parts = append(parts, ev.Attributes["exception.type"], ev.Attributes["exception.message"], ev.Attributes["message"])
		}
	}
	return strings.ToLower(strings.Join(parts, " "))
}

// exceptionMessage returns the first useful exception.message from attributes or events.
func exceptionMessage(sp *models.Span) string {
	if sp == nil {
		return ""
	}
	if v := strings.TrimSpace(attr(sp, "exception.message")); v != "" {
		return v
	}
	for _, ev := range sp.Events {
		name := strings.ToLower(strings.TrimSpace(ev.Name))
		if name != "exception" && name != "error" {
			continue
		}
		if ev.Attributes == nil {
			continue
		}
		if v := strings.TrimSpace(ev.Attributes["exception.message"]); v != "" {
			return v
		}
		if v := strings.TrimSpace(ev.Attributes["message"]); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(sp.Error); v != "" {
		return v
	}
	return ""
}

func truncateRunes(s string, max int) string {
	if max <= 0 || s == "" {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

func httpStatusName(code int) string {
	switch code {
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 408:
		return "Request Timeout"
	case 429:
		return "Too Many Requests"
	case 500:
		return "Internal Server Error"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	case 504:
		return "Gateway Timeout"
	default:
		if code >= 500 {
			return "Server Error"
		}
		if code >= 400 {
			return "Client Error"
		}
		return ""
	}
}

func formatDuration(ms float64) string {
	if ms >= 1000 {
		sec := ms / 1000
		if sec >= 100 {
			return strconv.FormatFloat(sec, 'f', 2, 64) + "s"
		}
		return strconv.FormatFloat(sec, 'f', 2, 64) + "s"
	}
	if ms >= 1 {
		if ms >= 10 {
			return strconv.FormatFloat(ms, 'f', 0, 64) + "ms"
		}
		return strconv.FormatFloat(ms, 'f', 1, 64) + "ms"
	}
	return strconv.FormatFloat(ms, 'f', 1, 64) + "ms"
}

func parseTimeoutValue(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d, true
	}
	if n, err := strconv.ParseFloat(raw, 64); err == nil && n > 0 {
		if n >= 1000 {
			return time.Duration(n) * time.Millisecond, true
		}
		return time.Duration(n * float64(time.Second)), true
	}
	return 0, false
}

var timeoutAttrKeys = []string{
	"http.server.timeout",
	"http.client.timeout",
	"http.request.timeout",
	"timeout.ms",
	"proxy.timeout",
	"PROXY_TIMEOUT",
	"proxy.timeout.ms",
}

func spanTimeout(sp *models.Span, opts Options) (time.Duration, bool) {
	if sp != nil && sp.Attributes != nil {
		for _, k := range timeoutAttrKeys {
			if v := sp.Attributes[k]; v != "" {
				if d, ok := parseTimeoutValue(v); ok {
					return d, true
				}
			}
		}
	}
	if opts.KnownTimeouts == nil || sp == nil {
		return 0, false
	}
	if d, ok := opts.KnownTimeouts[sp.SpanID]; ok && d > 0 {
		return d, true
	}
	if d, ok := opts.KnownTimeouts[sp.ServiceName]; ok && d > 0 {
		return d, true
	}
	return 0, false
}

func isRootParentID(id string) bool {
	return spantree.IsRoot(id)
}

func operationIdentity(sp *models.Span) string {
	method, _ := httpMethodRaw(sp)
	return strings.ToUpper(strings.TrimSpace(method)) + " " + httpPath(sp) + " " + httpURL(sp)
}
