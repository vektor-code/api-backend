package tracediag

import (
	"strconv"
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

// Runtime-agnostic failure signals. Exception types and error strings differ
// by language (Go, Java, Node, Python, .NET, PHP, Ruby) but map to the same
// transport / timeout / DNS classes.

func containsAny(s string, needles ...string) bool {
	if s == "" {
		return false
	}
	for _, n := range needles {
		if n != "" && strings.Contains(s, n) {
			return true
		}
	}
	return false
}

func isTimeoutText(s string) bool {
	s = strings.ToLower(s)
	return containsAny(s,
		"timeout", "timed out", "timedout", "etimedout",
		"deadline exceeded", "deadline_exceeded", "context deadline",
		"i/o timeout", "io timeout",
		"taskcanceled", "task canceled", "task cancelled",
		"operation timed out", "connect timed out", "read timed out",
		"curl error 28", "curl: (28)",
		"und_err_connect_timeout", "und_err_headers_timeout", "und_err_body_timeout",
		"net::opentimeout", "net::readtimeout",
		"http.client.timeout",
	)
}

func isResetText(s string) bool {
	s = strings.ToLower(s)
	return containsAny(s,
		"connection reset", "econnreset", "connectionreset",
		"broken pipe", "epipe",
		"connection aborted", "econnaborted",
	) || (strings.Contains(s, "wsarecv") && strings.Contains(s, "reset"))
}

func isRefusedText(s string) bool {
	s = strings.ToLower(s)
	if containsAny(s,
		"connection refused", "connectionrefused", "econnrefused", "connectexception",
		"no such host", "unknownhost", "unknown host",
		"enotfound", "eai_again", "eai_nodata", "eai_noname",
		"host unreachable", "network is unreachable", "no route to host",
		"nodename nor servname", "name or service not known",
		"getaddrinfo", "failed to lookup", "name resolution",
		"curl error 6", "curl: (6)", "curl error 7", "curl: (7)",
		"newconnectionerror", "unable to connect", "could not connect",
		"connectionerror",
		"errno::econnrefused", "errno::ehostunreach",
		"dial tcp", "dial udp",
	) {
		return true
	}
	return strings.Contains(s, "refused") && containsAny(s, "connection", "connect")
}

func isConnectSpanName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" {
		return false
	}
	if n == "connect" || strings.HasSuffix(n, ".connect") {
		return true
	}
	return containsAny(n,
		"tcp.connect", "tls.connect", "ssl.connect", "ssl.handshake",
		"net.connect", "socket.connect", "httpclient.connect",
		"dns.lookup", "dns.resolve",
	)
}

func grpcStatus(sp *models.Span) (int, bool) {
	raw := attr(sp, "rpc.grpc.status_code", "rpc.status_code", "grpc.status_code")
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return n, true
}

func grpcStatusName(code int) string {
	switch code {
	case 1:
		return "CANCELLED"
	case 2:
		return "UNKNOWN"
	case 3:
		return "INVALID_ARGUMENT"
	case 4:
		return "DEADLINE_EXCEEDED"
	case 5:
		return "NOT_FOUND"
	case 6:
		return "ALREADY_EXISTS"
	case 7:
		return "PERMISSION_DENIED"
	case 8:
		return "RESOURCE_EXHAUSTED"
	case 9:
		return "FAILED_PRECONDITION"
	case 10:
		return "ABORTED"
	case 11:
		return "OUT_OF_RANGE"
	case 12:
		return "UNIMPLEMENTED"
	case 13:
		return "INTERNAL"
	case 14:
		return "UNAVAILABLE"
	case 15:
		return "DATA_LOSS"
	case 16:
		return "UNAUTHENTICATED"
	default:
		return ""
	}
}

func isRPCSpan(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	if attrPresent(sp, "rpc.system", "rpc.service", "rpc.method", "rpc.grpc.status_code", "rpc.status_code") {
		return true
	}
	lib := strings.ToLower(libraryName(sp))
	return strings.Contains(lib, "grpc") || strings.Contains(lib, "rpc")
}

func isDBSpan(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	if attrPresent(sp,
		"db.system", "db.system.name",
		"db.statement", "db.query.text",
		"db.operation", "db.operation.name",
		"db.name", "db.namespace",
	) {
		return true
	}
	kind := strings.ToLower(attr(sp, "crnet.apm.dependency.kind"))
	if kind == "database" || kind == "cache" {
		return true
	}
	port := attr(sp, "server.port", "net.peer.port")
	if port == "" {
		host := attr(sp, "server.address", "net.peer.name", "net.sock.peer.addr")
		if i := strings.LastIndex(host, ":"); i > 0 && i < len(host)-1 && !strings.Contains(host[i+1:], "]") {
			port = host[i+1:]
		}
	}
	switch port {
	case "5432", "5433", "6432", "3306", "1433", "1521", "27017", "6379", "11211", "9042", "9200":
		return true
	}
	return false
}

func isMessagingSpan(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	return attrPresent(sp, "messaging.system", "messaging.destination", "messaging.operation", "messaging.destination.name")
}

func dbSystem(sp *models.Span) string {
	if sys := attr(sp, "db.system", "db.system.name", "crnet.apm.dependency.system"); sys != "" {
		return sys
	}
	port := attr(sp, "server.port", "net.peer.port")
	if port == "5432" || port == "5433" || port == "6432" {
		return "postgresql"
	}
	return ""
}

func failedSpan(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	if sp.Status == models.SpanStatusError || strings.TrimSpace(sp.Error) != "" {
		return true
	}
	return errorText(sp) != ""
}
