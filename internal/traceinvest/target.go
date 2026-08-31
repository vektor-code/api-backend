package traceinvest

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

// Target is the source workload and destination extracted from the trace.
type Target struct {
	Namespace    string
	Workload     string
	SourcePod    string
	DestHost     string
	DestPort     string
	DestPath     string
	DestURL      string
	DestType     string
	DestProtocol string
	RecordedHTTP int
	CheckType    string
}

func (t Target) CacheKey() string {
	return strings.Join([]string{t.Namespace, t.Workload, t.DestHost + ":" + t.DestPort + t.DestPath, t.CheckType}, "|")
}

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

func destURLOf(sp *models.Span) string {
	return attr(sp, "url.full", "http.url")
}

func destHostOf(sp *models.Span) string {
	return attr(sp, "server.address", "net.peer.name", "net.sock.peer.addr", "http.host")
}

func ExtractTarget(trace *models.Trace, diag *tracediag.Diagnosis) Target {
	t := Target{CheckType: "http"}
	if trace == nil {
		return t
	}
	t.Namespace = trace.Namespace
	t.Workload = trace.ServiceName

	var focus *models.Span
	if diag != nil {
		ids := map[string]struct{}{}
		for _, id := range diag.AffectedSpanIDs {
			ids[id] = struct{}{}
		}
		var first *models.Span
		for _, sp := range trace.Spans {
			if _, ok := ids[sp.SpanID]; !ok {
				continue
			}
			if first == nil {
				first = sp
			}
			if isStatefulRemote(sp) {
				focus = sp
				break
			}
		}
		if focus == nil {
			focus = first
		}
	}
	if focus == nil && trace.RootSpan != nil {
		focus = trace.RootSpan
	}
	if focus == nil && len(trace.Spans) > 0 {
		focus = trace.Spans[0]
	}

	var client *models.Span
	for _, sp := range trace.Spans {
		if sp.Kind == models.SpanKindClient {
			client = sp
			break
		}
	}

	source := client
	if source == nil {
		source = focus
	}
	if source != nil {
		if source.Namespace != "" {
			t.Namespace = source.Namespace
		}
		if ns := attr(source, "k8s.namespace.name"); ns != "" {
			t.Namespace = ns
		}
		if source.ServiceName != "" {
			t.Workload = source.ServiceName
		}
		if w := attr(source, "k8s.deployment.name", "service.name"); w != "" {
			t.Workload = w
		}
		t.SourcePod = source.PodName
		if p := attr(source, "k8s.pod.name"); p != "" {
			t.SourcePod = p
		}
	}

	urlSpan := focus
	if destHostOf(urlSpan) == "" && destURLOf(urlSpan) == "" && client != nil {
		urlSpan = client
	}
	rawURL := destURLOf(urlSpan)
	host := destHostOf(urlSpan)
	port := attr(urlSpan, "server.port", "net.peer.port")
	path := attr(urlSpan, "url.path", "http.target", "http.route")
	if rawURL != "" {
		if u, err := url.Parse(rawURL); err == nil && u.Host != "" {
			t.DestURL = u.Scheme + "://" + u.Host + u.EscapedPath()
			if u.RawQuery != "" {
				t.DestURL += "?" + u.RawQuery
			}
			h, p, err := net.SplitHostPort(u.Host)
			if err == nil {
				t.DestHost, t.DestPort = h, p
			} else {
				t.DestHost = u.Host
				if u.Scheme == "https" {
					t.DestPort = "443"
				} else {
					t.DestPort = "80"
				}
			}
			if u.Path != "" {
				t.DestPath = u.Path
			}
		}
	}
	if t.DestHost == "" {
		t.DestHost = host
	}
	if t.DestPort == "" {
		t.DestPort = port
	}
	if t.DestPath == "" {
		t.DestPath = path
	}
	if t.DestPath == "" {
		t.DestPath = "/"
	}
	t.DestProtocol = protocolFromSpan(urlSpan, t.DestPort)
	if t.DestProtocol == "http" || t.DestProtocol == "https" || t.DestProtocol == "" {
		if t.DestURL == "" && t.DestHost != "" {
			port := t.DestPort
			if port == "" {
				port = "80"
			}
			scheme := "http"
			if port == "443" || t.DestProtocol == "https" {
				scheme = "https"
			}
			t.DestURL = scheme + "://" + net.JoinHostPort(t.DestHost, port) + t.DestPath
		}
		t.CheckType = "http"
	} else {
		t.DestURL = ""
		t.CheckType = "tcp"
	}
	if t.CheckType == "http" {
		if status := attr(focus, "http.response.status_code", "http.status_code"); status != "" {
			if n, err := strconv.Atoi(status); err == nil {
				t.RecordedHTTP = n
			}
		}
	}
	if t.Namespace == "" {
		t.Namespace = "default"
	}
	t.DestType = classifyDestinationType(t.DestHost)
	return t
}

func isStatefulRemote(sp *models.Span) bool {
	if sp == nil {
		return false
	}
	if attr(sp, "db.system", "db.system.name", "db.statement", "db.query.text") != "" {
		return true
	}
	port := attr(sp, "server.port", "net.peer.port")
	if host := destHostOf(sp); host != "" {
		if _, p, err := net.SplitHostPort(host); err == nil && p != "" && port == "" {
			port = p
		}
	}
	id := protocolFromPort(port)
	return id != "" && id != "http" && id != "https"
}

func protocolFromSpan(sp *models.Span, port string) string {
	if sys := attr(sp, "db.system", "db.system.name", "crnet.apm.dependency.system"); sys != "" {
		return strings.ToLower(sys)
	}
	if sys := attr(sp, "rpc.system"); strings.EqualFold(sys, "grpc") {
		return "grpc"
	}
	if id := protocolFromPort(port); id != "" {
		return id
	}
	return "http"
}

func protocolFromPort(port string) string {
	switch strings.TrimSpace(port) {
	case "5432", "5433", "6432":
		return "postgresql"
	case "3306", "33060":
		return "mysql"
	case "6379", "6380":
		return "redis"
	case "27017", "27018", "27019":
		return "mongodb"
	case "9092", "9093", "9094":
		return "kafka"
	case "5672", "5671":
		return "rabbitmq"
	case "1433":
		return "mssql"
	case "1521":
		return "oracle"
	case "9200", "9300":
		return "elasticsearch"
	case "11211":
		return "memcached"
	case "2379":
		return "etcd"
	case "50051":
		return "grpc"
	case "443":
		return "https"
	case "80", "8080", "8000", "8443":
		return "http"
	default:
		return ""
	}
}

func classifyDestinationType(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	switch h {
	case "", "localhost", "127.0.0.1", "::1":
		if h == "" {
			return "unknown"
		}
		return "localhost"
	}
	if ip := net.ParseIP(h); ip != nil {
		if ip.IsLoopback() {
			return "localhost"
		}
		if ip.IsPrivate() {
			return "private_ip"
		}
		return "external_ip"
	}
	if strings.Contains(h, ".svc.") || strings.HasSuffix(h, ".svc") || strings.HasSuffix(h, ".cluster.local") {
		return "kubernetes_dns"
	}
	return "external_dns"
}
