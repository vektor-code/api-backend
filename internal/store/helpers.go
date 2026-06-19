package store

import (
	"net"
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

// getInfraNodeName constructs a specific and unique identifier for database/queue target nodes
func getInfraNodeName(span *models.Span, baseName string) string {
	if strings.ToLower(baseName) == "dns" {
		queryName := span.Attributes["dns.question.name"]
		if queryName == "" {
			queryName = span.Attributes["dns.question"]
		}
		if queryName == "" {
			queryName = span.Attributes["dns.name"]
		}
		if queryName == "" {
			queryName = span.Attributes["net.peer.name"]
		}
		if queryName == "" {
			queryName = span.Attributes["server.address"]
		}
		// If the span name contains the query (e.g. "DNS lookup: google.com")
		if queryName == "" && strings.Contains(span.Name, ":") {
			parts := strings.SplitN(span.Name, ":", 2)
			queryName = strings.TrimSpace(parts[1])
		}

		if queryName != "" {
			return "DNS (" + strings.ToLower(queryName) + ")"
		}
		return "DNS"
	}

	dbName := span.Attributes["db.name"]
	msgDest := span.Attributes["messaging.destination"]
	if msgDest == "" {
		msgDest = span.Attributes["messaging.destination.name"]
	}
	if msgDest == "" {
		msgDest = span.Attributes["messaging.destination_name"]
	}
	if msgDest == "" {
		msgDest = span.Attributes["messaging.dest"]
	}

	peerName := span.Attributes["net.peer.name"]
	if peerName == "" {
		peerName = span.Attributes["server.address"]
	}
	if peerName == "" {
		peerName = span.Attributes["peer.service"]
	}
	if peerName == "" {
		peerName = span.Attributes["net.peer.ip"]
	}
	if peerName == "" {
		peerName = span.Attributes["network.peer.address"]
	}

	resourceName := dbName
	if resourceName == "" {
		resourceName = msgDest
	}

	resource := ""
	if peerName != "" && resourceName != "" {
		resource = peerName + "/" + resourceName
	} else if resourceName != "" {
		resource = resourceName
	} else if peerName != "" {
		resource = peerName
	} else {
		resource = span.ServiceName
	}

	return strings.ToLower(baseName) + " (" + resource + ")"
}

// getClientDependencyName parses target hostname/identity from HTTP/gRPC client spans
func getClientDependencyName(span *models.Span) string {
	if peer := span.Attributes["peer.service"]; peer != "" {
		return peer
	}
	
	if strings.Contains(strings.ToLower(span.Name), "vault") {
		return "vault"
	}
	if urlStr := span.Attributes["http.url"]; urlStr != "" && strings.Contains(strings.ToLower(urlStr), "vault") {
		return "vault"
	}

	if strings.Contains(strings.ToLower(span.Name), "dns") {
		return "dns"
	}

	host := span.Attributes["server.address"]
	if host == "" {
		host = span.Attributes["net.peer.name"]
	}
	if host == "" {
		host = span.Attributes["http.host"]
	}
	
	if host != "" {
		if idx := strings.Index(host, ":"); idx != -1 {
			return host[:idx]
		}
		return host
	}

	if urlStr := span.Attributes["http.url"]; urlStr != "" {
		if idx := strings.Index(urlStr, "://"); idx != -1 {
			rem := urlStr[idx+3:]
			if endIdx := strings.IndexAny(rem, ":/"); endIdx != -1 {
				return rem[:endIdx]
			}
			return rem
		}
	}

	if span.Name != "" {
		return span.Name
	}

	return "external"
}

// parseK8sServiceAndNamespace extracts the clean microservice name and namespace from a target address
func (s *Store) parseK8sServiceAndNamespace(host string) (string, string) {
	if host == "" {
		return "", ""
	}
	host = strings.ToLower(host)
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}
	if idx := strings.Index(host, "://"); idx != -1 {
		host = host[idx+3:]
	}
	if idx := strings.Index(host, "/"); idx != -1 {
		host = host[:idx]
	}

	// Custom mapping for domain names to internal Kubernetes services
	// e.g. rmis-gendoc-dev.mof.az -> service: gendoc-backend, namespace: rmis-dev
	nsPrefixes := []string{"rmis", "emuhasibatliq", "econtract"}
	for _, pref := range nsPrefixes {
		prefixDash := pref + "-"
		if strings.HasPrefix(host, prefixDash) {
			cleanHost := host
			if strings.HasSuffix(cleanHost, ".mof.az") {
				cleanHost = strings.TrimSuffix(cleanHost, ".mof.az")
			}
			if strings.HasSuffix(cleanHost, "-dev") {
				svc := strings.TrimPrefix(cleanHost, prefixDash)
				svc = strings.TrimSuffix(svc, "-dev")
				targetSvc := svc
				if !strings.HasSuffix(targetSvc, "-backend") && !strings.HasSuffix(targetSvc, "-frontend") {
					targetSvc = svc + "-backend"
				}
				return targetSvc, pref + "-dev"
			}
		}
	}

	if net.ParseIP(host) != nil {
		return host, ""
	}

	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return "", ""
	}

	first := parts[0]
	// If the service name ends with our standard suffixes, extract it and attempt to parse the namespace
	if strings.HasSuffix(first, "-backend") || strings.HasSuffix(first, "-frontend") || first == "gateway" || first == "ingress-nginx" {
		if len(parts) >= 2 {
			return first, parts[1]
		}
		return first, ""
	}

	// Case 1: service.namespace.svc.cluster.local or service.namespace.svc
	if len(parts) >= 3 && parts[2] == "svc" {
		return parts[0], parts[1]
	}

	// Case 2: service.namespace where namespace is a known namespace
	if len(parts) == 2 {
		ns := parts[1]
		s.statsMu.RLock()
		hasNs := false
		for key := range s.statsCache {
			if strings.HasPrefix(key, ns+":") {
				hasNs = true
				break
			}
		}
		s.statsMu.RUnlock()
		if hasNs {
			return parts[0], ns
		}
	}

	// Case 3: check if any subsequent part matches a known namespace
	if len(parts) > 2 {
		s.statsMu.RLock()
		defer s.statsMu.RUnlock()
		for i := 1; i < len(parts); i++ {
			ns := parts[i]
			for key := range s.statsCache {
				if strings.HasPrefix(key, ns+":") {
					return parts[0], ns
				}
			}
		}
	}

	return parts[0], ""
}
