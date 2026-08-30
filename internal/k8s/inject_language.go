package k8s

import "strings"

// resolveInject maps a chosen tech stack onto the process that is actually
// running. Workload names are ignored — operators already set java / nodejs /
// nginx / … per service. The process wins only when requested is empty or is
// nodejs pointed at a static HTTP server (SPA images are often labelled
// nodejs because of package.json while the container runs nginx).
func resolveInject(requested string, images, commands []string) string {
	req := injectCanon(requested)
	rt := processStack(images, commands)
	if rt == "nginx" || rt == "apache-httpd" {
		if req == "" || req == "nodejs" || req == "nginx" || req == "apache-httpd" {
			return rt
		}
	}
	if req != "" {
		return req
	}
	return rt
}

func processStack(images, commands []string) string {
	blob := strings.ToLower(strings.Join(images, " ") + " " + strings.Join(commands, " "))
	switch {
	case blobContains(blob, "openjdk", "eclipse-temurin", "tomcat", "spring-boot", "/jre", "/jdk"):
		return "java"
	case blobContains(blob, "node:", "nodejs", "/node ", "npm", "pm2"):
		return "nodejs"
	case blobContains(blob, "python", "gunicorn", "uvicorn"):
		return "python"
	case blobContains(blob, "dotnet", "aspnet"):
		return "dotnet"
	case blobContains(blob, "php-fpm", "php:"):
		return "php"
	case blobContains(blob, "httpd", "apache2"):
		return "apache-httpd"
	case blobContains(blob, "nginx", "openresty", "caddy"):
		return "nginx"
	}
	return ""
}

func injectCanon(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "node", "javascript", "typescript":
		return "nodejs"
	case "apache", "httpd", "apachehttpd", "apache-httpd":
		return "apache-httpd"
	default:
		return strings.ToLower(strings.TrimSpace(s))
	}
}

func languageFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	for _, key := range []string{
		"language",
		"tech-stack",
		"app.kubernetes.io/language",
		"tags.datadoghq.com/language",
		"instrumentation.opentelemetry.io/container-language",
	} {
		if val := strings.TrimSpace(labels[key]); val != "" {
			return injectCanon(val)
		}
	}
	return ""
}

func blobContains(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}
