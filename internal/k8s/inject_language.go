package k8s

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// resolveInject maps a chosen tech stack onto the process that is actually
// running. The process wins when requested is empty or is nodejs/python/php
// pointed at a static HTTP server (SPA images are often labelled nodejs).
func resolveInject(requested string, images, commands []string) string {
	req := injectCanon(requested)
	rt := processStack(images, commands)
	if rt == "nginx" || rt == "apache-httpd" {
		if req == "" || req == "nodejs" || req == "nginx" || req == "apache-httpd" || req == "python" || req == "php" {
			return rt
		}
	}
	if req != "" {
		return req
	}
	return rt
}

func processStack(images, commands []string) string {
	if lang := languageFromCommand(corev1.Container{Command: commands}); lang != "" {
		return lang
	}
	for _, img := range images {
		if lang := languageFromImage(img); lang != "" {
			return lang
		}
	}
	blob := strings.ToLower(strings.Join(images, " ") + " " + strings.Join(commands, " "))
	switch {
	case blobContains(blob, "openjdk", "eclipse-temurin", "tomcat", "spring-boot", "/jre", "/jdk", "amazoncorretto", "zulu"):
		return "java"
	case blobContains(blob, "node:", "nodejs", "/node ", "npm", "pm2", "yarn", "pnpm", "bun"):
		return "nodejs"
	case blobContains(blob, "python", "gunicorn", "uvicorn", "django", "flask"):
		return "python"
	case blobContains(blob, "dotnet", "aspnet"):
		return "dotnet"
	case blobContains(blob, "php-fpm", "php:"):
		return "php"
	case blobContains(blob, "ruby", "rails", "puma"):
		return "ruby"
	case blobContains(blob, "httpd", "apache2"):
		return "apache-httpd"
	case blobContains(blob, "nginx", "openresty", "caddy"):
		return "nginx"
	case blobContains(blob, "golang", "/go:", "distroless/static"):
		return "go"
	}
	return ""
}

func injectCanon(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "node", "javascript", "typescript", "js", "ts", "nodejs":
		return "nodejs"
	case "apache", "httpd", "apachehttpd", "apache-httpd":
		return "apache-httpd"
	case "golang", "go":
		return "go"
	case "rails":
		return "ruby"
	case ".net", "csharp", "c#":
		return "dotnet"
	default:
		return strings.ToLower(strings.TrimSpace(s))
	}
}

func blobContains(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}
