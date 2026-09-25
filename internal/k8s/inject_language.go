package k8s

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// resolveInject maps a chosen tech stack onto the process that is actually
// running. Static HTTP (nginx/apache) wins when the operator picked nodejs/
// python/php for an SPA image — same rule Datadog uses for library injection.
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

// processStack derives runtime from declared command/args, then well-known
// base images only — never from opaque private repository paths.
func processStack(images, commands []string) string {
	if lang := languageFromCommand(corev1.Container{Command: commands}); lang != "" {
		return lang
	}
	for _, img := range images {
		if lang := languageFromImage(img); lang != "" {
			return lang
		}
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
