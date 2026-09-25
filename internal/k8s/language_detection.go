package k8s

import (
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func imagesAndCommands(containers []corev1.Container) (images, commands []string) {
	return containerImagesAndCommands(containers)
}

// Language detection follows the same model as Datadog APM:
//
//  1. Live process cmdline (ground truth — what is actually running)
//  2. Declared container command/args (same signals, when pod has not started yet)
//  3. Explicit language labels (operator override)
//  4. Well-known runtime base images / strong runtime env
//
// inject-* annotations, volume paths, and workload/pod name guesses are never
// used for detection — those were legacy heuristics that mislabeled SPA/
// opaque private images.

// languageFromProcessCmdline maps PID 1 cmdline (null- or space-separated) to a runtime.
func languageFromProcessCmdline(cmdline string) string {
	s := strings.ToLower(strings.ReplaceAll(cmdline, "\x00", " "))
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return languageFromArgv(strings.Fields(s), s)
}

// detectLanguage prefers live process cmdline, then declarative pod signals.
// inject-* annotations are never consulted.
func detectLanguage(pod *corev1.Pod, processCmdline string) string {
	if pod == nil {
		return ""
	}
	if lang := languageFromProcessCmdline(processCmdline); lang != "" {
		return lang
	}
	return detectLanguageFromPodSpec(pod)
}

// detectLanguageFromPodSpec is the cold-start / Pending-pod fallback when no
// live process has been observed yet.
func detectLanguageFromPodSpec(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	apps := appContainers(pod.Spec.Containers)

	// Declared command/args are the same signal as process cmdline.
	if lang := languageFromContainersCommand(apps); lang != "" {
		return lang
	}
	if lang := normalizeDetectLang(languageFromLabels(pod.Labels)); lang != "" {
		// Label says nodejs but the container image is clearly nginx/httpd —
		// trust the process image (SPA packages often keep a nodejs label).
		if isStaticHTTPStack(lang) {
			return lang
		}
		images, commands := imagesAndCommands(apps)
		if coerced := resolveInject(lang, images, commands); coerced != "" {
			return coerced
		}
		return lang
	}
	if lang := languageFromContainersEnv(apps); lang != "" {
		return lang
	}
	if lang := languageFromContainersImage(apps); lang != "" {
		return lang
	}
	return ""
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
			return val
		}
	}
	return ""
}

func normalizeDetectLang(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	switch injectCanon(raw) {
	case "java", "nodejs", "python", "dotnet", "go", "nginx", "apache-httpd", "php", "ruby", "sdk":
		return injectCanon(raw)
	case "rails":
		return "ruby"
	}
	switch {
	case containsAny(raw, "java", "jvm", "spring", "kotlin", "scala", "quarkus", "micronaut"):
		return "java"
	case containsAny(raw, "node", "javascript", "typescript", "express", "nestjs", "next"):
		return "nodejs"
	case containsAny(raw, "python", "django", "flask", "fastapi", "uvicorn", "gunicorn"):
		return "python"
	case containsAny(raw, "dotnet", ".net", "csharp", "aspnet"):
		return "dotnet"
	case raw == "go" || raw == "golang" || strings.HasPrefix(raw, "go-") || strings.HasSuffix(raw, "-go") || strings.Contains(raw, "golang"):
		return "go"
	case containsAny(raw, "nginx", "openresty"):
		return "nginx"
	case containsAny(raw, "apache", "httpd"):
		return "apache-httpd"
	case containsAny(raw, "php", "laravel", "wordpress"):
		return "php"
	case containsAny(raw, "ruby", "rails", "puma"):
		return "ruby"
	}
	return ""
}

func languageFromContainersCommand(containers []corev1.Container) string {
	bestLang := ""
	bestScore := 0
	for _, c := range containers {
		lang := languageFromCommandWithImage(c)
		if lang == "" {
			continue
		}
		score := 300
		name := strings.ToLower(c.Name)
		if containsAny(name, "app", "api", "backend", "server", "web", "svc", "service", "main", "worker") {
			score += 20
		}
		if score > bestScore {
			bestLang, bestScore = lang, score
		}
	}
	return bestLang
}

func languageFromContainersEnv(containers []corev1.Container) string {
	bestLang := ""
	bestScore := 0
	for _, c := range containers {
		if lang := languageFromEnv(c.Env); lang != "" {
			score := 200
			if score > bestScore {
				bestLang, bestScore = lang, score
			}
		}
	}
	return bestLang
}

func languageFromContainersImage(containers []corev1.Container) string {
	bestLang := ""
	bestScore := 0
	for _, c := range containers {
		if lang := languageFromImage(c.Image); lang != "" {
			score := 100
			name := strings.ToLower(c.Name)
			if containsAny(name, "app", "api", "backend", "server", "web", "svc", "service", "main", "worker") {
				score += 20
			}
			if score > bestScore {
				bestLang, bestScore = lang, score
			}
		}
	}
	return bestLang
}

func languageFromEnv(env []corev1.EnvVar) string {
	for _, e := range env {
		name := strings.ToUpper(e.Name)
		switch {
		case name == "JAVA_TOOL_OPTIONS", name == "JAVA_HOME", strings.HasPrefix(name, "JDK_"),
			strings.HasPrefix(name, "JAVA_"), name == "SPRING_PROFILES_ACTIVE", name == "CATALINA_HOME":
			return "java"
		case name == "NODE_ENV", name == "NODE_OPTIONS", strings.HasPrefix(name, "NPM_"), name == "NPM_CONFIG_LOGLEVEL":
			return "nodejs"
		case name == "PYTHONPATH", name == "PYTHONUNBUFFERED", name == "UVICORN_HOST", name == "GUNICORN_CMD_ARGS",
			name == "DJANGO_SETTINGS_MODULE", name == "FLASK_APP", strings.HasPrefix(name, "PYTHON_"):
			return "python"
		case strings.HasPrefix(name, "ASPNETCORE_"), strings.HasPrefix(name, "DOTNET_"), name == "DOTNET_ROOT":
			return "dotnet"
		case name == "PHP_INI_SCAN_DIR", strings.HasPrefix(name, "PHP_"):
			return "php"
		case name == "RAILS_ENV", name == "RACK_ENV", name == "BUNDLE_PATH", name == "BUNDLE_APP_CONFIG",
			strings.HasPrefix(name, "RUBY"):
			return "ruby"
		case name == "GOPATH", name == "GOROOT", name == "CGO_ENABLED":
			return "go"
		case name == "NGINX_ENTRYPOINT_QUIET_LOGS", name == "NGINX_VERSION":
			return "nginx"
		}
	}
	return ""
}

// languageFromImage matches well-known runtime base images only.
// Private tags like registry/.../app-frontend:dev-388 intentionally return "".
func languageFromImage(image string) string {
	img := strings.ToLower(image)
	repo := img
	if i := strings.LastIndex(repo, "@"); i >= 0 {
		repo = repo[:i]
	}
	if i := strings.LastIndex(repo, ":"); i >= 0 {
		after := repo[i+1:]
		if !strings.Contains(after, "/") {
			repo = repo[:i]
		}
	}
	base := path.Base(repo)

	switch {
	case containsAny(img, "openjdk", "eclipse-temurin", "temurin", "amazoncorretto", "corretto",
		"microsoft-openjdk", "ibm-semeru", "liberica", "sapmachine", "graalvm", "distroless/java",
		"tomcat", "wildfly", "jboss", "payara", "weblogic", "spring-boot",
		"azul/zulu", "bellsoft", "adoptium", "chainguard/jdk", "chainguard/jre",
		"bitnami/java", "bitnami/tomcat", "quarkus", "micronaut"):
		return "java"
	case containsAny(img, "node:", "nodejs", "distroless/nodejs", "/node@", "bitnami/node", "oven/bun",
		"chainguard/node", "cgr.dev/chainguard/node"):
		return "nodejs"
	case base == "node" || strings.HasPrefix(base, "node-"):
		return "nodejs"
	case containsAny(img, "python", "gunicorn", "uvicorn", "distroless/python",
		"bitnami/python", "pypy", "chainguard/python", "cgr.dev/chainguard/python"):
		return "python"
	case base == "python" || strings.HasPrefix(base, "python"):
		return "python"
	case containsAny(img, "php-fpm", "wordpress", "laravel", "bitnami/php", "php:", "chainguard/php"):
		return "php"
	case base == "php" || strings.HasPrefix(base, "php"):
		return "php"
	case containsAny(img, "ruby", "rails", "puma", "bitnami/ruby", "chainguard/ruby"):
		return "ruby"
	case containsAny(img, "dotnet", "aspnet", "microsoft-dotnet", "mcr.microsoft.com/dotnet"):
		return "dotnet"
	case containsAny(img, "nginx", "openresty", "bitnami/nginx", "chainguard/nginx"):
		return "nginx"
	case containsAny(img, "httpd", "apache2", "bitnami/apache"):
		return "apache-httpd"
	case containsAny(img, "golang", "library/golang", "chainguard/go", "bitnami/golang"):
		return "go"
	case containsAny(img, "distroless/static", "gcr.io/distroless/static", "gcr.io/distroless/base", "distroless/base"):
		return "go"
	}
	return ""
}

func languageFromCommand(c corev1.Container) string {
	parts := append(append([]string{}, c.Command...), c.Args...)
	joined := strings.ToLower(strings.Join(parts, " "))

	for i, part := range parts {
		base := strings.ToLower(filepathBase(part))
		if (base == "sh" || base == "bash" || base == "ash" || base == "dash") && i+1 < len(parts) {
			for j := i + 1; j < len(parts); j++ {
				if parts[j] == "-c" && j+1 < len(parts) {
					return languageFromArgv(nil, parts[j+1])
				}
			}
		}
	}
	return languageFromArgv(parts, joined)
}

// languageFromArgv is the shared Datadog-style argv classifier used for both
// live /proc cmdline and declared pod command/args.
func languageFromArgv(parts []string, joinedLower string) string {
	s := strings.ToLower(strings.TrimSpace(joinedLower))
	if s == "" && len(parts) == 0 {
		return ""
	}
	if s == "" {
		s = strings.ToLower(strings.Join(parts, " "))
	}

	if strings.Contains(s, "nginx") {
		if strings.Contains(s, "master process") || strings.Contains(s, "worker process") ||
			strings.Contains(s, "daemon off") || strings.HasPrefix(strings.TrimSpace(s), "nginx") {
			return "nginx"
		}
	}
	if containsAny(s, "httpd", "apache2", "apachectl") {
		return "apache-httpd"
	}

	for _, part := range parts {
		base := strings.ToLower(filepathBase(part))
		switch {
		case base == "java" || base == "jsvc" || strings.HasSuffix(base, ".jar"):
			return "java"
		case base == "node" || base == "nodejs" || base == "npm" || base == "npx" || base == "yarn" ||
			base == "pnpm" || base == "bun" || base == "next-server" ||
			strings.HasSuffix(base, ".js") || strings.HasSuffix(base, ".mjs") ||
			strings.HasSuffix(base, ".cjs") || strings.HasSuffix(base, ".ts"):
			return "nodejs"
		case base == "python" || base == "python3" || base == "python2" || base == "gunicorn" ||
			base == "uvicorn" || base == "uwsgi" || strings.HasSuffix(base, ".py"):
			return "python"
		case base == "php" || base == "php-fpm" || strings.HasPrefix(base, "php-fpm"):
			return "php"
		case base == "ruby" || base == "bundle" || base == "puma" || base == "rails" || base == "rackup":
			return "ruby"
		case base == "dotnet" || strings.HasSuffix(base, ".dll"):
			return "dotnet"
		case base == "nginx" || base == "nginx-debug" || base == "openresty":
			return "nginx"
		case base == "httpd" || base == "apache2" || base == "apachectl":
			return "apache-httpd"
		}
	}

	switch {
	case strings.Contains(s, "java") && !strings.Contains(s, "javascript") &&
		(strings.Contains(s, "-jar") || strings.Contains(s, ".jar") || strings.Contains(s, "spring-boot")):
		return "java"
	case strings.Contains(s, "next-server") || strings.Contains(s, "next start") || strings.Contains(s, "nest start"):
		return "nodejs"
	case containsAny(s, "node ", "nodejs ", "npm ", "npx ", "yarn ", "pnpm ", "bun "):
		return "nodejs"
	case containsAny(s, "python3", "python ", "gunicorn", "uvicorn", "uwsgi"):
		return "python"
	case containsAny(s, "php-fpm", "php ", "artisan"):
		return "php"
	case containsAny(s, "puma", "rails ", "bundle exec", "rackup"):
		return "ruby"
	case strings.Contains(s, "dotnet "):
		return "dotnet"
	case containsAny(s, "nginx", "openresty"):
		return "nginx"
	case containsAny(s, "httpd", "apache2", "apachectl"):
		return "apache-httpd"
	}

	// Absolute non-script binary on distroless/static is a weak Go signal —
	// only applied when callers pass image context via languageFromCommand.
	return ""
}

func languageFromCommandWithImage(c corev1.Container) string {
	if lang := languageFromCommand(c); lang != "" {
		return lang
	}
	img := strings.ToLower(c.Image)
	if !containsAny(img, "distroless/static", "distroless/base", "scratch", "golang") {
		return ""
	}
	parts := append(append([]string{}, c.Command...), c.Args...)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || strings.HasPrefix(part, "-") {
			continue
		}
		base := filepathBase(part)
		lower := strings.ToLower(base)
		if wrappers[lower] || wrappers[part] {
			continue
		}
		if strings.HasPrefix(part, "/") && !strings.Contains(lower, ".") && !looksScript(lower) && !genericProcessName(lower) {
			return "go"
		}
	}
	return ""
}

func looksScript(base string) bool {
	return strings.HasSuffix(base, ".sh") || strings.HasSuffix(base, ".bash") ||
		strings.HasSuffix(base, ".py") || strings.HasSuffix(base, ".js") ||
		strings.HasSuffix(base, ".rb") || strings.HasSuffix(base, ".pl")
}

var wrappers = map[string]bool{
	"sh": true, "bash": true, "ash": true, "dash": true, "busybox": true,
	"/bin/sh": true, "/bin/bash": true, "/bin/ash": true, "/usr/bin/env": true, "env": true,
	"entrypoint.sh": true, "docker-entrypoint.sh": true, "dumb-init": true, "tini": true,
}

func appContainers(containers []corev1.Container) []corev1.Container {
	out := make([]corev1.Container, 0, len(containers))
	for _, c := range containers {
		if isSidecarContainer(c.Name, c.Image) {
			continue
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return containers
	}
	return out
}

func isSidecarContainer(name, image string) bool {
	n := strings.ToLower(name)
	img := strings.ToLower(image)
	sidecars := []string{
		"istio-proxy", "istio-init", "vault-agent", "vault", "linkerd-proxy", "linkerd-init",
		"consul-sidecar", "consul-connect", "datadog-agent", "dsd-client", "fluent-bit", "fluentd",
		"filebeat", "logstash", "cloudsql-proxy", "cloud-sql-proxy", "alloy", "grafana-agent",
		"otel-collector", "opentelemetry-collector", "jaeger-agent", "promtail", "vector",
		"aws-otel", "k8s-sidecar", "config-reloader", "istio-validation",
	}
	for _, s := range sidecars {
		if n == s || strings.HasPrefix(n, s+"-") || strings.Contains(img, s) {
			return true
		}
	}
	return false
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}

func filepathBase(value string) string {
	value = strings.TrimSpace(value)
	if i := strings.LastIndexAny(value, "/\\"); i >= 0 {
		return value[i+1:]
	}
	return value
}
