package k8s

import (
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func imagesAndCommands(containers []corev1.Container) (images, commands []string) {
	return containerImagesAndCommands(containers)
}

// detectLanguageFromPodSpec picks the runtime the OpenTelemetry inject
// annotation should use. Priority:
//  1. Existing inject-* annotations (already instrumented)
//  2. Explicit language labels (normalized)
//  3. App containers: command > env > image (sidecars skipped)
//  4. Pod / workload name heuristics
//  5. processStack fallback over all app image+command blobs
func detectLanguageFromPodSpec(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	apps := appContainers(pod.Spec.Containers)
	images, commands := imagesAndCommands(apps)

	if lang := languageFromAnnotations(pod.Annotations); lang != "" {
		return resolveInject(lang, images, commands)
	}
	if lang := normalizeDetectLang(languageFromLabels(pod.Labels)); lang != "" {
		return resolveInject(lang, images, commands)
	}
	if lang := languageFromContainers(apps); lang != "" {
		return resolveInject(lang, images, commands)
	}
	// Init containers often reveal the build/runtime (e.g. maven, npm) when the
	// main image is a generic distroless/ubuntu binary.
	if lang := languageFromContainers(appContainers(pod.Spec.InitContainers)); lang != "" {
		return resolveInject(lang, images, commands)
	}
	if lang := languageFromVolumes(pod); lang != "" {
		return resolveInject(lang, images, commands)
	}
	if lang := languageFromPodName(pod.Name); lang != "" {
		return resolveInject(lang, images, commands)
	}
	if pod.Labels != nil {
		for _, key := range []string{"app.kubernetes.io/name", "app.kubernetes.io/component", "app", "app.kubernetes.io/instance"} {
			if lang := languageFromPodName(pod.Labels[key]); lang != "" {
				return resolveInject(lang, images, commands)
			}
		}
	}
	return resolveInject("", images, commands)
}

func languageFromAnnotations(annotations map[string]string) string {
	if annotations == nil {
		return ""
	}
	// Prefer more specific inject keys over inject-sdk.
	order := []struct {
		suffix string
		lang   string
	}{
		{"inject-java", "java"},
		{"inject-nodejs", "nodejs"},
		{"inject-python", "python"},
		{"inject-dotnet", "dotnet"},
		{"inject-go", "go"},
		{"inject-nginx", "nginx"},
		{"inject-apache-httpd", "apache-httpd"},
		{"inject-php", "php"},
		{"inject-ruby", "ruby"},
		{"inject-sdk", "sdk"},
	}
	for _, item := range order {
		for key, value := range annotations {
			if value == "" || value == "false" {
				continue
			}
			if strings.Contains(key, item.suffix) {
				return item.lang
			}
		}
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
		"tags.datadoghq.com/env", // not language — skip below via normalize
	} {
		if key == "tags.datadoghq.com/env" {
			continue
		}
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
	case "java", "nodejs", "python", "dotnet", "go", "nginx", "apache-httpd", "php", "ruby", "sdk", "rails":
		if injectCanon(raw) == "rails" {
			return "ruby"
		}
		return injectCanon(raw)
	}
	// Loose label values: "spring", "jvm", "express", …
	switch {
	case containsAny(raw, "java", "jvm", "spring", "kotlin", "scala", "quarkus", "micronaut"):
		return "java"
	case containsAny(raw, "node", "javascript", "typescript", "express", "nestjs", "next"):
		return "nodejs"
	case containsAny(raw, "python", "django", "flask", "fastapi", "uvicorn", "gunicorn"):
		return "python"
	case containsAny(raw, "dotnet", ".net", "csharp", "aspnet"):
		return "dotnet"
	case containsAny(raw, "golang", "go"):
		// avoid matching "mongo", "logo", etc. — require word-ish go
		if raw == "go" || raw == "golang" || strings.HasPrefix(raw, "go-") || strings.HasSuffix(raw, "-go") || strings.Contains(raw, "golang") {
			return "go"
		}
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

// languageFromContainers scores app containers; command beats env beats image.
func languageFromContainers(containers []corev1.Container) string {
	bestLang := ""
	bestScore := 0
	for _, c := range containers {
		if lang, score := scoreContainerLanguage(c); score > bestScore {
			bestLang, bestScore = lang, score
		}
	}
	return bestLang
}

func scoreContainerLanguage(c corev1.Container) (string, int) {
	if lang := languageFromCommand(c); lang != "" {
		return lang, 300
	}
	if lang := languageFromEnv(c.Env); lang != "" {
		return lang, 200
	}
	if lang := languageFromImage(c.Image); lang != "" {
		score := 100
		// Prefer containers that look like the main app.
		name := strings.ToLower(c.Name)
		if containsAny(name, "app", "api", "backend", "server", "web", "svc", "service", "main", "worker") {
			score += 20
		}
		return lang, score
	}
	return "", 0
}

func languageFromEnv(env []corev1.EnvVar) string {
	for _, e := range env {
		name := strings.ToUpper(e.Name)
		switch {
		case name == "JAVA_TOOL_OPTIONS", name == "JAVA_HOME", strings.HasPrefix(name, "JDK_"),
			strings.HasPrefix(name, "JAVA_"), name == "SPRING_PROFILES_ACTIVE", name == "CATALINA_HOME":
			return "java"
		case name == "NODE_ENV", name == "NODE_OPTIONS", strings.HasPrefix(name, "npm_"), name == "NPM_CONFIG_LOGLEVEL":
			return "nodejs"
		case name == "PYTHONPATH", name == "PYTHONUNBUFFERED", name == "UVICORN_HOST", name == "GUNICORN_CMD_ARGS",
			name == "DJANGO_SETTINGS_MODULE", name == "FLASK_APP", strings.HasPrefix(name, "PYTHON_"):
			return "python"
		case strings.HasPrefix(name, "ASPNETCORE_"), strings.HasPrefix(name, "DOTNET_"), name == "DOTNET_ROOT":
			return "dotnet"
		case name == "PHP_INI_SCAN_DIR", strings.HasPrefix(name, "PHP_"), name == "APP_ENV" && looksPHPEnv(env):
			if name != "APP_ENV" {
				return "php"
			}
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

func looksPHPEnv(env []corev1.EnvVar) bool {
	for _, e := range env {
		n := strings.ToUpper(e.Name)
		if strings.HasPrefix(n, "PHP_") || n == "LARAVEL_ENV" {
			return true
		}
	}
	return false
}

func languageFromVolumes(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	blob := ""
	for _, v := range pod.Spec.Volumes {
		blob += " " + strings.ToLower(v.Name)
	}
	for _, c := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		for _, m := range c.VolumeMounts {
			blob += " " + strings.ToLower(m.Name) + " " + strings.ToLower(m.MountPath)
		}
	}
	switch {
	case containsAny(blob, "node_modules", "/.npm", "npm-cache", "yarn-cache", "pnpm-store"):
		return "nodejs"
	case containsAny(blob, "/.m2", "maven", "/.gradle", "gradle-cache"):
		return "java"
	case containsAny(blob, "/.nuget", "nuget", "/app/publish"):
		return "dotnet"
	case containsAny(blob, "pip-cache", "/.cache/pip", "poetry-cache", "venv", "virtualenv"):
		return "python"
	case containsAny(blob, "bundle", "vendor/bundle", "/.gem"):
		return "ruby"
	case containsAny(blob, "composer", "/.composer"):
		return "php"
	case containsAny(blob, "nginx", "html"):
		// weak — only if mount looks like static site root with nginx name
		if containsAny(blob, "nginx") {
			return "nginx"
		}
	}
	return ""
}

func languageFromImage(image string) string {
	img := strings.ToLower(image)
	// Strip digest/tag noise for matching repo name.
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
		"tomcat", "wildfly", "jboss", "payara", "weblogic", "spring-boot", "maven", "gradle",
		"/jre", "/jdk", "jre-", "jdk-", "azul/zulu", "bellsoft", "adoptium", "chainguard/jdk",
		"chainguard/jre", "bitnami/java", "bitnami/tomcat", "quarkus", "micronaut"):
		return "java"
	case containsAny(img, "node:", "nodejs", "distroless/nodejs", "/node@", "bitnami/node", "oven/bun",
		"chainguard/node", "cgr.dev/chainguard/node"):
		return "nodejs"
	case base == "node" || strings.HasPrefix(base, "node-"):
		return "nodejs"
	case containsAny(img, "python", "gunicorn", "uvicorn", "flask", "django", "distroless/python",
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
	case containsAny(img, "nginx", "openresty", "bitnami/nginx", "chainguard/nginx", "caddy"):
		return "nginx"
	case containsAny(img, "httpd", "apache2", "bitnami/apache"):
		return "apache-httpd"
	case containsAny(img, "golang", "library/golang", "chainguard/go", "bitnami/golang"):
		return "go"
	case containsAny(img, "distroless/static", "gcr.io/distroless/static", "gcr.io/distroless/base", "distroless/base"):
		return "go"
	case containsAny(base, "java", "jdk", "jre", "spring", "tomcat"):
		return "java"
	case containsAny(base, "node", "npm"):
		return "nodejs"
	case containsAny(base, "python", "django", "flask"):
		return "python"
	case containsAny(base, "dotnet", "aspnet"):
		return "dotnet"
	case containsAny(base, "nginx", "openresty"):
		return "nginx"
	case containsAny(base, "php"):
		return "php"
	case containsAny(base, "ruby", "rails"):
		return "ruby"
	case base == "go" || strings.HasPrefix(base, "go-") || strings.Contains(base, "golang"):
		return "go"
	}
	return ""
}

func languageFromCommand(c corev1.Container) string {
	parts := append(append([]string{}, c.Command...), c.Args...)
	joined := strings.ToLower(strings.Join(parts, " "))

	// Shell wrappers: look deeper into -c scripts.
	for i, part := range parts {
		base := strings.ToLower(filepathBase(part))
		if (base == "sh" || base == "bash" || base == "ash" || base == "dash") && i+1 < len(parts) {
			// common: sh -c "java -jar ..."
			for j := i + 1; j < len(parts); j++ {
				if parts[j] == "-c" && j+1 < len(parts) {
					return languageFromScript(parts[j+1])
				}
			}
		}
	}

	for _, part := range parts {
		base := strings.ToLower(filepathBase(part))
		switch {
		case base == "java" || strings.HasSuffix(base, ".jar") || base == "jsvc":
			return "java"
		case base == "node" || base == "nodejs" || base == "npm" || base == "npx" || base == "yarn" || base == "pnpm" || base == "bun" ||
			strings.HasSuffix(base, ".js") || strings.HasSuffix(base, ".mjs") || strings.HasSuffix(base, ".cjs") || strings.HasSuffix(base, ".ts"):
			return "nodejs"
		case base == "python" || base == "python3" || base == "python2" || base == "gunicorn" || base == "uvicorn" ||
			base == "uwsgi" || strings.HasSuffix(base, ".py"):
			return "python"
		case base == "php" || base == "php-fpm" || base == "php-fpm8" || base == "php-fpm7":
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

	if lang := languageFromScript(joined); lang != "" {
		return lang
	}
	// Absolute path to a non-script binary is a weak Go signal — only when the
	// image also looks like distroless/scratch/static (common Go packaging).
	img := strings.ToLower(c.Image)
	if containsAny(img, "distroless/static", "distroless/base", "scratch", "golang") {
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
	}
	return ""
}

func languageFromScript(script string) string {
	s := strings.ToLower(script)
	switch {
	case containsAny(s, "java -jar", "java ", "/java ", "spring-boot"):
		return "java"
	case containsAny(s, "node ", "nodejs ", "npm ", "npx ", "yarn ", "pnpm ", "next start", "nest start"):
		return "nodejs"
	case containsAny(s, "python3", "python ", "gunicorn", "uvicorn", "uwsgi"):
		return "python"
	case containsAny(s, "php-fpm", "php ", "artisan"):
		return "php"
	case containsAny(s, "puma", "rails ", "bundle exec", "rackup"):
		return "ruby"
	case containsAny(s, "dotnet "):
		return "dotnet"
	case containsAny(s, "nginx", "openresty"):
		return "nginx"
	case containsAny(s, "httpd", "apache2", "apachectl"):
		return "apache-httpd"
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

func languageFromPodName(name string) string {
	podName := strings.ToLower(strings.TrimSpace(name))
	if podName == "" {
		return ""
	}
	switch {
	case containsAny(podName, "java", "jvm", "spring", "tomcat", "quarkus"):
		return "java"
	case containsAny(podName, "nodejs", "node-", "-node", "express", "nestjs"):
		return "nodejs"
	case containsAny(podName, "python", "django", "flask", "fastapi"):
		return "python"
	case containsAny(podName, "dotnet", "aspnet"):
		return "dotnet"
	case containsAny(podName, "php", "laravel", "wordpress"):
		return "php"
	case containsAny(podName, "ruby", "rails"):
		return "ruby"
	case containsAny(podName, "nginx", "openresty"):
		return "nginx"
	case containsAny(podName, "httpd", "apache"):
		return "apache-httpd"
	case podName == "go" || strings.HasPrefix(podName, "go-") || strings.Contains(podName, "-go-") ||
		strings.HasSuffix(podName, "-go") || strings.Contains(podName, "golang"):
		return "go"
	}
	return ""
}
