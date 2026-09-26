package k8s

import (
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// nginxAgentRelease maps an autoinstrumentation-apache-httpd (or CRNET instrumentation-nginx)
// image tag to ngx_http_opentelemetry_module.so builds under
// /opt/opentelemetry/WebServerModule/Nginx/{version}/.
//
// Upstream OTel does NOT publish “newer inject for newer nginx” on demand —
// each agent image is a fixed set of prebuilt modules. Verified for :1.0.4
// (2026-09): only 1.24.0 and 1.25.3. CRNET ships crnet-1.1.0 with the expanded
// matrix in nginx-agent/version.properties (includes highping 1.31.4).
//
// Best options when the app nginx is ahead of the agent image:
//  1. Rebuild the app on a supported nginx (e.g. 1.25.3) — safest.
//  2. Point OTEL_NGINX_IMAGE at CRNET instrumentation-nginx (or a custom build).
//  3. Leave inject off (HTTP tracing via upstream services still works).
//
// Alpine/musl nginx cannot load these glibc-built modules.
type nginxAgentRelease struct {
	Tag     string
	Modules []string
}

// crnetNginxModuleVersions must stay in sync with nginx-agent/version.properties.
var crnetNginxModuleVersions = []string{
	"1.24.0", "1.25.3", "1.25.5", "1.26.0", "1.26.2",
	"1.27.3", "1.27.4", "1.28.0", "1.29.0", "1.30.0", "1.31.0", "1.31.4",
}

var knownNginxAgentReleases = []nginxAgentRelease{
	{Tag: "1.0.4", Modules: []string{"1.24.0", "1.25.3"}},
	{Tag: "crnet-1.1.0", Modules: crnetNginxModuleVersions},
}

// SupportedNginxModuleVersions is the module set for OTEL_NGINX_IMAGE at process start.
var SupportedNginxModuleVersions = modulesForConfiguredNginxAgent()

var (
	nginxSemverRE    = regexp.MustCompile(`(\d+\.\d+\.\d+)`)
	nginxTagSemverRE = regexp.MustCompile(`^(\d+\.\d+\.\d+)`)
)

func configuredNginxAgentImage() string {
	return envOr("OTEL_NGINX_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-apache-httpd:1.0.4")
}

func configuredApacheAgentImage() string {
	return envOr("OTEL_APACHE_IMAGE", configuredNginxAgentImage())
}

func isCrnetWebserverAgentImage(img string) bool {
	// Only tags that advertise the fat matrix (crnet-1.1.0). Smoke builds of
	// instrumentation-nginx:dev still only ship upstream 1.24.0/1.25.3 — do not
	// claim 1.31.4 support for those.
	tag := strings.ToLower(imageTag(img))
	return strings.HasPrefix(tag, "crnet-") || tag == "crnet-1.1.0"
}

func modulesFromEnvOverride() []string {
	raw := strings.TrimSpace(envOr("OTEL_NGINX_SUPPORTED_MODULES", ""))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ModulesForNginxAgentImage resolves supported nginx module semvers for an agent image ref.
func ModulesForNginxAgentImage(img string) []string {
	if fromEnv := modulesFromEnvOverride(); len(fromEnv) > 0 {
		return fromEnv
	}
	tag := imageTag(img)
	for _, rel := range knownNginxAgentReleases {
		if rel.Tag == tag {
			out := make([]string, len(rel.Modules))
			copy(out, rel.Modules)
			return out
		}
	}
	if isCrnetWebserverAgentImage(img) {
		out := make([]string, len(crnetNginxModuleVersions))
		copy(out, crnetNginxModuleVersions)
		return out
	}
	return []string{"1.24.0", "1.25.3"}
}

func modulesForConfiguredNginxAgent() []string {
	return ModulesForNginxAgentImage(configuredNginxAgentImage())
}

func supportedNginxModuleVersions() []string {
	return modulesForConfiguredNginxAgent()
}

func imageTag(image string) string {
	s := image
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[:i]
	}
	colon := strings.LastIndex(s, ":")
	if colon < 0 {
		return ""
	}
	tag := s[colon+1:]
	if strings.Contains(tag, "/") {
		return ""
	}
	return tag
}

// RecommendNginxAgentImage returns an agent image that ships a module for
// nginxVersion, if any known release does. Empty ok=false means upstream has
// no published image for that nginx — do not invent a tag.
func RecommendNginxAgentImage(nginxVersion string) (image string, ok bool) {
	v := strings.TrimSpace(nginxVersion)
	if v == "" {
		return "", false
	}
	if isCrnetWebserverAgentImage(configuredNginxAgentImage()) {
		for _, m := range crnetNginxModuleVersions {
			if m == v {
				return configuredNginxAgentImage(), true
			}
		}
	}
	base := "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-apache-httpd"
	for _, rel := range knownNginxAgentReleases {
		for _, m := range rel.Modules {
			if m == v {
				return base + ":" + rel.Tag, true
			}
		}
	}
	return "", false
}

// ParseNginxVersion extracts an nginx semver from image tags, env values, or cmdline / nginx -v output.
func ParseNginxVersion(imageOrCmdline string) string {
	s := strings.TrimSpace(imageOrCmdline)
	if s == "" {
		return ""
	}
	lower := strings.ToLower(s)
	if strings.Contains(lower, "nginx version") || strings.Contains(lower, "nginx/") {
		if v := nginxSemverRE.FindString(s); v != "" {
			return v
		}
	}
	if !strings.Contains(s, "/") && !strings.Contains(s, " ") && !strings.Contains(s, ":") {
		if v := extractLeadingSemver(s); v != "" {
			return v
		}
	}
	if isNginxImageRef(lower) {
		return nginxVersionFromImageTag(s)
	}
	return ""
}

func extractLeadingSemver(s string) string {
	m := nginxTagSemverRE.FindStringSubmatch(s)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

func isNginxImageRef(lowerImage string) bool {
	return strings.Contains(lowerImage, "nginx") || strings.Contains(lowerImage, "openresty")
}

func nginxVersionFromImageTag(image string) string {
	repo := image
	if i := strings.LastIndex(repo, "@"); i >= 0 {
		repo = repo[:i]
	}
	colon := strings.LastIndex(repo, ":")
	if colon < 0 {
		return ""
	}
	tag := repo[colon+1:]
	if strings.Contains(tag, "/") {
		return ""
	}
	return extractLeadingSemver(tag)
}

func isAlpineMuslNginx(containers []corev1.Container) bool {
	for _, c := range containers {
		lower := strings.ToLower(c.Image)
		if !isNginxImageRef(lower) {
			continue
		}
		if strings.Contains(lower, "alpine") || strings.Contains(lower, "-musl") || strings.Contains(lower, "musl") {
			return true
		}
	}
	return false
}

func formatSupportedNginxModules(modules []string) string {
	return "Supported nginx modules: " + strings.Join(modules, ", ")
}

// NginxInjectSupported reports whether OTel nginx inject is safe for the given version.
func NginxInjectSupported(version string) bool {
	version = strings.TrimSpace(version)
	if version == "" {
		return false
	}
	for _, supported := range supportedNginxModuleVersions() {
		if version == supported {
			return true
		}
	}
	return false
}

// NginxInjectBlockedReason explains why inject must not be enabled and what to do instead.
func NginxInjectBlockedReason(version string) string {
	modules := supportedNginxModuleVersions()
	supported := strings.Join(modules, ", ")
	agent := configuredNginxAgentImage()
	supportedLine := formatSupportedNginxModules(modules)
	if strings.TrimSpace(version) == "" {
		return fmt.Sprintf(
			"nginx version unknown; agent image %s only has modules for %s — probe a Ready pod, pin nginx:%s, or set OTEL_NGINX_IMAGE to CRNET instrumentation-nginx. %s",
			agent, supported, modules[len(modules)-1], supportedLine,
		)
	}
	if img, ok := RecommendNginxAgentImage(version); ok && img != agent {
		return fmt.Sprintf(
			"nginx %s needs agent image %s (current %s) — set OTEL_NGINX_IMAGE or rebuild Instrumentation. %s",
			version, img, agent, supportedLine,
		)
	}
	return fmt.Sprintf(
		"nginx %s has no OTel module in agent %s (%s). Rebuild on nginx:%s, extend instrumentation-nginx, or leave inject off. %s",
		version, agent, supported, modules[len(modules)-1], supportedLine,
	)
}

func alpineNginxBlockedReason() string {
	return "Alpine/musl nginx cannot load glibc-built OTel modules (ngx_http_opentelemetry_module.so). Use a Debian/RHEL-based nginx image or leave inject off. " +
		formatSupportedNginxModules(supportedNginxModuleVersions())
}

func resolveNginxVersionFromContainers(containers []corev1.Container) string {
	for _, c := range containers {
		for _, e := range c.Env {
			if strings.EqualFold(e.Name, "NGINX_VERSION") {
				if v := ParseNginxVersion(e.Value); v != "" {
					return v
				}
			}
		}
	}
	for _, c := range containers {
		if v := ParseNginxVersion(c.Image); v != "" {
			return v
		}
	}
	for _, c := range containers {
		parts := append(append([]string{}, c.Command...), c.Args...)
		if v := ParseNginxVersion(strings.Join(parts, " ")); v != "" {
			return v
		}
	}
	return ""
}

// NginxInjectStatus resolves nginx version and reports inject compatibility.
func NginxInjectStatus(containers []corev1.Container, probedVersion string) (version string, compatible bool, blockedReason string) {
	if isAlpineMuslNginx(containers) {
		version = resolveNginxVersionFromContainers(containers)
		if version == "" {
			version = strings.TrimSpace(probedVersion)
		}
		return version, false, alpineNginxBlockedReason()
	}
	version = resolveNginxVersionFromContainers(containers)
	if version == "" {
		version = strings.TrimSpace(probedVersion)
	}
	compatible = NginxInjectSupported(version)
	if !compatible {
		blockedReason = NginxInjectBlockedReason(version)
	}
	return version, compatible, blockedReason
}

func validateNginxInject(containers []corev1.Container, probedVersion string) error {
	_, compatible, blockedReason := NginxInjectStatus(containers, probedVersion)
	if compatible {
		return nil
	}
	return fmt.Errorf("%s", blockedReason)
}

func hasNginxInjectAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	_, ok := annotations["instrumentation.opentelemetry.io/inject-nginx"]
	return ok
}
