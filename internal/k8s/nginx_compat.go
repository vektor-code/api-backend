package k8s

import (
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// nginxAgentRelease maps an autoinstrumentation-apache-httpd image tag to the
// ngx_http_opentelemetry_module.so builds it ships under
// /opt/opentelemetry/WebServerModule/Nginx/{version}/.
//
// Upstream OTel does NOT publish “newer inject for newer nginx” on demand —
// each agent image is a fixed set of prebuilt modules. Verified for :1.0.4
// (2026-09): only 1.24.0 and 1.25.3. Official docs historically listed even
// fewer. When a workload runs nginx 1.31.x the operator still requests
// Nginx/1.31.4/... and dlopen fails → CrashLoop (highping incident).
//
// Best options when the app nginx is ahead of the agent image:
//  1. Rebuild the app on a supported nginx (e.g. 1.25.3) — safest.
//  2. Point OTEL_NGINX_IMAGE at a custom image that includes the needed module.
//  3. Leave inject off (HTTP tracing via upstream services still works).
type nginxAgentRelease struct {
	Tag     string
	Modules []string
}

var knownNginxAgentReleases = []nginxAgentRelease{
	{Tag: "1.0.4", Modules: []string{"1.24.0", "1.25.3"}},
}

// SupportedNginxModuleVersions is the module set for the currently configured
// agent image (OTEL_NGINX_IMAGE), defaulting to 1.0.4.
var SupportedNginxModuleVersions = modulesForConfiguredNginxAgent()

var (
	nginxSemverRE    = regexp.MustCompile(`(\d+\.\d+\.\d+)`)
	nginxTagSemverRE = regexp.MustCompile(`^(\d+\.\d+\.\d+)`)
)

func configuredNginxAgentImage() string {
	return envOr("OTEL_NGINX_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-apache-httpd:1.0.4")
}

func modulesForConfiguredNginxAgent() []string {
	img := configuredNginxAgentImage()
	tag := imageTag(img)
	for _, rel := range knownNginxAgentReleases {
		if rel.Tag == tag {
			out := make([]string, len(rel.Modules))
			copy(out, rel.Modules)
			return out
		}
	}
	// Unknown custom image: keep the conservative 1.0.4 set so we never
	// assume a private build has every module.
	return []string{"1.24.0", "1.25.3"}
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

// NginxInjectSupported reports whether OTel nginx inject is safe for the given version.
func NginxInjectSupported(version string) bool {
	version = strings.TrimSpace(version)
	if version == "" {
		return false
	}
	for _, supported := range SupportedNginxModuleVersions {
		if version == supported {
			return true
		}
	}
	return false
}

// NginxInjectBlockedReason explains why inject must not be enabled and what to do instead.
func NginxInjectBlockedReason(version string) string {
	supported := strings.Join(SupportedNginxModuleVersions, ", ")
	agent := configuredNginxAgentImage()
	if strings.TrimSpace(version) == "" {
		return fmt.Sprintf(
			"nginx version unknown; agent image %s only has modules for %s — probe a Ready pod, pin nginx:%s, or set OTEL_NGINX_IMAGE to a custom build that includes your module",
			agent, supported, SupportedNginxModuleVersions[len(SupportedNginxModuleVersions)-1],
		)
	}
	if img, ok := RecommendNginxAgentImage(version); ok {
		return fmt.Sprintf(
			"nginx %s needs agent image %s (current %s) — set OTEL_NGINX_IMAGE or rebuild Instrumentation",
			version, img, agent,
		)
	}
	return fmt.Sprintf(
		"nginx %s has no published OTel module (agent %s ships %s only). Options: rebuild app on nginx:%s, supply a custom OTEL_NGINX_IMAGE with Nginx/%s/ngx_http_opentelemetry_module.so, or leave inject off",
		version, agent, supported, SupportedNginxModuleVersions[len(SupportedNginxModuleVersions)-1], version,
	)
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
