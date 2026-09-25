package k8s

import (
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// SupportedNginxModuleVersions lists nginx versions whose ngx_http_opentelemetry_module.so
// ships in ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-apache-httpd:1.0.4.
// The agent image mounts modules at .../Nginx/{version}/ngx_http_opentelemetry_module.so;
// other versions fail dlopen() and CrashLoopBackOff (e.g. highping app-frontend on nginx 1.31.4).
var SupportedNginxModuleVersions = []string{"1.24.0", "1.25.3"}

var (
	nginxSemverRE      = regexp.MustCompile(`(\d+\.\d+\.\d+)`)
	nginxTagSemverRE   = regexp.MustCompile(`^(\d+\.\d+\.\d+)`)
)

// ParseNginxVersion extracts an nginx semver from image tags, env values, or cmdline / nginx -v output.
// Returns "" when the version cannot be determined.
func ParseNginxVersion(imageOrCmdline string) string {
	s := strings.TrimSpace(imageOrCmdline)
	if s == "" {
		return ""
	}

	lower := strings.ToLower(s)

	// nginx -v writes to stderr: "nginx version: nginx/1.31.4"
	if strings.Contains(lower, "nginx version") || strings.Contains(lower, "nginx/") {
		if v := nginxSemverRE.FindString(s); v != "" {
			return v
		}
	}

	// Bare semver (e.g. NGINX_VERSION=1.31.4).
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
// Empty/unknown versions return false so enable paths stay conservative.
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

// NginxInjectBlockedReason returns a human-readable explanation when inject must not be enabled.
func NginxInjectBlockedReason(version string) string {
	supported := strings.Join(SupportedNginxModuleVersions, ", ")
	if strings.TrimSpace(version) == "" {
		return fmt.Sprintf(
			"nginx version unknown; OTel inject module only supports %s — pin a supported nginx image tag or pick the stack manually after verifying",
			supported,
		)
	}
	return fmt.Sprintf(
		"nginx %s is not supported by OTel inject (modules only built for %s); rebuild on nginx 1.25.3 or disable inject",
		version,
		supported,
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

// NginxInjectStatus resolves nginx version from the pod spec (and optional probe result)
// and reports whether OTel nginx inject is compatible.
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
