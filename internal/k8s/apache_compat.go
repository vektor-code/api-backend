package k8s

import (
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// apacheAgentRelease maps a webserver agent image tag to supported Apache httpd
// major.minor lines (operator picks libmod_apache_otel.so for 2.4, *_otel22.so for 2.2).
type apacheAgentRelease struct {
	Tag     string
	Modules []string
}

var crnetApacheModuleVersions = []string{"2.4", "2.2"}

var knownApacheAgentReleases = []apacheAgentRelease{
	{Tag: "1.0.4", Modules: []string{"2.4", "2.2"}},
	{Tag: "crnet-1.1.0", Modules: crnetApacheModuleVersions},
}

var (
	apacheSemverRE    = regexp.MustCompile(`(?i)(?:apache|httpd)[/\s]+(\d+\.\d+(?:\.\d+)?)`)
	apacheTagVersionRE = regexp.MustCompile(`^(\d+\.\d+(?:\.\d+)?)`)
)

func modulesForConfiguredApacheAgent() []string {
	return ModulesForApacheAgentImage(configuredApacheAgentImage())
}

// ModulesForApacheAgentImage resolves supported httpd major.minor modules for an agent image.
func ModulesForApacheAgentImage(img string) []string {
	tag := imageTag(img)
	for _, rel := range knownApacheAgentReleases {
		if rel.Tag == tag {
			out := make([]string, len(rel.Modules))
			copy(out, rel.Modules)
			return out
		}
	}
	if isCrnetWebserverAgentImage(img) {
		out := make([]string, len(crnetApacheModuleVersions))
		copy(out, crnetApacheModuleVersions)
		return out
	}
	return []string{"2.4", "2.2"}
}

func normalizeApacheMajorMinor(version string) string {
	v := strings.TrimSpace(version)
	if v == "" {
		return ""
	}
	parts := strings.Split(v, ".")
	if len(parts) >= 2 {
		return parts[0] + "." + parts[1]
	}
	return v
}

// ParseApacheVersion extracts httpd major.minor from image tags, env, or httpd -v output.
func ParseApacheVersion(imageOrCmdline string) string {
	s := strings.TrimSpace(imageOrCmdline)
	if s == "" {
		return ""
	}
	if m := apacheSemverRE.FindStringSubmatch(s); len(m) >= 2 {
		return normalizeApacheMajorMinor(m[1])
	}
	lower := strings.ToLower(s)
	if isApacheImageRef(lower) {
		repo := s
		if i := strings.LastIndex(repo, "@"); i >= 0 {
			repo = repo[:i]
		}
		colon := strings.LastIndex(repo, ":")
		if colon >= 0 {
			tag := repo[colon+1:]
			if !strings.Contains(tag, "/") {
				if v := apacheTagVersionRE.FindStringSubmatch(tag); len(v) >= 2 {
					return normalizeApacheMajorMinor(v[1])
				}
			}
		}
	}
	if !strings.Contains(s, "/") && !strings.Contains(s, " ") && !strings.Contains(s, ":") {
		if v := apacheTagVersionRE.FindStringSubmatch(s); len(v) >= 2 {
			return normalizeApacheMajorMinor(v[1])
		}
	}
	return ""
}

func isApacheImageRef(lowerImage string) bool {
	return strings.Contains(lowerImage, "httpd") ||
		strings.Contains(lowerImage, "apache") ||
		strings.Contains(lowerImage, "bitnami/apache")
}

func isAlpineMuslApache(containers []corev1.Container) bool {
	for _, c := range containers {
		lower := strings.ToLower(c.Image)
		if !isApacheImageRef(lower) {
			continue
		}
		if strings.Contains(lower, "alpine") || strings.Contains(lower, "-musl") || strings.Contains(lower, "musl") {
			return true
		}
	}
	return false
}

func formatSupportedApacheModules(modules []string) string {
	return "Supported Apache modules: " + strings.Join(modules, ", ")
}

// ApacheInjectSupported reports whether OTel apache-httpd inject is safe for the version.
func ApacheInjectSupported(version string) bool {
	version = normalizeApacheMajorMinor(version)
	if version == "" {
		return false
	}
	for _, supported := range modulesForConfiguredApacheAgent() {
		if version == supported {
			return true
		}
	}
	return false
}

// ApacheInjectBlockedReason explains why apache inject must not be enabled.
func ApacheInjectBlockedReason(version string) string {
	modules := modulesForConfiguredApacheAgent()
	supported := strings.Join(modules, ", ")
	agent := configuredApacheAgentImage()
	supportedLine := formatSupportedApacheModules(modules)
	if strings.TrimSpace(version) == "" {
		return fmt.Sprintf(
			"Apache httpd version unknown; agent image %s supports %s — probe a Ready pod, pin httpd:2.4, or set OTEL_APACHE_IMAGE to CRNET instrumentation-nginx. %s",
			agent, supported, supportedLine,
		)
	}
	return fmt.Sprintf(
		"Apache httpd %s is not supported by agent %s (modules: %s). Use httpd 2.4.x or 2.2.x on glibc, or leave inject off. %s",
		version, agent, supported, supportedLine,
	)
}

func alpineApacheBlockedReason() string {
	return "Alpine/musl httpd cannot load glibc-built OTel Apache modules. Use a Debian/RHEL-based httpd image or leave inject off. " +
		formatSupportedApacheModules(modulesForConfiguredApacheAgent())
}

func resolveApacheVersionFromContainers(containers []corev1.Container) string {
	for _, c := range containers {
		for _, e := range c.Env {
			if strings.EqualFold(e.Name, "HTTPD_VERSION") || strings.EqualFold(e.Name, "APACHE_VERSION") {
				if v := ParseApacheVersion(e.Value); v != "" {
					return v
				}
			}
		}
	}
	for _, c := range containers {
		if v := ParseApacheVersion(c.Image); v != "" {
			return v
		}
	}
	for _, c := range containers {
		parts := append(append([]string{}, c.Command...), c.Args...)
		if v := ParseApacheVersion(strings.Join(parts, " ")); v != "" {
			return v
		}
	}
	return ""
}

// ApacheInjectStatus resolves httpd version and reports inject compatibility.
func ApacheInjectStatus(containers []corev1.Container, probedVersion string) (version string, compatible bool, blockedReason string) {
	if isAlpineMuslApache(containers) {
		version = resolveApacheVersionFromContainers(containers)
		if version == "" {
			version = normalizeApacheMajorMinor(probedVersion)
		}
		return version, false, alpineApacheBlockedReason()
	}
	version = resolveApacheVersionFromContainers(containers)
	if version == "" {
		version = normalizeApacheMajorMinor(probedVersion)
	}
	compatible = ApacheInjectSupported(version)
	if !compatible {
		blockedReason = ApacheInjectBlockedReason(version)
	}
	return version, compatible, blockedReason
}

func validateApacheInject(containers []corev1.Container, probedVersion string) error {
	_, compatible, blockedReason := ApacheInjectStatus(containers, probedVersion)
	if compatible {
		return nil
	}
	return fmt.Errorf("%s", blockedReason)
}

func hasApacheInjectAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	_, ok := annotations["instrumentation.opentelemetry.io/inject-apache-httpd"]
	return ok
}
