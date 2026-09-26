package k8s

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestParseNginxVersionFromImageTags(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"nginx:1.31.4", "1.31.4"},
		{"nginx:1.31.4-alpine", "1.31.4"},
		{"nginx:1.25.3", "1.25.3"},
		{"docker.io/bitnami/nginx:1.24.0", "1.24.0"},
		{"registry.example/highping/app-frontend:dev-1286", ""},
		{"nginx:alpine", ""},
	}
	for _, tc := range tests {
		if got := ParseNginxVersion(tc.in); got != tc.want {
			t.Fatalf("ParseNginxVersion(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseNginxVersionFromCmdlineAndEnv(t *testing.T) {
	if got := ParseNginxVersion("nginx version: nginx/1.31.4"); got != "1.31.4" {
		t.Fatalf("cmdline parse = %q", got)
	}
	if got := ParseNginxVersion("1.25.3"); got != "1.25.3" {
		t.Fatalf("env parse = %q", got)
	}
}

func TestNginxInjectSupportedDefaultAgent(t *testing.T) {
	if !NginxInjectSupported("1.24.0") || !NginxInjectSupported("1.25.3") {
		t.Fatal("expected supported nginx versions to pass")
	}
	for _, v := range []string{"", "1.31.4", "1.27.0", "latest"} {
		if NginxInjectSupported(v) {
			t.Fatalf("NginxInjectSupported(%q) should be false", v)
		}
	}
}

func TestModulesForNginxAgentImageCrnet(t *testing.T) {
	img := "git.cloudraft.net:5050/devops/images/crnet-apm/instrumentation-nginx:crnet-1.1.0"
	modules := ModulesForNginxAgentImage(img)
	if len(modules) != len(crnetNginxModuleVersions) {
		t.Fatalf("got %d modules, want %d", len(modules), len(crnetNginxModuleVersions))
	}
}

func TestNginxInjectSupportedCrnetEnv(t *testing.T) {
	t.Setenv("OTEL_NGINX_IMAGE", "registry.example/instrumentation-nginx:crnet-1.1.0")
	if !NginxInjectSupported("1.31.4") {
		t.Fatal("expected 1.31.4 with CRNET fat tag crnet-1.1.0")
	}
	t.Setenv("OTEL_NGINX_IMAGE", "registry.example/instrumentation-nginx:dev")
	if NginxInjectSupported("1.31.4") {
		t.Fatal("smoke :dev must not claim 1.31.4")
	}
}

func TestNginxInjectBlockedReason(t *testing.T) {
	unknown := NginxInjectBlockedReason("")
	if !strings.Contains(unknown, "unknown") || !strings.Contains(unknown, "Supported nginx modules") {
		t.Fatalf("unknown reason = %q", unknown)
	}
	bad := NginxInjectBlockedReason("1.31.4")
	if !strings.Contains(bad, "1.31.4") {
		t.Fatalf("unsupported reason = %q", bad)
	}
}

func TestResolveNginxVersionFromContainers(t *testing.T) {
	containers := []corev1.Container{{
		Name:  "www",
		Image: "registry.example/app-frontend:dev",
		Env:   []corev1.EnvVar{{Name: "NGINX_VERSION", Value: "1.25.3"}},
	}}
	version, compatible, reason := NginxInjectStatus(containers, "")
	if version != "1.25.3" || !compatible || reason != "" {
		t.Fatalf("got version=%q compatible=%v reason=%q", version, compatible, reason)
	}
}

func TestValidateNginxInjectBlocksUnsupported(t *testing.T) {
	containers := []corev1.Container{{
		Image: "nginx:1.31.4",
	}}
	err := validateNginxInject(containers, "")
	if err == nil {
		t.Fatal("expected error for nginx 1.31.4 with default agent")
	}
	if !strings.Contains(err.Error(), "1.31.4") {
		t.Fatalf("error = %v", err)
	}
}
