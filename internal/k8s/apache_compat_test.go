package k8s

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestParseApacheVersion(t *testing.T) {
	if got := ParseApacheVersion("httpd:2.4.57"); got != "2.4" {
		t.Fatalf("got %q", got)
	}
}

func TestValidateApacheInjectBlocksUnknown(t *testing.T) {
	containers := []corev1.Container{{
		Image: "registry.example/custom:http",
	}}
	err := validateApacheInject(containers, "")
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("error = %v", err)
	}
}

func TestApacheInjectSupportedCrnetEnv(t *testing.T) {
	t.Setenv("OTEL_APACHE_IMAGE", "registry.example/instrumentation-nginx:prod")
	if !ApacheInjectSupported("2.4") {
		t.Fatal("expected 2.4 supported with CRNET agent")
	}
}
