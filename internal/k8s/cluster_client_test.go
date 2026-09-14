package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGuessGoTargetExeFromContainerName(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "reverse-proxy",
				Image: "maderovs/reverse-proxy:latest",
			}},
		},
	}

	if got := guessGoTargetExe(template); got != "/app/reverse-proxy" {
		t.Fatalf("guessGoTargetExe() = %q, want /app/reverse-proxy", got)
	}
}

func TestPatchPodTemplateSetsGoTargetAndCleansStaleAnnotations(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"instrumentation.opentelemetry.io/inject-python":           "old",
				"instrumentation.opentelemetry.io/otel-go-auto-target-exe": "/app",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "reverse-proxy",
				Image: "maderovs/reverse-proxy:latest",
			}},
		},
	}

	patchPodTemplate(template, "go", "highping-client-instrumentation", "http://agent:4317", "highping-client", "default", true)

	if got := template.Annotations["instrumentation.opentelemetry.io/inject-go"]; got != "highping-client-instrumentation" {
		t.Fatalf("inject-go = %q, want highping-client-instrumentation", got)
	}
	if _, ok := template.Annotations["instrumentation.opentelemetry.io/inject-python"]; ok {
		t.Fatalf("stale inject-python annotation was not removed")
	}
	if got := template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"]; got != "/app/reverse-proxy" {
		t.Fatalf("go target exe = %q, want /app/reverse-proxy", got)
	}
}

func TestPatchPodTemplatePHPUsesSDKAndAutoload(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "www",
				Image: "php:8.3-fpm",
			}},
		},
	}
	patchPodTemplate(template, "php", "ns-instrumentation", "http://agent:4317", "billing", "cluster-a", true)
	if got := template.Annotations["instrumentation.opentelemetry.io/inject-sdk"]; got != "ns-instrumentation" {
		t.Fatalf("inject-sdk = %q", got)
	}
	if _, ok := template.Annotations["instrumentation.opentelemetry.io/inject-php"]; ok {
		t.Fatal("operator has no inject-php; should not set it")
	}
	found := false
	for _, env := range template.Spec.Containers[0].Env {
		if env.Name == "OTEL_PHP_AUTOLOAD_ENABLED" && env.Value == "true" {
			found = true
		}
		if env.Name == "OTEL_EXPORTER_OTLP_PROTOCOL" && env.Value != "http/protobuf" {
			t.Fatalf("php protocol = %q", env.Value)
		}
	}
	if !found {
		t.Fatal("missing OTEL_PHP_AUTOLOAD_ENABLED")
	}
}
