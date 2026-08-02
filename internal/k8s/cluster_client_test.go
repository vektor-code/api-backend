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

	if got := guessGoTargetExe(template); got != "/reverse-proxy" {
		t.Fatalf("guessGoTargetExe() = %q, want /reverse-proxy", got)
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
	if got := template.Annotations["instrumentation.opentelemetry.io/otel-go-auto-target-exe"]; got != "/reverse-proxy" {
		t.Fatalf("go target exe = %q, want /reverse-proxy", got)
	}
}
