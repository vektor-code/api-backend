package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDetectInstrumentationFromJavaAgentEnv(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Env: []corev1.EnvVar{{
					Name:  "JAVA_TOOL_OPTIONS",
					Value: "-javaagent:/otel/opentelemetry-javaagent.jar",
				}},
			}},
		},
	}
	ok, kind, _ := detectInstrumentation(pod)
	if !ok || kind != "env" {
		t.Fatalf("java agent env: ok=%v kind=%q", ok, kind)
	}
}

func TestDetectInstrumentationFromOTLPEndpoint(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Env: []corev1.EnvVar{{
					Name:  "OTEL_EXPORTER_OTLP_ENDPOINT",
					Value: "http://agent-backend.crnet-apm-dev.svc:4318",
				}},
			}},
		},
	}
	ok, _, _ := detectInstrumentation(pod)
	if !ok {
		t.Fatal("OTLP endpoint should count as instrumented")
	}
}

func TestDetectInstrumentationIgnoresDisabledInject(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"instrumentation.opentelemetry.io/inject-java": "false",
			},
		},
	}
	ok, _, _ := detectInstrumentation(pod)
	if ok {
		t.Fatal("inject-*=false should not count as instrumented")
	}
}
