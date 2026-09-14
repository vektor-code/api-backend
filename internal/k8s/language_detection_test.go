package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDetectLanguageFromTemurinImage(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "checkout-7d9f"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "eclipse-temurin:21-jre",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "java" {
		t.Fatalf("detectLanguageFromPodSpec() = %q, want java", got)
	}
}

func TestDetectNginxEvenWhenFrontend(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "nginx:1.27-alpine",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "nginx" {
		t.Fatalf("detectLanguageFromPodSpec() = %q, want nginx", got)
	}
}

func TestDetectApacheHttpdFromImage(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "httpd:2.4",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "apache-httpd" {
		t.Fatalf("detectLanguageFromPodSpec() = %q, want apache-httpd", got)
	}
}

func TestInjectSDKAnnotationDoesNotUseCRName(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"instrumentation.opentelemetry.io/inject-sdk": "troni-dev-instrumentation",
			},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "sdk" {
		t.Fatalf("detectLanguageFromPodSpec() = %q, want sdk", got)
	}
}

func TestDetectIgnoresIstioSidecar(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "istio-proxy", Image: "docker.io/istio/proxyv2:1.20.0"},
				{Name: "api", Image: "my.registry/payments:1.2.3", Command: []string{"java", "-jar", "/app.jar"}},
			},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "java" {
		t.Fatalf("got %q, want java (ignore istio)", got)
	}
}

func TestDetectPythonFromCommand(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "web",
				Image:   "ubuntu:22.04",
				Command: []string{"uvicorn", "main:app", "--host", "0.0.0.0"},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "python" {
		t.Fatalf("got %q, want python", got)
	}
}

func TestDetectNodeFromEnv(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "svc",
				Image: "ubuntu:22.04",
				Env:   []corev1.EnvVar{{Name: "NODE_ENV", Value: "production"}},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "nodejs" {
		t.Fatalf("got %q, want nodejs", got)
	}
}

func TestDetectShellWrappedJava(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image:   "custom/app:latest",
				Command: []string{"/bin/sh", "-c", "java -jar /opt/app.jar"},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "java" {
		t.Fatalf("got %q, want java", got)
	}
}

func TestDetectGoDistroless(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image:   "gcr.io/distroless/static:nonroot",
				Command: []string{"/payments"},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "go" {
		t.Fatalf("got %q, want go", got)
	}
}

func TestDetectLabelJavascriptNormalized(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app.kubernetes.io/language": "TypeScript"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Image: "custom/app:1"}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "nodejs" {
		t.Fatalf("got %q, want nodejs", got)
	}
}

func TestDetectDotnetImage(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "mcr.microsoft.com/dotnet/aspnet:8.0",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "dotnet" {
		t.Fatalf("got %q, want dotnet", got)
	}
}

func TestSPALabeledNodeButRunsNginx(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"language": "nodejs"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image:   "nginx:1.27-alpine",
				Command: []string{"nginx", "-g", "daemon off;"},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "nginx" {
		t.Fatalf("got %q, want nginx for SPA", got)
	}
}

func TestCommandBeatsWrongImage(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image:   "alpine:3.19",
				Command: []string{"node", "server.js"},
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "nodejs" {
		t.Fatalf("got %q, want nodejs", got)
	}
}
