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

func TestInjectAnnotationIsNotDetection(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"instrumentation.opentelemetry.io/inject-sdk":  "troni-dev-instrumentation",
				"instrumentation.opentelemetry.io/inject-java": "ns-instrumentation",
			},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "" {
		t.Fatalf("detectLanguageFromPodSpec() = %q, want empty (inject-* is not detection)", got)
	}
}

func TestOpaqueImageNotDetectedFromName(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-frontend-abc"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app-frontend",
				Image: "registry.ext.cloudraft.net:18001/development/code/4sim-website/app-frontend:dev-388",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "" {
		t.Fatalf("opaque private image must not invent a stack from the tag, got %q", got)
	}
}

func TestProcessCmdlineDetectsOpaqueNginx(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "registry.example/app-frontend:dev-1",
			}},
		},
	}
	cmdline := "nginx: master process nginx -g daemon off;"
	if got := detectLanguage(pod, cmdline); got != "nginx" {
		t.Fatalf("detectLanguage() = %q, want nginx", got)
	}
}

func TestProcessCmdlineDetectsOpaqueJava(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "registry.example/auth-backend:dev-1",
			}},
		},
	}
	if got := detectLanguage(pod, "java -Xms128m -Xmx512m -jar /app/app.jar"); got != "java" {
		t.Fatalf("detectLanguage() = %q, want java", got)
	}
}

func TestProcessCmdlineDetectsOpaquePython(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "registry.example/admin-backend:dev-1",
			}},
		},
	}
	if got := detectLanguage(pod, "/usr/local/bin/python3.12 /usr/local/bin/uvicorn app.main:app --host 0.0.0.0"); got != "python" {
		t.Fatalf("detectLanguage() = %q, want python", got)
	}
}

func TestProcessBeatsWrongInjectAnnotation(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"instrumentation.opentelemetry.io/inject-nodejs": "ns-instrumentation",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "registry.example/app-frontend:dev-1",
			}},
		},
	}
	if got := detectLanguage(pod, "nginx: master process nginx -g daemon off;"); got != "nginx" {
		t.Fatalf("detectLanguage() = %q, want nginx (process beats inject)", got)
	}
}

func TestNextServerCmdlineNodejs(t *testing.T) {
	if got := languageFromProcessCmdline("next-server (v16.2.4)"); got != "nodejs" {
		t.Fatalf("languageFromProcessCmdline() = %q, want nodejs", got)
	}
}

func TestLanguageFromProcessCmdline(t *testing.T) {
	cases := []struct {
		cmdline, want string
	}{
		{"nginx: master process nginx -g daemon off;", "nginx"},
		{"java -jar /app/app.jar", "java"},
		{"sh -c java $JAVA_OPTS -jar /app/app.jar", "java"},
		{"/usr/local/bin/python3.11 /usr/local/bin/gunicorn --bind 0.0.0.0:80 app:app", "python"},
		{"npm run start:prod", "nodejs"},
		{"next-server (v16.2.4)", "nodejs"},
		{"/venv/bin/uwsgi --ini /code/uwsgi.ini", "python"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := languageFromProcessCmdline(tc.cmdline); got != tc.want {
			t.Errorf("languageFromProcessCmdline(%q) = %q, want %q", tc.cmdline, got, tc.want)
		}
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

func TestPodNameIsNotDetection(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "java-worker-xyz"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image: "registry.example/custom:1",
			}},
		},
	}
	if got := detectLanguageFromPodSpec(pod); got != "" {
		t.Fatalf("pod name must not invent a stack, got %q", got)
	}
}
