package k8s

import "testing"

func TestAssignedNginxKept(t *testing.T) {
	if got := resolveInject("nginx", []string{"registry/shop:1"}, nil); got != "nginx" {
		t.Fatalf("got %s", got)
	}
}

func TestNodeLabelOnNginxProcess(t *testing.T) {
	got := resolveInject("nodejs", []string{"nginx:1.27-alpine"}, []string{"nginx"})
	if got != "nginx" {
		t.Fatalf("got %s, want nginx", got)
	}
}

func TestJavaUnchanged(t *testing.T) {
	if got := resolveInject("java", []string{"eclipse-temurin:21-jre"}, nil); got != "java" {
		t.Fatalf("got %s", got)
	}
}

func TestNodeSSRKept(t *testing.T) {
	got := resolveInject("nodejs", []string{"node:20-alpine"}, []string{"node", "server.js"})
	if got != "nodejs" {
		t.Fatalf("got %s", got)
	}
}

func TestLanguageFromLabels(t *testing.T) {
	got := languageFromLabels(map[string]string{"app.kubernetes.io/language": "javascript"})
	if got != "nodejs" {
		t.Fatalf("got %s, want nodejs", got)
	}
}
