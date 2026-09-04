package collector

import "testing"

func TestResolveServiceNamePrefersWorkloadOverUnknown(t *testing.T) {
	got := resolveServiceName(map[string]string{
		"service.name":        "unknown_service:java",
		"k8s.deployment.name": "api-backend",
		"k8s.container.name":  "app",
	})
	if got != "api-backend" {
		t.Fatalf("got %q, want api-backend", got)
	}
}

func TestResolveServiceNameKeepsRealName(t *testing.T) {
	got := resolveServiceName(map[string]string{
		"service.name":        "payments",
		"k8s.deployment.name": "payments-v2",
	})
	if got != "payments" {
		t.Fatalf("got %q, want payments", got)
	}
}
