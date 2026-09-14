package license

import "testing"

func TestEndpointDefaultsToActivationCloudraft(t *testing.T) {
	t.Setenv("ACTIVATION_ENDPOINT", "")
	if got := Endpoint(); got != defaultActivationEndpoint {
		t.Fatalf("Endpoint() = %q, want %q", got, defaultActivationEndpoint)
	}
}

func TestEndpointHonorsOverride(t *testing.T) {
	t.Setenv("ACTIVATION_ENDPOINT", "https://activation-dev.cloudraft.net/")
	if got := Endpoint(); got != "https://activation-dev.cloudraft.net" {
		t.Fatalf("Endpoint() = %q", got)
	}
}
