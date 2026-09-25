package api

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/store"
)

func TestMaskClusterInventoryItemAgentHasAccess(t *testing.T) {
	item := store.ClusterInventoryItem{
		ID:             "default",
		DisplayName:    "default",
		Token:          "",
		Status:         "Active",
		CredentialType: "agent",
		AgentNamespace: "crnet-apm-dev",
		ManagedByAgent: true,
	}
	got := maskClusterInventoryItem(item)
	if got["hasCredentials"] != true {
		t.Fatalf("hasCredentials = %v, want true for agent-managed cluster", got["hasCredentials"])
	}
	if got["accessMode"] != "agent" {
		t.Fatalf("accessMode = %v, want agent", got["accessMode"])
	}
	if got["managedByAgent"] != true {
		t.Fatalf("managedByAgent = %v, want true", got["managedByAgent"])
	}
}

func TestMaskClusterInventoryItemManualMissingCreds(t *testing.T) {
	item := store.ClusterInventoryItem{
		ID:     "remote",
		Token:  "",
		Status: "Inactive",
	}
	got := maskClusterInventoryItem(item)
	if got["hasCredentials"] != false {
		t.Fatalf("hasCredentials = %v, want false", got["hasCredentials"])
	}
	if got["accessMode"] != "none" {
		t.Fatalf("accessMode = %v, want none", got["accessMode"])
	}
}
