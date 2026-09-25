package api

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/store"
)

func TestResolveApplicationLanguageUsesLastDetectedFallback(t *testing.T) {
	wLanguage := ""
	cfg := store.WorkloadInstrumentation{LastDetectedLanguage: "java", Language: ""}

	detected := wLanguage
	hasCfg := true
	if detected == "" && hasCfg && cfg.LastDetectedLanguage != "" {
		detected = cfg.LastDetectedLanguage
	}
	lang := detected
	if hasCfg && store.IsAssignedStack(cfg.Language) {
		lang = cfg.Language
	}
	if detected != "java" || lang != "java" {
		t.Fatalf("detected=%q lang=%q, want java for both", detected, lang)
	}
}

func TestResolveApplicationLanguageManualOverrideWins(t *testing.T) {
	wLanguage := ""
	cfg := store.WorkloadInstrumentation{LastDetectedLanguage: "java", Language: "nodejs", ManualOverride: true}

	detected := wLanguage
	hasCfg := true
	if detected == "" && hasCfg && cfg.LastDetectedLanguage != "" {
		detected = cfg.LastDetectedLanguage
	}
	lang := detected
	if hasCfg && store.IsAssignedStack(cfg.Language) {
		lang = cfg.Language
	}
	if detected != "java" {
		t.Fatalf("detected = %q, want java from last detected", detected)
	}
	if lang != "nodejs" {
		t.Fatalf("lang = %q, want nodejs manual override", lang)
	}
}
