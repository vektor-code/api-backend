package store

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestIsAssignedStack(t *testing.T) {
	if IsAssignedStack("") || IsAssignedStack("unknown") || IsAssignedStack("Auto") || IsAssignedStack("UNK") {
		t.Fatal("auto inject must not count as an assigned stack")
	}
	if !IsAssignedStack("nodejs") || !IsAssignedStack("Python") {
		t.Fatal("detected runtimes must count as assigned")
	}
}

func TestResolveServiceLanguageIgnoresAutoInjectUnknown(t *testing.T) {
	got := ResolveServiceLanguage("nodejs", "unknown", "java", "go")
	if got != "nodejs" {
		t.Fatalf("got %q, want nodejs from spans", got)
	}
	got = ResolveServiceLanguage("", "auto", "", "python")
	if got != "python" {
		t.Fatalf("got %q, want python from reported pods", got)
	}
	got = ResolveServiceLanguage("", "java", "python", "go")
	if got != "java" {
		t.Fatalf("got %q, want manual java override", got)
	}
	got = ResolveServiceLanguage("", "unknown", "unknown", "unknown")
	if got != "" {
		t.Fatalf("got %q, want empty so the UI does not render UNK", got)
	}
}

func TestDetectLanguageFromSpanIgnoresUnknownSDKTag(t *testing.T) {
	span := &models.Span{Attributes: map[string]string{
		"telemetry.sdk.language": "unknown",
		"process.runtime.name":   "nodejs",
	}}
	if got := DetectLanguageFromSpan(span); got != "nodejs" {
		t.Fatalf("got %q, want nodejs from process.runtime.name", got)
	}
}
