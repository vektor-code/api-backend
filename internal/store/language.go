package store

import "strings"

// IsAssignedStack reports whether language is a real runtime (java, go, …)
// rather than Admin Auto. Auto-inject persists as "unknown"; that must not
// replace a language already detected from traces or the pod.
func IsAssignedStack(language string) bool {
	key := strings.ToLower(strings.TrimSpace(language))
	return key != "" && key != "unknown" && key != "auto" && key != "unk"
}

// ResolveServiceLanguage prefers a manual Admin stack, then span telemetry,
// then Kubernetes/image detection, then the agent's reported pod language.
// Auto/unknown is never treated as an assignment.
func ResolveServiceLanguage(fromSpans, assigned, fromK8s, fromPods string) string {
	if IsAssignedStack(assigned) {
		return cleanLanguage(assigned)
	}
	for _, candidate := range []string{fromSpans, fromK8s, fromPods} {
		if IsAssignedStack(candidate) {
			return cleanLanguage(candidate)
		}
	}
	return ""
}
