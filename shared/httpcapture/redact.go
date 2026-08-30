package httpcapture

import (
	"encoding/json"
	"regexp"
	"strings"
)

const DefaultMaxBytes = 4096

var secretKey = regexp.MustCompile(`(?i)password|passwd|pwd|secret|token|authorization|cookie|set-cookie|api[_-]?key|access[_-]?token|refresh[_-]?token|private[_-]?key|ssn`)

var bodyKeys = []string{
	"http.request.body", "request.body", "http.request_body",
	"http.response.body", "response.body", "http.response_body",
}

// SanitizeBodies copies alias keys onto the canonical http.*.body attributes
// and redacts secrets. Safe to run on every span.
func SanitizeBodies(tags map[string]string, max int) {
	if tags == nil {
		return
	}
	if max <= 0 {
		max = DefaultMaxBytes
	}
	copyFirst(tags, "http.request.body", "request.body", "http.request_body")
	copyFirst(tags, "http.response.body", "response.body", "http.response_body")
	for _, key := range []string{"http.request.body", "http.response.body"} {
		if raw := strings.TrimSpace(tags[key]); raw != "" {
			tags[key] = Body(raw, max)
		}
	}
}

func copyFirst(tags map[string]string, canonical string, aliases ...string) {
	if strings.TrimSpace(tags[canonical]) != "" {
		return
	}
	for _, alias := range aliases {
		if v := strings.TrimSpace(tags[alias]); v != "" {
			tags[canonical] = v
			return
		}
	}
}

func Body(raw string, max int) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}
	if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
		var value any
		if json.Unmarshal([]byte(text), &value) == nil {
			redactJSON(value)
			if encoded, err := json.Marshal(value); err == nil {
				text = string(encoded)
			}
		}
	} else if strings.Contains(text, "=") {
		parts := strings.Split(text, "&")
		for i, part := range parts {
			key, rest, ok := strings.Cut(part, "=")
			if ok && secretKey.MatchString(key) {
				parts[i] = key + "=[redacted]"
			} else {
				parts[i] = part
				_ = rest
			}
		}
		text = strings.Join(parts, "&")
	}
	if max > 0 && len(text) > max {
		return text[:max] + "…[truncated]"
	}
	return text
}

func redactJSON(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for k, v := range typed {
			if secretKey.MatchString(k) {
				typed[k] = "[redacted]"
				continue
			}
			redactJSON(v)
		}
	case []any:
		for _, item := range typed {
			redactJSON(item)
		}
	}
}
