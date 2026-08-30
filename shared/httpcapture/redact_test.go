package httpcapture

import "testing"

func TestBodyRedactsJSONSecrets(t *testing.T) {
	got := Body(`{"password":"hunter2","user":"ada"}`, 4096)
	if !containsAll(got, `"user":"ada"`, `[redacted]`) || containsAll(got, "hunter2") {
		t.Fatalf("json redact = %s", got)
	}
}

func TestBodyRedactsFormSecrets(t *testing.T) {
	got := Body("user=ada&token=abc&ok=1", 4096)
	if got != "user=ada&token=[redacted]&ok=1" {
		t.Fatalf("form redact = %s", got)
	}
}

func TestBodyTruncates(t *testing.T) {
	got := Body(`{"user":"ada","blob":"xxxxxxxxxxxxxxxxxxxx"}`, 12)
	if got == "" || len(got) < 12 || got[len(got)-12:] != "" && !hasTrunc(got) {
		if !hasTrunc(got) {
			t.Fatalf("expected truncation, got %q", got)
		}
	}
}

func TestSanitizeBodiesAliases(t *testing.T) {
	tags := map[string]string{"request.body": `{"password":"x"}`}
	SanitizeBodies(tags, 4096)
	if tags["http.request.body"] == "" {
		t.Fatal("expected canonical request body")
	}
	if tags["http.request.body"] == `{"password":"x"}` {
		t.Fatalf("secret survived: %s", tags["http.request.body"])
	}
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !contains(s, part) {
			return false
		}
	}
	return true
}

func contains(s, part string) bool {
	return len(part) == 0 || (len(s) >= len(part) && (s == part || len(s) > 0 && (func() bool {
		for i := 0; i+len(part) <= len(s); i++ {
			if s[i:i+len(part)] == part {
				return true
			}
		}
		return false
	})()))
}

func hasTrunc(s string) bool {
	return contains(s, "truncated")
}
