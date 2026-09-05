package probe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSupported(t *testing.T) {
	for _, tool := range []string{"kafka", "clickhouse", "minio", "ldap", "prometheus", "elasticsearch"} {
		if !Supported(tool) {
			t.Fatalf("expected %s to be supported", tool)
		}
	}
	if Supported("telegram") {
		t.Fatal("telegram is not an infrastructure probe")
	}
}

func TestRunUnsupported(t *testing.T) {
	result := Run(context.Background(), "unknown", nil)
	if result.OK || result.Message != "unsupported tool" {
		t.Fatalf("got %+v", result)
	}
}

func TestMergeMaskedSecrets(t *testing.T) {
	stored := Settings{"host": "ch.internal", "password": "s3cret"}
	incoming := Settings{"host": "ch.internal", "password": "******", "database": "traces"}
	got := Merge(incoming, stored, []string{"password"})
	if got["password"] != "s3cret" {
		t.Fatalf("password = %q, want stored secret", got["password"])
	}
	if got["database"] != "traces" {
		t.Fatalf("database = %q", got["database"])
	}
}

func TestParseHTTPURLRejectsSchemes(t *testing.T) {
	if _, err := parseHTTPURL("file:///etc/passwd", "http"); err == nil {
		t.Fatal("file URL should be rejected")
	}
	if _, err := parseHTTPURL("javascript:alert(1)", "http"); err == nil {
		t.Fatal("javascript URL should be rejected")
	}
}

func TestSanitizeRedactsCredentials(t *testing.T) {
	got := sanitize(context.DeadlineExceeded)
	if strings.Contains(got, "user:pass") {
		t.Fatalf("unexpected: %s", got)
	}
	got = sanitize(errString("dial tcp http://user:pass@clickhouse:8123: timeout"))
	if strings.Contains(got, "user:pass") {
		t.Fatalf("credentials leaked: %s", got)
	}
	if !strings.Contains(got, "://***@") {
		t.Fatalf("expected redaction, got %s", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestPrometheusHealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/-/healthy" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Prometheus is Healthy.\n"))
	}))
	defer server.Close()

	result := Run(context.Background(), "prometheus", Settings{"url": server.URL})
	if !result.OK {
		t.Fatalf("expected healthy prometheus, got %+v", result)
	}
}

func TestPrometheusRejectsRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.invalid/steal", http.StatusFound)
	}))
	defer server.Close()

	result := Run(context.Background(), "prometheus", Settings{"url": server.URL})
	if result.OK {
		t.Fatal("redirect should not be treated as healthy")
	}
}

func TestClickHouseQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if user != "default" || pass != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("1\n"))
	}))
	defer server.Close()

	result := Run(context.Background(), "clickhouse", Settings{
		"url":      server.URL,
		"username": "default",
		"password": "secret",
		"database": "kubetrace",
	})
	if !result.OK {
		t.Fatalf("expected clickhouse success, got %+v", result)
	}

	denied := Run(context.Background(), "clickhouse", Settings{
		"url":      server.URL,
		"username": "default",
		"password": "wrong",
	})
	if denied.OK || denied.Message != "authentication failed" {
		t.Fatalf("expected auth failure, got %+v", denied)
	}
}

func TestElasticsearchHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cluster/health" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "green"})
	}))
	defer server.Close()

	result := Run(context.Background(), "elasticsearch", Settings{"url": server.URL, "tlsVerify": "true"})
	if !result.OK {
		t.Fatalf("expected elasticsearch success, got %+v", result)
	}
}

func TestIncompleteRequiredFields(t *testing.T) {
	cases := []struct {
		tool string
		want string
	}{
		{"kafka", "bootstrap brokers are required"},
		{"clickhouse", "host is required"},
		{"minio", "endpoint is required"},
		{"ldap", "directory server URL is required"},
		{"prometheus", "API URL is required"},
		{"elasticsearch", "node URL is required"},
	}
	for _, tc := range cases {
		result := Run(context.Background(), tc.tool, Settings{})
		if result.OK || result.Message != tc.want {
			t.Fatalf("%s: got %+v, want %q", tc.tool, result, tc.want)
		}
	}
}

func TestNormalizeBroker(t *testing.T) {
	got, err := normalizeBroker("kafka.internal")
	if err != nil || got != "kafka.internal:9092" {
		t.Fatalf("got %q %v", got, err)
	}
	got, err = normalizeBroker("kafka://broker-0:9093")
	if err != nil || got != "broker-0:9093" {
		t.Fatalf("got %q %v", got, err)
	}
}

func TestNormalizeLDAPURL(t *testing.T) {
	got, err := normalizeLDAPURL("ad.internal")
	if err != nil || got != "ldap://ad.internal:389" {
		t.Fatalf("got %q %v", got, err)
	}
	if _, err := normalizeLDAPURL("http://ad.internal"); err == nil {
		t.Fatal("http LDAP URL should be rejected")
	}
}
