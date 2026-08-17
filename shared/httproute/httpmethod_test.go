package httproute

import (
	"regexp"
	"strings"
	"testing"
)

func TestIsValidHTTPMethod(t *testing.T) {
	valid := []string{
		"GET", "POST", "PATCH", "DELETE", "PURGE", "PROPFIND",
		"custom", "Custom-Token", "X-METH",
	}
	for _, m := range valid {
		if !IsValidHTTPMethod(m) {
			t.Errorf("IsValidHTTPMethod(%q) = false, want true", m)
		}
	}

	invalid := []string{
		"",
		"gzip, br",
		"https://",
		"GET /foo",
		"GET ",
		" GET",
		"GET\tPOST",
		"foo,bar",
		"GET/foo",
		"http:",
		"GET\x01",
		"GET\n",
		"GET\x7f",
		" ",
		"\t",
	}
	for _, m := range invalid {
		if IsValidHTTPMethod(m) {
			t.Errorf("IsValidHTTPMethod(%q) = true, want false", m)
		}
	}
}

func TestHTTPMethodTokenParity(t *testing.T) {
	re, err := regexp.Compile(HTTPMethodTokenPattern)
	if err != nil {
		t.Fatalf("compile %s: %v", HTTPMethodTokenPattern, err)
	}
	samples := []string{
		"GET", "POST", "PATCH", "DELETE", "PURGE", "PROPFIND", "custom",
		"", "gzip, br", "https://", "GET /foo", "GET ", " GET",
		"foo,bar", "GET/foo", "http:", "GET\x01", "0x10", " ",
	}
	for _, m := range samples {
		if IsValidHTTPMethod(m) != re.MatchString(m) {
			t.Errorf("%q: IsValidHTTPMethod=%v regexp=%v", m, IsValidHTTPMethod(m), re.MatchString(m))
		}
	}
}

func TestHTTPServerIdentityEligible(t *testing.T) {
	httpGET := map[string]string{"http.request.method": "GET", "url.path": "/x"}
	gzip := map[string]string{"http.request.method": "gzip, br", "http.method": "gzip, br"}
	https := map[string]string{"http.request.method": "https://"}
	emptyMethod := map[string]string{"http.request.method": "", "http.method": "", "url.path": "/x"}
	rpc := map[string]string{"rpc.system": "grpc", "rpc.method": "Foo"}

	if !HTTPServerIdentityEligible("SERVER", httpGET) {
		t.Fatal("valid GET SERVER must be eligible")
	}
	if HTTPServerIdentityEligible("SERVER", gzip) {
		t.Fatal("gzip, br SERVER must be ineligible")
	}
	if HTTPServerIdentityEligible("SERVER", https) {
		t.Fatal("https:// SERVER must be ineligible")
	}
	if HTTPServerIdentityEligible("SERVER", emptyMethod) {
		t.Fatal("HTTP SERVER with empty method must be ineligible")
	}
	if !HTTPServerIdentityEligible("CLIENT", gzip) {
		t.Fatal("CLIENT must remain eligible (dependency path)")
	}
	if !HTTPServerIdentityEligible("CONSUMER", gzip) {
		t.Fatal("CONSUMER must remain eligible")
	}
	if !HTTPServerIdentityEligible("SERVER", rpc) {
		t.Fatal("RPC SERVER without HTTP attrs must remain eligible")
	}
	if !HTTPServerIdentityEligible("SERVER", map[string]string{}) {
		t.Fatal("SERVER with no HTTP signal must remain eligible")
	}
}

func TestCHHTTPServerIdentityEligibleMirrorsGo(t *testing.T) {
	sql := CHHTTPServerIdentityEligible("kind", "tags")
	if !strings.Contains(sql, "upperUTF8(kind) != 'SERVER'") {
		t.Fatalf("missing SERVER gate:\n%s", sql)
	}
	if !strings.Contains(sql, "http.request.method") || !strings.Contains(sql, "http.method") {
		t.Fatalf("missing method keys:\n%s", sql)
	}
	if !strings.Contains(sql, "match(") {
		t.Fatalf("missing match():\n%s", sql)
	}
	// The SQL pattern must be the Go token pattern (quotes escaped for SQL).
	wantPat := strings.ReplaceAll(HTTPMethodTokenPattern, `'`, `\'`)
	if !strings.Contains(sql, wantPat) {
		t.Fatalf("SQL pattern %q not in:\n%s", wantPat, sql)
	}
	for _, k := range httpSignalAttrKeys {
		if !strings.Contains(sql, "tags['"+k+"']") {
			t.Fatalf("missing HTTP signal key %s in:\n%s", k, sql)
		}
	}
}
