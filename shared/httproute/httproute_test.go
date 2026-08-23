package httproute

import "testing"

// The paths in these tests are the real shapes observed in the cluster, so a
// regression here corresponds to a visible regression in Top transactions.
func TestNormalizePath(t *testing.T) {
	tests := []struct{ in, want string }{
		// Record ids must collapse, or one endpoint becomes thousands of rows.
		{"/api/v2/inter-service/users/11", "/api/v2/inter-service/users/{id}"},
		{"/api/v2/inter-service/users/65", "/api/v2/inter-service/users/{id}"},
		{"/api/v2/inter-service/organizations/272", "/api/v2/inter-service/organizations/{id}"},

		// Collection endpoints stay distinct from item endpoints.
		{"/api/v2/inter-service/users", "/api/v2/inter-service/users"},

		// Readable route names are not identifiers and must survive, or
		// genuinely different endpoints merge.
		{"/api/v2/dictionary/inter-service/dictionary-values/PROCUREMENT_METHOD",
			"/api/v2/dictionary/inter-service/dictionary-values/PROCUREMENT_METHOD"},
		{"/api/v2/dictionary/inter-service/functional-sections/get-tree",
			"/api/v2/dictionary/inter-service/functional-sections/get-tree"},
		{"/api/v2/inter-service/users/authorization/me",
			"/api/v2/inter-service/users/authorization/me"},
		{"/api/health/readiness", "/api/health/readiness"},

		// Version segments are not ids.
		{"/api/v2/users", "/api/v2/users"},

		// Already-templated segments are left alone.
		{"/actuator/health/{*path}", "/actuator/health/{*path}"},
		{"/users/{id}/roles", "/users/{id}/roles"},
		{"/users/:id/roles", "/users/:id/roles"},

		// Opaque identifiers.
		{"/files/550e8400-e29b-41d4-a716-446655440000", "/files/{uuid}"},
		{"/blobs/a3f5c9d2e8b71046ff2c", "/blobs/{hash}"},
		{"/sessions/01HQ8Z3XK4MNPQ7R", "/sessions/{id}"},

		// Query strings are parameters, not endpoints.
		{"/search?q=alice&page=3", "/search"},
		{"/search#frag", "/search"},

		// Trailing slashes are not distinct endpoints.
		{"/api/users/", "/api/users"},
		{"/", "/"},
		{"", ""},
	}

	for _, tc := range tests {
		if got := NormalizePath(tc.in); got != tc.want {
			t.Errorf("NormalizePath(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
		}
	}
}

// Deep paths are truncated rather than allowed to add unbounded cardinality.
func TestNormalizePathTruncatesDeepPaths(t *testing.T) {
	got := NormalizePath("/a/b/c/d/e/f/g/h/i/j/k/l")
	if got != "/a/b/c/d/e/f/g/h/..." {
		t.Errorf("deep path = %q", got)
	}
}

// intgw-backend and extgw-backend name every server span after the bare method
// and emit no http.route, so ~28k spans a day collapse into one row. The path
// is the only thing that can recover the endpoint.
func TestBareMethodSpanRecoversEndpointFromPath(t *testing.T) {
	tags := map[string]string{
		"http.method": "GET",
		"url.path":    "/api/v2/inter-service/users/11",
	}
	if got := TransactionName(tags, "GET"); got != "GET /api/v2/inter-service/users/{id}" {
		t.Errorf("TransactionName = %q", got)
	}

	// Two ids must land on one transaction.
	a := TransactionName(map[string]string{"http.method": "GET", "url.path": "/api/v2/inter-service/users/11"}, "GET")
	b := TransactionName(map[string]string{"http.method": "GET", "url.path": "/api/v2/inter-service/users/65"}, "GET")
	if a != b {
		t.Errorf("same endpoint split into %q and %q", a, b)
	}
}

// When the framework supplies a route, it wins over the raw path.
func TestRoutePreferredOverPath(t *testing.T) {
	tags := map[string]string{
		"http.method": "GET",
		"http.route":  "/users/{id}",
		"url.path":    "/users/11",
	}
	if got := TransactionName(tags, "GET"); got != "GET /users/{id}" {
		t.Errorf("TransactionName = %q, want the templated route", got)
	}
}

// A span the instrumentation already named well must not be rewritten.
func TestWellNamedSpansAreLeftAlone(t *testing.T) {
	tests := []struct{ spanName, want string }{
		{"GET /api/health/readiness", "GET /api/health/readiness"},
		{"GET /actuator/health/{*path}", "GET /actuator/health/{*path}"},
		{"SELECT orders", "SELECT orders"},
		{"PDOStatement::execute", "PDOStatement::execute"},
		{"Modules\\User\\Models\\User::get", "Modules\\User\\Models\\User::get"},
		{"middleware - jsonParser", "middleware - jsonParser"},
	}
	for _, tc := range tests {
		if got := TransactionName(map[string]string{}, tc.spanName); got != tc.want {
			t.Errorf("TransactionName(%q) = %q, want %q", tc.spanName, got, tc.want)
		}
	}
}

// A span already named with a path still gets its ids collapsed.
func TestNamedSpanWithRawIDsIsNormalized(t *testing.T) {
	got := TransactionName(map[string]string{"http.method": "GET"}, "GET /api/orders/98765")
	if got != "GET /api/orders/{id}" {
		t.Errorf("TransactionName = %q", got)
	}
}

func TestFallsBackToURLWhenPathAbsent(t *testing.T) {
	tags := map[string]string{
		"http.method": "POST",
		"http.url":    "https://svc.internal:8080/api/orders/42?debug=1",
	}
	if got := TransactionName(tags, "POST"); got != "POST /api/orders/{id}" {
		t.Errorf("TransactionName = %q", got)
	}
}

func TestNoRoutingInformationKeepsSpanName(t *testing.T) {
	if got := TransactionName(map[string]string{"http.method": "GET"}, "GET"); got != "GET" {
		t.Errorf("TransactionName = %q, want the original name", got)
	}
	if got := TransactionName(nil, "whatever"); got != "whatever" {
		t.Errorf("TransactionName(nil) = %q", got)
	}
}

func TestIsBareMethod(t *testing.T) {
	for _, name := range []string{"GET", "post", " PUT "} {
		if !isBareMethod(name) {
			t.Errorf("%q should be a bare method", name)
		}
	}
	for _, name := range []string{"GET /x", "SELECT", "getUser"} {
		if isBareMethod(name) {
			t.Errorf("%q should not be a bare method", name)
		}
	}
}

func TestGarbageSpanNameRecoversRoute(t *testing.T) {
	tags := map[string]string{
		"http.method": "GET",
		"url.path":    "/actuator/health",
	}
	if got := TransactionName(tags, "nosniff"); got != "GET /actuator/health" {
		t.Errorf("nosniff = %q, want recovered route", got)
	}
	if got := TransactionName(tags, "text/html"); got != "GET /actuator/health" {
		t.Errorf("text/html = %q, want recovered route", got)
	}
}

func TestControlCharactersCollapseToBareMethod(t *testing.T) {
	tags := map[string]string{
		"http.method": "POST",
		"url.path":    "/g/collect",
	}
	if got := TransactionName(tags, "POST\x08"); got != "POST /g/collect" {
		t.Errorf("control-char name = %q, want recovered route", got)
	}
}
