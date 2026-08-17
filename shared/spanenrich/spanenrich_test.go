package spanenrich

import (
	"testing"

	"github.com/kubetrace/shared/dependency"
)

func testEnricher(t *testing.T) *Enricher {
	t.Helper()
	c, err := dependency.Default()
	if err != nil {
		t.Fatalf("default classifier: %v", err)
	}
	return NewWith(c, true, 0)
}

// The headline case: a service that upgraded to the stable conventions emits
// none of the legacy keys, and must still land on the database dashboard.
func TestStableSemconvSpanIsFullyEnriched(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"db.system.name": "postgresql",
		"db.query.text":  "SELECT id, email FROM users WHERE tenant = 'acme' AND id = 42",
		"db.namespace":   "app_prod",
		"server.address": "pg.prod.svc",
		"server.port":    "5432",
	}

	got := e.Enrich(tags, "SELECT app_prod.users", "CLIENT")

	if got.System != "postgresql" {
		t.Errorf("System = %q, want postgresql", got.System)
	}
	if !got.IsDatabase() {
		t.Errorf("Kind = %q, expected a database kind", got.Kind)
	}
	if got.Operation != "SELECT" {
		t.Errorf("Operation = %q, want SELECT", got.Operation)
	}
	if got.Collection != "users" {
		t.Errorf("Collection = %q, want users", got.Collection)
	}
	if got.Summary != "SELECT users" {
		t.Errorf("Summary = %q, want 'SELECT users'", got.Summary)
	}
	if got.Namespace != "app_prod" {
		t.Errorf("Namespace = %q, want app_prod", got.Namespace)
	}
	if got.Fingerprint == "" {
		t.Error("Fingerprint should not be empty")
	}
	// Legacy readers must see the same span.
	if tags["db.system"] != "postgresql" {
		t.Errorf("db.system = %q, want postgresql", tags["db.system"])
	}
}

// Literals must not reach storage.
func TestQueryTextIsRedacted(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"db.system":    "postgresql",
		"db.statement": "SELECT * FROM users WHERE email = 'person@example.com'",
	}
	e.Enrich(tags, "SELECT", "CLIENT")

	for _, key := range []string{"db.statement", "db.query.text"} {
		if got := tags[key]; got != "SELECT * FROM users WHERE email = ?" {
			t.Errorf("%s = %q, still contains literals", key, got)
		}
	}
}

func TestRawModeKeepsLiterals(t *testing.T) {
	c, err := dependency.Default()
	if err != nil {
		t.Fatalf("classifier: %v", err)
	}
	e := NewWith(c, false, 0)

	raw := "SELECT * FROM users WHERE email = 'person@example.com'"
	tags := map[string]string{"db.system": "postgresql", "db.statement": raw}
	got := e.Enrich(tags, "SELECT", "CLIENT")

	if tags["db.statement"] != raw {
		t.Errorf("raw mode should preserve the statement, got %q", tags["db.statement"])
	}
	// The fingerprint is still computed from the normalized form.
	if got.Fingerprint == "" {
		t.Error("fingerprint should still be produced in raw mode")
	}
}

// Two calls of the same shape must share a fingerprint so they aggregate.
func TestFingerprintGroupsAcrossCalls(t *testing.T) {
	e := testEnricher(t)

	a := e.Enrich(map[string]string{
		"db.system": "mysql", "db.statement": "SELECT * FROM t WHERE id = 1",
	}, "q", "CLIENT")
	b := e.Enrich(map[string]string{
		"db.system": "mysql", "db.statement": "SELECT * FROM t WHERE id = 77",
	}, "q", "CLIENT")

	if a.Fingerprint != b.Fingerprint {
		t.Errorf("same shape gave different fingerprints: %s vs %s", a.Fingerprint, b.Fingerprint)
	}
}

// A gateway is recorded, but never as a database.
func TestGatewayIsNotADatabase(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{"server.address": "nginx-ingress.kube-system.svc"}
	got := e.Enrich(tags, "GET /", "CLIENT")

	if got.Kind != string(dependency.KindGateway) {
		t.Errorf("Kind = %q, want gateway", got.Kind)
	}
	if got.IsDatabase() {
		t.Error("a gateway must not count as a database")
	}
	if tags["db.system"] != "" {
		t.Errorf("db.system was set to %q for a gateway", tags["db.system"])
	}
	if tags[TagSystem] != "nginx" {
		t.Errorf("%s = %q, want nginx", TagSystem, tags[TagSystem])
	}
}

func TestProvenanceTagsAreRecorded(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{"server.port": "5432"}
	e.Enrich(tags, "query", "CLIENT")

	if tags[TagEvidence] != "port" {
		t.Errorf("%s = %q, want port", TagEvidence, tags[TagEvidence])
	}
	if tags[TagConfidence] != "0.85" {
		t.Errorf("%s = %q, want 0.85", TagConfidence, tags[TagConfidence])
	}
	if tags[TagKind] != "database" {
		t.Errorf("%s = %q, want database", TagKind, tags[TagKind])
	}
}

// Re-enriching an already-enriched span must not change it, since a span can
// pass through the API backend and the ingestor in sequence.
func TestEnrichIsIdempotent(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"db.system.name": "postgresql",
		"db.query.text":  "SELECT * FROM users WHERE id = 5",
		"server.port":    "5432",
	}

	first := e.Enrich(tags, "SELECT users", "CLIENT")
	snapshot := make(map[string]string, len(tags))
	for k, v := range tags {
		snapshot[k] = v
	}

	second := e.Enrich(tags, "SELECT users", "CLIENT")

	if first != second {
		t.Errorf("second pass gave a different result:\n%+v\n%+v", first, second)
	}
	for k, v := range snapshot {
		if tags[k] != v {
			t.Errorf("second pass changed tag %s: %q -> %q", k, v, tags[k])
		}
	}
}

// Instrumentation-provided values outrank anything parsed out of the text.
func TestInstrumentationValuesArePreferred(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"db.system":          "postgresql",
		"db.operation.name":  "sElEcT",
		"db.collection.name": "AuditLog",
		"db.query.text":      "SELECT * FROM users",
	}
	got := e.Enrich(tags, "q", "CLIENT")

	if got.Operation != "sElEcT" {
		t.Errorf("Operation = %q; the conventions ask that application values keep their case", got.Operation)
	}
	if got.Collection != "AuditLog" {
		t.Errorf("Collection = %q, want AuditLog", got.Collection)
	}
}

func TestPlainHTTPSpanIsLeftAlone(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"http.request.method": "GET",
		"url.full":            "https://api.example.com/v1/users",
		"server.address":      "api.example.com",
	}
	got := e.Enrich(tags, "GET /v1/users", "CLIENT")

	if got.System != "" {
		t.Errorf("System = %q, expected no classification", got.System)
	}
	if tags["db.system"] != "" {
		t.Errorf("db.system was invented: %q", tags["db.system"])
	}
}

func TestRedisSpan(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{"server.port": "6379", "db.statement": "GET session:abc123"}
	got := e.Enrich(tags, "GET", "CLIENT")

	if got.System != "redis" || got.Kind != string(dependency.KindCache) {
		t.Errorf("got %+v, want redis/cache", got)
	}
	if tags["db.statement"] != "GET ?" {
		t.Errorf("db.statement = %q, want 'GET ?'", tags["db.statement"])
	}
}

func TestNilTags(t *testing.T) {
	e := testEnricher(t)
	if got := e.Enrich(nil, "x", "CLIENT"); got.System != "" {
		t.Errorf("nil tags should yield an empty result, got %+v", got)
	}
}

// An ORM emits an INTERNAL span carrying the same statement the driver reports
// on its CLIENT span. Classifying both counted every query twice and produced a
// second, engine-less "database" system alongside the real PostgreSQL — which
// is what showed up in production as `database` and `postgresql` side by side.
func TestORMWrapperSpanIsNotADependency(t *testing.T) {
	e := testEnricher(t)

	// The Laravel/Eloquent span: INTERNAL, generic db.system, no db.name.
	orm := map[string]string{
		"db.system":    "database",
		"db.statement": `select * from "organizations" where "organizations"."id" = 7`,
	}
	ormResult := e.Enrich(orm, `Modules\Organization\Models\Organization::get`, "INTERNAL")

	if ormResult.System != "" || ormResult.Kind != "" {
		t.Errorf("INTERNAL ORM span was classified as a dependency: %+v", ormResult)
	}
	if ormResult.IsDatabase() {
		t.Error("INTERNAL ORM span must not count as a database call")
	}
	// Redaction still has to happen — the ORM span holds the same literals.
	if orm["db.statement"] != `select * from "organizations" where "organizations"."id" = ?` {
		t.Errorf("ORM statement was not sanitized: %q", orm["db.statement"])
	}

	// The driver span for the same query is the one that counts.
	driver := map[string]string{
		"db.system":    "postgresql",
		"db.name":      "rmis_iam_backend_uat",
		"db.statement": `select * from "organizations" where "organizations"."id" = 7`,
	}
	driverResult := e.Enrich(driver, "PDOStatement::execute", "CLIENT")

	if driverResult.System != "postgresql" || !driverResult.IsDatabase() {
		t.Errorf("driver span should be the database call, got %+v", driverResult)
	}
	// Both spans describe one query, so they must share a fingerprint for
	// anyone correlating them.
	if ormResult.Fingerprint != driverResult.Fingerprint {
		t.Errorf("same query produced different fingerprints: %q vs %q",
			ormResult.Fingerprint, driverResult.Fingerprint)
	}
}

// SERVER spans are inbound; they have no remote dependency to identify. One had
// been classified as db2 purely from a name match.
func TestServerSpansAreNotDependencies(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{"server.address": "db2-reporting.internal", "server.port": "50000"}
	if got := e.Enrich(tags, "GET /report", "SERVER"); got.System != "" {
		t.Errorf("SERVER span was classified as %+v", got)
	}
}

func TestProducerAndConsumerStillClassify(t *testing.T) {
	e := testEnricher(t)
	for _, kind := range []string{"PRODUCER", "CONSUMER"} {
		tags := map[string]string{"messaging.system": "kafka"}
		if got := e.Enrich(tags, "publish", kind); got.System != "kafka" {
			t.Errorf("%s span -> %+v, want kafka", kind, got)
		}
	}
}

func TestMalformedHTTPServerPreservesRawSpan(t *testing.T) {
	e := testEnricher(t)
	cases := []struct{ name, method string }{
		{"gzip, br", "gzip, br"},
		{"https://", "https://"},
		{"", ""},
	}
	for _, tc := range cases {
		tags := map[string]string{
			"http.request.method": tc.method,
			"http.method":         tc.method,
		}
		if tc.method == "" {
			tags["url.path"] = "/still-http"
		}
		got := e.Enrich(tags, tc.name, "SERVER")
		if got.Transaction != "" {
			t.Errorf("%q: Transaction = %q, want empty", tc.name, got.Transaction)
		}
		if tags[TagHTTPIdentity] != HTTPIdentityMalformed {
			t.Errorf("%q: missing malformed marker, got %q", tc.name, tags[TagHTTPIdentity])
		}
		if tags["http.request.method"] != tc.method {
			t.Errorf("%q: method rewritten to %q", tc.name, tags["http.request.method"])
		}
	}
}

func TestCLIENTMalformedMethodStillDependencyPath(t *testing.T) {
	e := testEnricher(t)
	tags := map[string]string{
		"http.request.method": "gzip, br",
		"http.method":         "gzip, br",
		"url.full":            "https://example.com/g/collect",
	}
	got := e.Enrich(tags, "gzip, br", "CLIENT")
	if tags[TagHTTPIdentity] == HTTPIdentityMalformed {
		t.Fatal("CLIENT must not be marked malformed HTTP SERVER telemetry")
	}
	if got.Transaction == "" {
		t.Fatal("CLIENT must still follow existing naming, not SERVER identity gating")
	}
}

func TestCustomTokenMethodRemainsTransaction(t *testing.T) {
	e := testEnricher(t)
	for _, method := range []string{"PURGE", "PROPFIND", "CUSTOM"} {
		tags := map[string]string{"http.request.method": method, "http.route": "/cache"}
		got := e.Enrich(tags, method, "SERVER")
		if got.Transaction == "" {
			t.Errorf("%s: Transaction empty", method)
		}
		if tags[TagHTTPIdentity] != "" {
			t.Errorf("%s: unexpectedly marked %q", method, tags[TagHTTPIdentity])
		}
	}
	getTags := map[string]string{"http.request.method": "GET", "http.route": "/g/collect"}
	got := e.Enrich(getTags, "GET", "SERVER")
	if got.Transaction != "GET /g/collect" {
		t.Errorf("GET /g/collect Transaction = %q", got.Transaction)
	}
}
