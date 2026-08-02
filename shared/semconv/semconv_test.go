package semconv

import "testing"

// An SDK that emits only the stable spelling must still be readable by code
// that asks for the legacy keys, and vice versa.
func TestNormalizeBridgesStableAndLegacy(t *testing.T) {
	stable := map[string]string{
		"db.system.name":      "postgresql",
		"db.query.text":       "SELECT ?",
		"db.namespace":        "orders",
		"db.operation.name":   "SELECT",
		"http.request.method": "get",
		"url.full":            "https://x/y",
		"server.address":      "db.internal",
		"server.port":         "5432",
	}
	Normalize(stable)

	for key, want := range map[string]string{
		"db.system":     "postgresql",
		"db.statement":  "SELECT ?",
		"db.name":       "orders",
		"db.operation":  "SELECT",
		"http.method":   "GET", // known methods are uppercased
		"http.url":      "https://x/y",
		"net.peer.name": "db.internal",
		"net.peer.port": "5432",
	} {
		if stable[key] != want {
			t.Errorf("legacy key %s = %q, want %q", key, stable[key], want)
		}
	}

	legacy := map[string]string{
		"db.system":     "postgresql",
		"db.statement":  "SELECT ?",
		"db.name":       "orders",
		"http.method":   "GET",
		"net.peer.name": "db.internal",
		"net.peer.port": "5432",
	}
	Normalize(legacy)

	for key, want := range map[string]string{
		"db.system.name":      "postgresql",
		"db.query.text":       "SELECT ?",
		"db.namespace":        "orders",
		"http.request.method": "GET",
		"server.address":      "db.internal",
		"server.port":         "5432",
	} {
		if legacy[key] != want {
			t.Errorf("stable key %s = %q, want %q", key, legacy[key], want)
		}
	}
}

// The stable conventions renamed enum values to a <vendor>.<product> form.
func TestCanonicalSystemHandlesStableEnums(t *testing.T) {
	tests := map[string]string{
		"microsoft.sql_server": "mssql",
		"oracle.db":            "oracle",
		"aws.dynamodb":         "dynamodb",
		"azure.cosmosdb":       "cosmosdb",
		"gcp.spanner":          "spanner",
		"ibm.db2":              "db2",
		"sap.hana":             "hana",
		"postgres":             "postgresql",
		"mongo":                "mongodb",
		"elastic":              "elasticsearch",
		// Generic legacy value: it names a bus, not a product. Resolving it to
		// rabbitmq would mislabel a Kafka-backed bus.
		"message_bus": "queue",
		"MySQL":       "mysql",
		"unknown":     "",
		"":            "",
	}
	for in, want := range tests {
		if got := CanonicalSystem(in); got != want {
			t.Errorf("CanonicalSystem(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStableSystemNameRoundTrip(t *testing.T) {
	tests := map[string]string{
		"mssql":      "microsoft.sql_server",
		"oracle":     "oracle.db",
		"postgresql": "postgresql",
		"redis":      "redis",
	}
	for id, want := range tests {
		if got := StableSystemName(id); got != want {
			t.Errorf("StableSystemName(%q) = %q, want %q", id, got, want)
		}
		if back := CanonicalSystem(want); back != id {
			t.Errorf("round trip of %q gave %q, want %q", id, back, id)
		}
	}
}

// A service running with OTEL_SEMCONV_STABILITY_OPT_IN=database/dup emits both
// spellings at once; they must reconcile to one canonical id.
func TestDupModeEmitsBothSpellings(t *testing.T) {
	tags := map[string]string{
		"db.system":      "mssql",
		"db.system.name": "microsoft.sql_server",
	}
	Normalize(tags)

	if tags["db.system"] != "mssql" {
		t.Errorf("db.system = %q, want mssql", tags["db.system"])
	}
	if tags["db.system.name"] != "microsoft.sql_server" {
		t.Errorf("db.system.name = %q, want microsoft.sql_server", tags["db.system.name"])
	}
}

func TestNormalizeIsIdempotent(t *testing.T) {
	tags := map[string]string{"db.system.name": "oracle.db", "db.query.text": "SELECT ?"}
	Normalize(tags)
	first := make(map[string]string, len(tags))
	for k, v := range tags {
		first[k] = v
	}
	Normalize(tags)

	if len(first) != len(tags) {
		t.Fatalf("second pass changed key count: %d then %d", len(first), len(tags))
	}
	for k, v := range first {
		if tags[k] != v {
			t.Errorf("second pass changed %s: %q -> %q", k, v, tags[k])
		}
	}
}

func TestUnknownSystemPassesThrough(t *testing.T) {
	// A store we have never heard of should survive rather than be dropped.
	if got := CanonicalSystem("QuestDB"); got != "questdb" {
		t.Errorf("CanonicalSystem(QuestDB) = %q, want questdb", got)
	}
}

func TestNormalizeNilMap(t *testing.T) {
	Normalize(nil) // must not panic
}

func TestCollectionAliases(t *testing.T) {
	tags := map[string]string{"db.sql.table": "orders"}
	Normalize(tags)
	if tags["db.collection.name"] != "orders" {
		t.Errorf("db.collection.name = %q, want orders", tags["db.collection.name"])
	}

	tags = map[string]string{"db.mongodb.collection": "sessions"}
	Normalize(tags)
	if tags["db.collection.name"] != "sessions" {
		t.Errorf("db.collection.name = %q, want sessions", tags["db.collection.name"])
	}
}

// peer.service is a logical service name, not a host, so it may seed
// server.address but must never be overwritten by it.
func TestPeerServiceIsNotBackfilled(t *testing.T) {
	tags := map[string]string{"server.address": "10.0.0.5"}
	Normalize(tags)
	if tags["peer.service"] != "" {
		t.Errorf("peer.service should stay empty, got %q", tags["peer.service"])
	}
}

func TestHTTPMethodNormalization(t *testing.T) {
	// Known methods are uppercased on both spellings.
	tags := map[string]string{"http.request.method": "post"}
	Normalize(tags)
	if tags["http.method"] != "POST" || tags["http.request.method"] != "POST" {
		t.Errorf("method = %q/%q, want POST/POST", tags["http.method"], tags["http.request.method"])
	}

	// An unknown method is not ours to reinterpret.
	tags = map[string]string{"http.request.method": "PurgeCache"}
	Normalize(tags)
	if tags["http.method"] != "PurgeCache" {
		t.Errorf("unknown method was rewritten to %q", tags["http.method"])
	}
}
