// Package semconv bridges legacy and stable OpenTelemetry semantic conventions.
//
// The database conventions became stable in semconv 1.34 and renamed most of the
// attributes an SDK emits (db.system -> db.system.name, db.statement ->
// db.query.text, db.name -> db.namespace, ...). SDKs migrate on their own
// schedule and OTEL_SEMCONV_STABILITY_OPT_IN=database/dup makes a single service
// emit both spellings at once, so a collector cannot assume either one.
//
// Normalize resolves that: after it runs, every alias in a group holds the same
// value, and db.system/db.system.name hold the legacy and stable spelling of the
// same canonical system id. Everything downstream reads whichever it prefers.
//
// See https://opentelemetry.io/docs/specs/semconv/non-normative/db-migration/
package semconv

import "strings"

// aliasRule fills canonical from the first non-empty source, then (when
// backfill is set) copies the resolved value into every source that is still
// empty. Order within sources is significant: earlier sources win.
type aliasRule struct {
	canonical string
	sources   []string
	backfill  bool
}

// aliasRules is an ordered slice rather than a map so resolution is
// deterministic — two spans with identical attributes always resolve the same
// way, on every replica.
var aliasRules = []aliasRule{
	// --- database ---
	{canonical: "db.statement", sources: []string{"db.query.text"}, backfill: true},
	{canonical: "db.operation", sources: []string{"db.operation.name"}, backfill: true},
	{canonical: "db.name", sources: []string{"db.namespace", "db.instance"}, backfill: true},
	{canonical: "db.collection.name", sources: []string{
		"db.sql.table", "db.mongodb.collection", "db.cassandra.table", "db.cosmosdb.container",
	}},
	{canonical: "db.query.summary", sources: []string{"db.operation.name"}},

	// --- http ---
	{canonical: "http.method", sources: []string{"http.request.method"}, backfill: true},
	{canonical: "http.url", sources: []string{"url.full"}, backfill: true},
	{canonical: "http.target", sources: []string{"url.path"}, backfill: true},
	{canonical: "http.status_code", sources: []string{"http.response.status_code"}, backfill: true},
	{canonical: "http.route", sources: []string{"url.template"}},

	// --- network peer ---
	// peer.service is a logical service name rather than a host, so it is read
	// as a last-resort source but never written back into.
	{canonical: "server.address", sources: []string{
		"net.peer.name", "network.peer.address", "net.peer.ip", "http.host", "peer.service",
	}},
	{canonical: "net.peer.name", sources: []string{"server.address"}},
	{canonical: "server.port", sources: []string{"net.peer.port", "network.peer.port", "peer.port"}},
	{canonical: "net.peer.port", sources: []string{"server.port"}},

	// --- messaging ---
	{canonical: "messaging.destination", sources: []string{
		"messaging.destination.name", "messaging.destination_name",
	}, backfill: true},

	// --- errors ---
	{canonical: "error.type", sources: []string{"exception.type"}},
}

// stableSystemName maps a canonical system id to its stable db.system.name
// spelling. Ids that are already spelled correctly are absent.
var stableSystemName = map[string]string{
	"mssql":         "microsoft.sql_server",
	"oracle":        "oracle.db",
	"dynamodb":      "aws.dynamodb",
	"redshift":      "aws.redshift",
	"cosmosdb":      "azure.cosmosdb",
	"db2":           "ibm.db2",
	"informix":      "ibm.informix",
	"netezza":       "ibm.netezza",
	"spanner":       "gcp.spanner",
	"bigquery":      "gcp.bigquery",
	"hana":          "sap.hana",
	"maxdb":         "sap.maxdb",
	"adabas":        "softwareag.adabas",
	"ingres":        "actian.ingres",
	"cache":         "intersystems.cache",
	"h2":            "h2database",
	"firebird":      "firebirdsql",
	"elasticsearch": "elasticsearch",
	"cockroachdb":   "cockroachdb",
}

// canonicalSystem maps every spelling we have seen — legacy enum values, stable
// <vendor>.<product> values, and common informal names — onto one internal id.
var canonicalSystem = map[string]string{
	// relational
	"postgres": "postgresql", "postgresql": "postgresql", "pgsql": "postgresql",
	"mysql": "mysql", "mariadb": "mariadb",
	"mssql": "mssql", "sqlserver": "mssql", "microsoft.sql_server": "mssql", "mssqlcompact": "mssql",
	"oracle": "oracle", "oracle.db": "oracle",
	"db2": "db2", "ibm.db2": "db2",
	"informix": "informix", "ibm.informix": "informix",
	"netezza": "netezza", "ibm.netezza": "netezza",
	"sqlite": "sqlite", "cockroachdb": "cockroachdb", "cockroach": "cockroachdb",
	"h2": "h2", "h2database": "h2",
	"firebird": "firebird", "firebirdsql": "firebird",
	"derby": "derby", "hsqldb": "hsqldb",
	"hana": "hana", "sap.hana": "hana", "hanadb": "hana",
	"maxdb": "maxdb", "sap.maxdb": "maxdb",
	"adabas": "adabas", "softwareag.adabas": "adabas",
	"ingres": "ingres", "actian.ingres": "ingres",
	"trino": "trino", "presto": "trino", "teradata": "teradata",

	// analytical / warehouse
	"clickhouse": "clickhouse",
	"bigquery":   "bigquery", "gcp.bigquery": "bigquery",
	"redshift": "redshift", "aws.redshift": "redshift",
	"snowflake": "snowflake", "duckdb": "duckdb", "vertica": "vertica",

	// document / wide-column / graph
	"mongo": "mongodb", "mongodb": "mongodb",
	"cassandra": "cassandra", "scylladb": "cassandra", "scylla": "cassandra",
	"cosmosdb": "cosmosdb", "azure.cosmosdb": "cosmosdb",
	"dynamodb": "dynamodb", "aws.dynamodb": "dynamodb",
	"spanner": "spanner", "gcp.spanner": "spanner",
	"couchbase": "couchbase", "couchdb": "couchdb",
	"neo4j": "neo4j", "influxdb": "influxdb", "opensearch": "opensearch",
	"elastic": "elasticsearch", "elasticsearch": "elasticsearch",
	"intersystems.cache": "cache", "intersystems_cache": "cache",

	// caches / key-value
	"redis": "redis", "valkey": "valkey", "memcached": "memcached", "etcd": "etcd",
	"hazelcast": "hazelcast", "aerospike": "aerospike",

	// messaging
	//
	// "message_bus" is the legacy convention's generic value — it says a bus is
	// in use, not which one. It used to be rewritten to "rabbitmq", which would
	// label a Kafka-backed bus as RabbitMQ; it now stays generic so the UI shows
	// an unnamed queue rather than a confident wrong answer.
	"kafka":    "kafka",
	"rabbitmq": "rabbitmq", "amqp": "rabbitmq", "message_bus": "queue",
	"nats": "nats", "pulsar": "pulsar", "activemq": "activemq", "artemis": "activemq",
	"sqs": "sqs", "aws.sqs": "sqs", "sns": "sns", "aws.sns": "sns",
	"servicebus": "servicebus", "azure.servicebus": "servicebus",
	"eventhubs": "eventhubs", "azure.eventhubs": "eventhubs",
	"pubsub": "pubsub", "gcp.pubsub": "pubsub",

	// object storage
	"s3": "s3", "aws.s3": "s3", "minio": "minio",
	"gcs": "gcs", "gcp.gcs": "gcs", "azureblob": "azureblob",
}

// Normalize resolves alias groups and canonicalizes the database system id.
// It mutates tags in place and is safe to run more than once.
func Normalize(tags map[string]string) {
	if tags == nil {
		return
	}

	for _, rule := range aliasRules {
		value := strings.TrimSpace(tags[rule.canonical])
		if value == "" {
			for _, src := range rule.sources {
				if v := strings.TrimSpace(tags[src]); v != "" {
					value = v
					break
				}
			}
		}
		if value == "" {
			continue
		}
		tags[rule.canonical] = value
		if rule.backfill {
			for _, src := range rule.sources {
				if tags[src] == "" {
					tags[src] = value
				}
			}
		}
	}

	normalizeHTTPMethod(tags)
	normalizeSystemPair(tags, "db.system", "db.system.name")
	normalizeSystemPair(tags, "messaging.system", "messaging.system.name")
}

// knownHTTPMethods are the methods the conventions expect to see uppercased.
// Anything else is left exactly as the application wrote it, since an unknown
// method is not ours to reinterpret.
var knownHTTPMethods = map[string]string{
	"get": "GET", "head": "HEAD", "post": "POST", "put": "PUT",
	"delete": "DELETE", "connect": "CONNECT", "options": "OPTIONS",
	"trace": "TRACE", "patch": "PATCH", "query": "QUERY",
}

func normalizeHTTPMethod(tags map[string]string) {
	raw := strings.TrimSpace(tags["http.method"])
	if raw == "" {
		return
	}
	upper, ok := knownHTTPMethods[strings.ToLower(raw)]
	if !ok {
		return
	}
	tags["http.method"] = upper
	tags["http.request.method"] = upper
}

// normalizeSystemPair reads a system id from either the legacy or the stable
// key, canonicalizes it, and writes both spellings back.
func normalizeSystemPair(tags map[string]string, legacyKey, stableKey string) {
	raw := strings.TrimSpace(tags[legacyKey])
	if raw == "" || strings.EqualFold(raw, "unknown") {
		raw = strings.TrimSpace(tags[stableKey])
	}
	if raw == "" || strings.EqualFold(raw, "unknown") {
		return
	}
	id := CanonicalSystem(raw)
	if id == "" {
		return
	}
	tags[legacyKey] = id
	tags[stableKey] = StableSystemName(id)
}

// CanonicalSystem maps any known spelling of a system onto the internal id.
// Unknown values are lowercased and returned unchanged so new systems still
// flow through rather than being dropped.
func CanonicalSystem(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" || v == "unknown" {
		return ""
	}
	if id, ok := canonicalSystem[v]; ok {
		return id
	}
	return v
}

// StableSystemName returns the stable db.system.name spelling for a canonical id.
func StableSystemName(id string) string {
	if name, ok := stableSystemName[id]; ok {
		return name
	}
	return id
}
