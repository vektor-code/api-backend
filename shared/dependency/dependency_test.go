package dependency

import (
	"testing"
)

func testClassifier(t *testing.T) *Classifier {
	t.Helper()
	c, err := NewFromJSON(defaultRulesJSON)
	if err != nil {
		t.Fatalf("embedded ruleset failed to load: %v", err)
	}
	return c
}

func TestEmbeddedRulesetIsValid(t *testing.T) {
	testClassifier(t)
}

func TestExplicitSystemWins(t *testing.T) {
	c := testClassifier(t)

	// An explicit db.system must beat a port that says something else.
	got, ok := c.Classify(map[string]string{
		"db.system":   "mysql",
		"server.port": "5432",
	}, "query", "CLIENT")
	if !ok || got.System != "mysql" {
		t.Fatalf("explicit db.system should win, got %+v", got)
	}
	if got.Confidence != ConfidenceExplicit {
		t.Errorf("confidence = %v, want %v", got.Confidence, ConfidenceExplicit)
	}
	if got.Evidence != "db.system" {
		t.Errorf("evidence = %q, want db.system", got.Evidence)
	}
}

func TestPortDetection(t *testing.T) {
	c := testClassifier(t)
	tests := []struct {
		port, host, wantSystem string
		wantKind               Kind
	}{
		{"5432", "", "postgresql", KindDatabase},
		{"3306", "", "mysql", KindDatabase},
		{"6379", "", "redis", KindCache},
		{"27017", "", "mongodb", KindDatabase},
		{"9092", "", "kafka", KindMessaging},
		{"5672", "", "rabbitmq", KindMessaging},
		{"9042", "", "cassandra", KindDatabase},
		{"1433", "", "mssql", KindDatabase},
		{"8123", "", "clickhouse", KindDatabase},
	}
	for _, tc := range tests {
		got, ok := c.Classify(map[string]string{
			"server.port": tc.port, "server.address": tc.host,
		}, "op", "CLIENT")
		if !ok || got.System != tc.wantSystem || got.Kind != tc.wantKind {
			t.Errorf("port %s -> %+v, want %s/%s", tc.port, got, tc.wantSystem, tc.wantKind)
		}
	}
}

// Ports that unrelated software commonly uses must not classify on the number
// alone; they need corroboration from the host name.
func TestAmbiguousPortsRequireHostHint(t *testing.T) {
	c := testClassifier(t)

	if got, ok := c.Classify(map[string]string{"server.port": "9000"}, "op", "CLIENT"); ok {
		t.Errorf("bare port 9000 should not classify, got %+v", got)
	}
	if got, ok := c.Classify(map[string]string{"server.port": "8000"}, "op", "CLIENT"); ok {
		t.Errorf("bare port 8000 should not classify, got %+v", got)
	}

	got, ok := c.Classify(map[string]string{
		"server.port": "9000", "server.address": "minio.trace-prod.svc",
	}, "op", "CLIENT")
	if !ok || got.System != "minio" {
		t.Errorf("port 9000 with minio host -> %+v, want minio", got)
	}

	got, ok = c.Classify(map[string]string{
		"server.port": "9000", "server.address": "clickhouse.trace-prod.svc",
	}, "op", "CLIENT")
	if !ok || got.System != "clickhouse" {
		t.Errorf("port 9000 with clickhouse host -> %+v, want clickhouse", got)
	}
}

// Port 8200 is used by both Vault and Elastic APM Server.
func TestSharedPortDisambiguatesByHost(t *testing.T) {
	c := testClassifier(t)

	got, _ := c.Classify(map[string]string{
		"server.port": "8200", "server.address": "apm-server.observability.svc",
	}, "op", "CLIENT")
	if got.System != "apm" {
		t.Errorf("8200 + apm host -> %q, want apm", got.System)
	}

	got, _ = c.Classify(map[string]string{
		"server.port": "8200", "server.address": "secrets.internal",
	}, "op", "CLIENT")
	if got.System != "vault" {
		t.Errorf("8200 without apm host -> %q, want vault", got.System)
	}
}

// The previous implementation matched "pg" as a bare substring, so any host or
// span name containing "upgrade" was reported as PostgreSQL.
func TestTokenMatchingAvoidsSubstringFalsePositives(t *testing.T) {
	c := testClassifier(t)

	for _, host := range []string{"upgrade-service", "image-upgrader.default.svc", "pageviews"} {
		if got, ok := c.Classify(map[string]string{"server.address": host}, "op", "CLIENT"); ok && got.System == "postgresql" {
			t.Errorf("host %q was misclassified as postgresql", host)
		}
	}

	// A real token boundary still matches.
	got, ok := c.Classify(map[string]string{"server.address": "rmis-pg-primary.db.svc"}, "op", "CLIENT")
	if !ok || got.System != "postgresql" {
		t.Errorf("host rmis-pg-primary -> %+v, want postgresql", got)
	}
}

// Generic tokens are trustworthy in a hostname and misleading in a span name.
func TestGenericTokensAreHostScoped(t *testing.T) {
	c := testClassifier(t)

	got, ok := c.Classify(map[string]string{"server.address": "orders-db.prod.svc"}, "op", "CLIENT")
	if !ok || got.System != "database" {
		t.Errorf("host orders-db -> %+v, want generic database", got)
	}

	if got, ok := c.Classify(map[string]string{}, "GET /api/db-health", "CLIENT"); ok {
		t.Errorf("span name alone should not imply a database, got %+v", got)
	}
}

// An HTTP call to a service whose name mentions a database is still an HTTP
// call. Weak name evidence must not manufacture database spans.
func TestHTTPSpansAreNotTurnedIntoDatabaseCalls(t *testing.T) {
	c := testClassifier(t)

	tags := map[string]string{
		"http.request.method": "GET",
		"url.full":            "https://postgres-admin.example.com/status",
		"server.address":      "postgres-admin.example.com",
	}
	if got, ok := c.Classify(tags, "GET /status", "CLIENT"); ok && got.Kind == KindDatabase {
		t.Errorf("HTTP span was classified as a database call: %+v", got)
	}

	// With a real database attribute present, the same host is trusted again.
	tags["db.statement"] = "SELECT ?"
	got, ok := c.Classify(tags, "GET /status", "CLIENT")
	if !ok || got.System != "postgresql" {
		t.Errorf("db attributes present -> %+v, want postgresql", got)
	}
}

// Messaging over HTTP (SQS, SNS, Pub/Sub) must survive the HTTP guard.
func TestMessagingOverHTTPStillClassifies(t *testing.T) {
	c := testClassifier(t)
	got, ok := c.Classify(map[string]string{
		"http.request.method": "POST",
		"server.address":      "sqs.eu-west-1.amazonaws.com",
	}, "SQS.SendMessage", "CLIENT")
	if !ok || got.System != "sqs" || got.Kind != KindMessaging {
		t.Errorf("SQS over HTTP -> %+v, want sqs/messaging", got)
	}
}

// Gateways and secret stores must not be filed as databases.
func TestNonDatabaseKindsAreSeparated(t *testing.T) {
	c := testClassifier(t)
	tests := []struct {
		host     string
		wantKind Kind
	}{
		{"nginx-ingress.kube-system.svc", KindGateway},
		{"vault.security.svc", KindSecrets},
		{"consul.discovery.svc", KindDiscovery},
		{"minio.trace-prod.svc", KindStorage},
	}
	for _, tc := range tests {
		got, ok := c.Classify(map[string]string{"server.address": tc.host}, "op", "CLIENT")
		if !ok || got.Kind != tc.wantKind {
			t.Errorf("host %s -> %+v, want kind %s", tc.host, got, tc.wantKind)
		}
	}
}

// Operator-configured host rules replace what used to be a hardcoded IP.
func TestHostRulesAndCIDR(t *testing.T) {
	c, err := New(Ruleset{
		Systems: []SystemRule{{ID: "oracle", Kind: KindDatabase}},
		Hosts: []HostRule{
			{Match: "exact", Value: "10.254.5.30", System: "oracle"},
			{Match: "cidr", Value: "10.99.0.0/16", System: "oracle"},
		},
	})
	if err != nil {
		t.Fatalf("ruleset: %v", err)
	}

	got, ok := c.Classify(map[string]string{"server.address": "10.254.5.30"}, "op", "CLIENT")
	if !ok || got.System != "oracle" || got.Evidence != "host.rule" {
		t.Errorf("exact host rule -> %+v", got)
	}

	got, ok = c.Classify(map[string]string{"server.address": "10.99.4.7"}, "op", "CLIENT")
	if !ok || got.System != "oracle" || got.Evidence != "host.cidr" {
		t.Errorf("cidr host rule -> %+v", got)
	}

	if _, ok := c.Classify(map[string]string{"server.address": "10.98.4.7"}, "op", "CLIENT"); ok {
		t.Error("address outside the CIDR should not match")
	}
}

func TestHostPortSplitting(t *testing.T) {
	c := testClassifier(t)

	got, ok := c.Classify(map[string]string{"server.address": "db.internal:5432"}, "op", "CLIENT")
	if !ok || got.System != "postgresql" {
		t.Errorf("host:port form -> %+v, want postgresql", got)
	}

	got, ok = c.Classify(map[string]string{"server.address": "[2001:db8::1]:6379"}, "op", "CLIENT")
	if !ok || got.System != "redis" {
		t.Errorf("ipv6 host:port form -> %+v, want redis", got)
	}
}

// Classification must not depend on Go map iteration order.
func TestClassificationIsDeterministic(t *testing.T) {
	c := testClassifier(t)
	tags := map[string]string{
		"server.address":     "clickhouse-nginx-vault.svc",
		"db.statement":       "SELECT ?",
		"some.other.attr":    "redis-ish",
		"another.attr":       "kafka-ish",
		"yet.another.attr":   "mongo-ish",
		"still.another.attr": "minio-ish",
	}

	first, _ := c.Classify(tags, "query", "CLIENT")
	for i := 0; i < 200; i++ {
		got, _ := c.Classify(tags, "query", "CLIENT")
		if got != first {
			t.Fatalf("classification varied between runs: %+v then %+v", first, got)
		}
	}
}

func TestDBAttributesFallback(t *testing.T) {
	c := testClassifier(t)
	got, ok := c.Classify(map[string]string{"db.statement": "SELECT ?"}, "query", "CLIENT")
	if !ok || got.System != "database" || got.Confidence != ConfidenceAttribute {
		t.Errorf("db attributes fallback -> %+v", got)
	}
}

func TestUnclassifiedSpan(t *testing.T) {
	c := testClassifier(t)
	if got, ok := c.Classify(map[string]string{"http.request.method": "GET"}, "GET /healthz", "CLIENT"); ok {
		t.Errorf("plain HTTP span should not classify, got %+v", got)
	}
}

func TestRulesetValidationRejectsUnknownSystem(t *testing.T) {
	_, err := New(Ruleset{
		Systems: []SystemRule{{ID: "postgresql", Kind: KindDatabase}},
		Ports:   []PortRule{{Port: "5432", System: "typo-postgres"}},
	})
	if err == nil {
		t.Fatal("expected an error for a port referencing an unknown system")
	}
}

func TestRulesetValidationRejectsBadCIDR(t *testing.T) {
	_, err := New(Ruleset{
		Systems: []SystemRule{{ID: "oracle", Kind: KindDatabase}},
		Hosts:   []HostRule{{Match: "cidr", Value: "not-a-cidr", System: "oracle"}},
	})
	if err == nil {
		t.Fatal("expected an error for an invalid CIDR")
	}
}

// A Laravel job queue backed by Redis reports both db.system=redis and
// messaging.system=redis. Reading db.system first filed these as a cache, so
// queue traffic never appeared under messaging at all.
func TestRedisBackedQueueIsMessagingNotCache(t *testing.T) {
	c := testClassifier(t)
	tags := map[string]string{
		"db.system":             "redis",
		"messaging.system":      "redis",
		"messaging.destination": "queues:template-sync",
	}

	for _, kind := range []string{"PRODUCER", "CONSUMER"} {
		got, ok := c.Classify(tags, "process queues:template-sync", kind)
		if !ok || got.Kind != KindMessaging {
			t.Errorf("%s span -> %+v, want messaging", kind, got)
		}
		if got.System != "redis" {
			t.Errorf("%s span system = %q, want redis", kind, got.System)
		}
	}

	// A plain CLIENT span against Redis is still a cache.
	got, ok := c.Classify(map[string]string{"db.system": "redis"}, "GET", "CLIENT")
	if !ok || got.Kind != KindCache {
		t.Errorf("CLIENT redis -> %+v, want cache", got)
	}
}

func TestHTTPClientIsNotDatabaseFromPeerCIDR(t *testing.T) {
	c := testClassifier(t)
	tags := map[string]string{
		"http.request.method": "GET",
		"url.path":            "/apis/apps/v1/namespaces/default/deployments/x",
		"server.address":      "crtnet-ext-k8s-ha.cloudraft.dc",
	}
	if _, ok := c.Classify(tags, "GET", "CLIENT"); ok {
		t.Fatal("HTTP CLIENT must not be classified as a database even when db.name is present from pod hints")
	}
}

func TestDNSLookupIsNotPostgreSQL(t *testing.T) {
	c := testClassifier(t)
	tags := map[string]string{"net.peer.name": "172.16.45.15", "server.address": "172.16.45.15"}
	if got, ok := c.Classify(tags, "dns.lookup", "CLIENT"); ok {
		t.Fatalf("dns.lookup classified as %+v", got)
	}
}

// Laravel's queue instrumentation emits a full set of messaging.* attributes
// and no messaging.system. Those spans were left unclassified.
func TestQueueSpanWithoutBrokerNameIsStillMessaging(t *testing.T) {
	c := testClassifier(t)
	tags := map[string]string{
		"messaging.destination":       "(anonymous)",
		"messaging.message.id":        "d655f357-dd90-48b8-b57f-fd3a",
		"messaging.message.job_name":  `Modules\Contract\Jobs\Insert`,
		"messaging.message.max_tries": "3",
		"messaging.message.attempts":  "0",
	}

	got, ok := c.Classify(tags, "process (anonymous)", "CONSUMER")
	if !ok {
		t.Fatal("queue span with messaging attributes was not classified")
	}
	if got.Kind != KindMessaging {
		t.Errorf("Kind = %q, want messaging", got.Kind)
	}
	if got.Evidence != "messaging.attributes" {
		t.Errorf("Evidence = %q, want messaging.attributes", got.Evidence)
	}

	// The same attributes on a CLIENT span are not a queue operation.
	if got, ok := c.Classify(tags, "something", "CLIENT"); ok && got.Kind == KindMessaging {
		t.Errorf("CLIENT span was treated as a queue: %+v", got)
	}
}

// Kafka detection itself, by each available signal.
func TestKafkaDetectedByEverySignal(t *testing.T) {
	c := testClassifier(t)
	cases := []struct {
		name string
		tags map[string]string
		span string
	}{
		{"explicit messaging.system", map[string]string{"messaging.system": "kafka"}, "send"},
		{"broker port", map[string]string{"server.port": "9092"}, "send"},
		{"alternate broker port", map[string]string{"server.port": "29092"}, "send"},
		{"broker hostname", map[string]string{"server.address": "kafka-0.kafka.svc"}, "send"},
		{"redpanda hostname", map[string]string{"server.address": "redpanda.default.svc"}, "send"},
	}
	for _, tc := range cases {
		got, ok := c.Classify(tc.tags, tc.span, "PRODUCER")
		if !ok || got.Kind != KindMessaging {
			t.Errorf("%s -> %+v, want messaging", tc.name, got)
		}
	}
}

// Kafka and RabbitMQ must never be mistaken for one another. The previous
// implementation defaulted *any* unidentified messaging span to "rabbitmq"
// (both in the ingestor's inferDependencySystem and the API's
// enrichSpanMetadata), so a Kafka producer with no explicit messaging.system
// was confidently labelled RabbitMQ.
func TestKafkaAndRabbitAreNeverConfused(t *testing.T) {
	c := testClassifier(t)

	kafka := []map[string]string{
		{"messaging.system": "kafka"},
		{"server.port": "9092"},
		{"server.port": "9093"},
		{"server.port": "29092"},
		{"server.address": "kafka-0.kafka.svc.cluster.local"},
		{"server.address": "redpanda.default.svc"},
		{"messaging.system": "kafka", "server.port": "5672"}, // explicit beats port
	}
	for _, tags := range kafka {
		got, ok := c.Classify(tags, "send", "PRODUCER")
		if !ok || got.System != "kafka" {
			t.Errorf("expected kafka for %v, got %+v", tags, got)
		}
	}

	rabbit := []map[string]string{
		{"messaging.system": "rabbitmq"},
		{"server.port": "5672"},
		{"server.port": "5671"},
		{"server.port": "15672"},
		{"server.address": "rabbitmq.default.svc"},
		{"server.address": "amqp-broker.internal"},
		{"messaging.system": "rabbitmq", "server.port": "9092"}, // explicit beats port
	}
	for _, tags := range rabbit {
		got, ok := c.Classify(tags, "publish", "PRODUCER")
		if !ok || got.System != "rabbitmq" {
			t.Errorf("expected rabbitmq for %v, got %+v", tags, got)
		}
	}
}

// An unidentified queue must stay unidentified rather than being guessed as a
// specific broker. This is the regression that mattered most: it is what made
// Kafka traffic show up as RabbitMQ.
func TestUnknownBrokerIsNotGuessed(t *testing.T) {
	c := testClassifier(t)

	got, ok := c.Classify(map[string]string{
		"messaging.destination": "orders",
		"messaging.message.id":  "abc-123",
	}, "send orders", "PRODUCER")

	if !ok || got.Kind != KindMessaging {
		t.Fatalf("expected a messaging classification, got %+v", got)
	}
	for _, wrong := range []string{"rabbitmq", "kafka", "nats", "pulsar", "activemq"} {
		if got.System == wrong {
			t.Errorf("unidentified queue was guessed as %q", wrong)
		}
	}
	if got.System != "queue" {
		t.Errorf("System = %q, want the generic \"queue\"", got.System)
	}
}

// Broker ports must not overlap between products, or a port match becomes a
// coin flip. This asserts the property rather than trusting the table by eye.
func TestBrokerPortsAreUnambiguous(t *testing.T) {
	c := testClassifier(t)
	seen := map[string]string{}
	for _, p := range c.rules.Ports {
		if len(p.HostContains) > 0 {
			continue // deliberately disambiguated by hostname
		}
		if prev, dup := seen[p.Port]; dup && prev != p.System {
			t.Errorf("port %s maps to both %q and %q with no host hint", p.Port, prev, p.System)
		}
		seen[p.Port] = p.System
	}
}

// ActiveMQ speaks AMQP too, so its hostname must not fall into the rabbitmq
// rule via the "amqp" substring.
func TestActiveMQIsNotRabbit(t *testing.T) {
	c := testClassifier(t)
	for _, host := range []string{"activemq-broker.svc", "artemis-0.artemis.svc"} {
		got, ok := c.Classify(map[string]string{"server.address": host}, "send", "PRODUCER")
		if !ok || got.System != "activemq" {
			t.Errorf("host %s -> %+v, want activemq", host, got)
		}
	}
}

// The legacy generic value must not be resolved to a specific broker.
func TestMessageBusIsNotAssumedRabbit(t *testing.T) {
	c := testClassifier(t)
	got, ok := c.Classify(map[string]string{"messaging.system": "queue"}, "send", "PRODUCER")
	if !ok || got.Kind != KindMessaging {
		t.Fatalf("got %+v", got)
	}
	if got.System == "rabbitmq" {
		t.Error("generic message_bus was resolved to rabbitmq")
	}
}
