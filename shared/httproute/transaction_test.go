package httproute

import (
	"strings"
	"testing"
)

func TestTransactionIdentityEligible(t *testing.T) {
	httpGET := map[string]string{"http.request.method": "GET", "url.path": "/users"}
	gzip := map[string]string{"http.request.method": "gzip, br", "http.method": "gzip, br"}
	jdbc := map[string]string{"db.system": "postgresql", "db.name": "dashboard_db"}
	hikari := map[string]string{"thread.name": "HikariPool-1:housekeeper"}
	rpc := map[string]string{"rpc.system": "grpc"}

	if !TransactionIdentityEligible("SERVER", "GET /users", httpGET) {
		t.Fatal("HTTP SERVER must be a transaction")
	}
	if TransactionIdentityEligible("SERVER", "gzip, br", gzip) {
		t.Fatal("malformed HTTP SERVER must not be a transaction")
	}
	if !TransactionIdentityEligible("SERVER", "/trading.OrderGateway/GetOrder", rpc) {
		t.Fatal("RPC SERVER must remain a transaction")
	}
	if TransactionIdentityEligible("CLIENT", "dashboard_db", jdbc) {
		t.Fatal("JDBC CLIENT must not be a transaction")
	}
	if TransactionIdentityEligible("CLIENT", "GET", gzip) {
		t.Fatal("HTTP CLIENT must not be a transaction")
	}
	if TransactionIdentityEligible("PRODUCER", "publish", map[string]string{"messaging.system": "kafka"}) {
		t.Fatal("PRODUCER must not be a transaction")
	}
	if !TransactionIdentityEligible("CONSUMER", "process", map[string]string{"messaging.system": "kafka"}) {
		t.Fatal("CONSUMER must be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "UserRepository.findByEmail", jdbc) {
		t.Fatal("datastore INTERNAL must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "housekeeper", hikari) {
		t.Fatal("Hikari INTERNAL must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "SELECT 1", map[string]string{"db.statement": "SELECT 1"}) {
		t.Fatal("ORM INTERNAL with a statement must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "SELECT", map[string]string{"db.operation": "SELECT"}) {
		t.Fatal("ORM INTERNAL with db.operation must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "", map[string]string{}) {
		t.Fatal("empty INTERNAL must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "tcp.connect", map[string]string{}) {
		t.Fatal("tcp.connect must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", "Create Nest App", map[string]string{}) {
		t.Fatal("Nest bootstrap must not be a transaction")
	}
	if !TransactionIdentityEligible("INTERNAL", "DailySweepJob.processDueSweeps", map[string]string{}) {
		t.Fatal("cron INTERNAL must be a transaction")
	}
	if !TransactionIdentityEligible("INTERNAL", "billing.nightly", map[string]string{}) {
		t.Fatal("named INTERNAL cron must be a transaction")
	}
}

func TestRequestIdentityEligible(t *testing.T) {
	httpGET := map[string]string{"http.request.method": "GET", "url.path": "/users"}
	if !RequestIdentityEligible("SERVER", "GET /users", httpGET) {
		t.Fatal("HTTP SERVER must count as a request")
	}
	if !RequestIdentityEligible("CONSUMER", "process", nil) {
		t.Fatal("CONSUMER must count as a request")
	}
	if RequestIdentityEligible("INTERNAL", "job", nil) {
		t.Fatal("INTERNAL must not count as incoming throughput")
	}
	if RequestIdentityEligible("CLIENT", "query", map[string]string{"db.system": "postgresql"}) {
		t.Fatal("CLIENT must not count as a request")
	}
	health := map[string]string{"http.request.method": "GET", "url.path": "/healthz"}
	if RequestIdentityEligible("SERVER", "GET /healthz", health) {
		t.Fatal("probe SERVER must not count as incoming throughput")
	}
	if TransactionIdentityEligible("SERVER", "GET /healthz", health) {
		// healthz remains searchable; it is excluded from RED and default lists
	} else {
		t.Fatal("probe SERVER must remain a transaction so it can be searched")
	}
}

func TestCHTransactionIdentityEligibleMirrorsGo(t *testing.T) {
	sql := CHTransactionIdentityEligible("kind", "tags", "dep_kind", "operation_name")
	if !strings.Contains(sql, "upperUTF8(kind) = 'SERVER'") {
		t.Fatalf("missing SERVER entrypoint gate:\n%s", sql)
	}
	if !strings.Contains(sql, "CONSUMER") {
		t.Fatalf("missing CONSUMER entrypoint:\n%s", sql)
	}
	if !strings.Contains(sql, "INTERNAL") {
		t.Fatalf("missing INTERNAL entrypoint:\n%s", sql)
	}
	if !strings.Contains(sql, "database") {
		t.Fatalf("missing datastore exclusion:\n%s", sql)
	}
	if !strings.Contains(sql, "db.statement") {
		t.Fatalf("missing ORM statement exclusion:\n%s", sql)
	}
	if !strings.Contains(sql, "tcp.connect") {
		t.Fatalf("missing network INTERNAL exclusion:\n%s", sql)
	}
	if strings.Contains(sql, "healthz") {
		t.Fatalf("transaction identity must not hide probes; list filters do that:\n%s", sql)
	}
	if strings.Contains(sql, "upperUTF8(kind) != 'SERVER'") && !strings.Contains(sql, "upperUTF8(kind) = 'SERVER'") {
		t.Fatalf("CLIENT must not pass solely because it is not SERVER:\n%s", sql)
	}
}

func TestCHRequestIdentityEligibleExcludesProbes(t *testing.T) {
	sql := CHRequestIdentityEligible("kind", "tags", "operation_name")
	if !strings.Contains(sql, "healthz") {
		t.Fatalf("request identity must exclude probes:\n%s", sql)
	}
}
