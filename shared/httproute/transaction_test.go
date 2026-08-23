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

	if !TransactionIdentityEligible("SERVER", httpGET) {
		t.Fatal("HTTP SERVER must be a transaction")
	}
	if TransactionIdentityEligible("SERVER", gzip) {
		t.Fatal("malformed HTTP SERVER must not be a transaction")
	}
	if !TransactionIdentityEligible("SERVER", rpc) {
		t.Fatal("RPC SERVER must remain a transaction")
	}
	if TransactionIdentityEligible("CLIENT", jdbc) {
		t.Fatal("JDBC CLIENT must not be a transaction")
	}
	if TransactionIdentityEligible("CLIENT", gzip) {
		t.Fatal("HTTP CLIENT must not be a transaction")
	}
	if TransactionIdentityEligible("PRODUCER", map[string]string{"messaging.system": "kafka"}) {
		t.Fatal("PRODUCER must not be a transaction")
	}
	if !TransactionIdentityEligible("CONSUMER", map[string]string{"messaging.system": "kafka"}) {
		t.Fatal("CONSUMER must be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", jdbc) {
		t.Fatal("datastore INTERNAL must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", hikari) {
		t.Fatal("Hikari INTERNAL must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", map[string]string{"db.statement": "SELECT 1"}) {
		t.Fatal("ORM INTERNAL with a statement must not be a transaction")
	}
	if TransactionIdentityEligible("INTERNAL", map[string]string{"db.operation": "SELECT"}) {
		t.Fatal("ORM INTERNAL with db.operation must not be a transaction")
	}
	if !TransactionIdentityEligible("INTERNAL", map[string]string{}) {
		t.Fatal("non-datastore INTERNAL must be a transaction")
	}
}

func TestRequestIdentityEligible(t *testing.T) {
	httpGET := map[string]string{"http.request.method": "GET", "url.path": "/users"}
	if !RequestIdentityEligible("SERVER", httpGET) {
		t.Fatal("HTTP SERVER must count as a request")
	}
	if !RequestIdentityEligible("CONSUMER", nil) {
		t.Fatal("CONSUMER must count as a request")
	}
	if RequestIdentityEligible("INTERNAL", nil) {
		t.Fatal("INTERNAL must not count as incoming throughput")
	}
	if RequestIdentityEligible("CLIENT", map[string]string{"db.system": "postgresql"}) {
		t.Fatal("CLIENT must not count as a request")
	}
}

func TestCHTransactionIdentityEligibleMirrorsGo(t *testing.T) {
	sql := CHTransactionIdentityEligible("kind", "tags", "dep_kind")
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
	if strings.Contains(sql, "upperUTF8(kind) != 'SERVER'") && !strings.Contains(sql, "upperUTF8(kind) = 'SERVER'") {
		t.Fatalf("CLIENT must not pass solely because it is not SERVER:\n%s", sql)
	}
}
