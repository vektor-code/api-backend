package store

import (
	"testing"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestSpanAcceptsDatabaseHints(t *testing.T) {
	httpGET := map[string]string{"http.request.method": "GET", "url.path": "/apis/apps/v1/namespaces/x"}
	if spanAcceptsDatabaseHints("GET", models.SpanKindClient, httpGET) {
		t.Fatal("HTTP CLIENT must not receive pod database hints")
	}
	if spanAcceptsDatabaseHints("dns.lookup", models.SpanKindClient, map[string]string{"net.peer.name": "172.16.45.15"}) {
		t.Fatal("dns.lookup must not receive pod database hints")
	}
	if !spanAcceptsDatabaseHints("SELECT users", models.SpanKindClient, map[string]string{"db.system": "postgresql"}) {
		t.Fatal("JDBC CLIENT must receive pod database hints")
	}
}
