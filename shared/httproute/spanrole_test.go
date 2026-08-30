package httproute

import "testing"

func TestIsNetworkSpan(t *testing.T) {
	for _, name := range []string{"dns.lookup", "tcp.connect", "tls.connect"} {
		if !IsNetworkSpan(name) {
			t.Errorf("%s should be a network span", name)
		}
	}
	if IsNetworkSpan("GET /users") {
		t.Fatal("HTTP route is not a network span")
	}
}

func TestIsProbe(t *testing.T) {
	if !IsProbe("GET /healthz", map[string]string{"url.path": "/healthz"}) {
		t.Fatal("expected /healthz probe")
	}
	if !IsProbe("GET", map[string]string{"http.route": "/actuator/health"}) {
		t.Fatal("expected actuator health probe")
	}
	if !IsProbe("GET /api/health/readiness", map[string]string{"url.path": "/api/health/readiness"}) {
		t.Fatal("expected nested readiness probe")
	}
	if IsProbe("GET /users", map[string]string{"url.path": "/users"}) {
		t.Fatal("business route is not a probe")
	}
}

func TestIsStreaming(t *testing.T) {
	if !IsStreaming("/trading.OrderGateway/StreamExecutions", nil) {
		t.Fatal("expected gRPC stream")
	}
	if IsStreaming("GET /upstream", map[string]string{"url.path": "/upstream"}) {
		t.Fatal("upstream must not count as a stream")
	}
}

func TestInternalEntrypointEligible(t *testing.T) {
	if !InternalEntrypointEligible("DailySweepJob.processDueSweeps", nil) {
		t.Fatal("job must be an entrypoint")
	}
	if InternalEntrypointEligible("tcp.connect", nil) {
		t.Fatal("tcp.connect must not be an entrypoint")
	}
	if InternalEntrypointEligible("MerchantSettlementConfigRepository.findAll", nil) {
		t.Fatal("repository method must not be an entrypoint")
	}
	if InternalEntrypointEligible("SELECT az.foodcard.api.settlement.entity.Merchant", nil) {
		t.Fatal("SQL-shaped INTERNAL must not be an entrypoint")
	}
}
