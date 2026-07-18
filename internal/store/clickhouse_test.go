package store

import (
	"strings"
	"testing"
)

func TestChTraceIDPredicateUsesEqualityForFullHexID(t *testing.T) {
	predicate := chTraceIDPredicate("A1234567890ABCDEF1234567890ABCDE")
	if !strings.Contains(predicate, "trace_id =") {
		t.Fatalf("expected exact trace_id predicate, got %s", predicate)
	}
	if strings.Contains(predicate, "positionCaseInsensitive") {
		t.Fatalf("did not expect substring predicate for full trace id: %s", predicate)
	}
}

func TestChTraceIDPredicateKeepsPartialSearch(t *testing.T) {
	predicate := chTraceIDPredicate("abc123")
	if !strings.Contains(predicate, "positionCaseInsensitive") {
		t.Fatalf("expected substring predicate for partial trace id, got %s", predicate)
	}
}
