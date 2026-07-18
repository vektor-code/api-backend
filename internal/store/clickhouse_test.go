package store

import (
	"strings"
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
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

func TestChQueryBoundsUsesRetentionWindow(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 720}

	start, end := s.chQueryBounds(&models.SearchQuery{}, now)
	if want := now.Add(-720 * time.Hour); !start.Equal(want) {
		t.Fatalf("expected start %s, got %s", want, start)
	}
	if want := now.Add(5 * time.Minute); !end.Equal(want) {
		t.Fatalf("expected end %s, got %s", want, end)
	}
}

func TestChQueryBoundsClampsExplicitStartToRetention(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 24}

	start, _ := s.chQueryBounds(&models.SearchQuery{
		StartTime: now.Add(-72 * time.Hour),
		EndTime:   now,
	}, now)
	if want := now.Add(-24 * time.Hour); !start.Equal(want) {
		t.Fatalf("expected retained start %s, got %s", want, start)
	}
}

func TestChQueryBoundsKeepsForeverRetentionBoundedByDefault(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	s := &Store{retentionHours: 0}

	start, _ := s.chQueryBounds(&models.SearchQuery{}, now)
	if want := now.Add(-time.Duration(chDefaultForeverQueryHours) * time.Hour); !start.Equal(want) {
		t.Fatalf("expected bounded default start %s, got %s", want, start)
	}
}
