package store

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPurgeAllTracesCountsClickHouseRows(t *testing.T) {
	var truncated bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		q := string(body)
		switch {
		case strings.Contains(q, "count()"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"c": 1284}},
			})
		case strings.Contains(strings.ToUpper(q), "TRUNCATE"):
			truncated = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected query %q", q)
		}
	}))
	t.Cleanup(srv.Close)

	s := &Store{chURL: srv.URL, chMode: true}
	n, err := s.PurgeAllTraces()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1284 {
		t.Fatalf("deletedCount = %d, want 1284 ClickHouse rows", n)
	}
	if !truncated {
		t.Fatal("expected TRUNCATE TABLE kubetrace.spans")
	}
}

func TestPurgeAllTracesFailsWhenTruncateFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		q := string(body)
		if strings.Contains(q, "count()") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"c": 10}},
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	t.Cleanup(srv.Close)

	s := &Store{chURL: srv.URL, chMode: true}
	if _, err := s.PurgeAllTraces(); err == nil {
		t.Fatal("expected truncate failure")
	}
}

func TestPurgeAllTracesWithoutClickHouseOrMinio(t *testing.T) {
	s := &Store{}
	n, err := s.PurgeAllTraces()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deletedCount = %d, want 0", n)
	}
}
