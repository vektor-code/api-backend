package alerts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNotifier_PostsWebhookJSON(t *testing.T) {
	var gotBody string
	var gotURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(204)
	}))
	defer server.Close()

	n := NewNotifier(server.Client())
	err := n.Notify(context.Background(), Channel{Type: ChannelWebhook, Target: server.URL + "/hook"}, ActiveAlert{
		RuleName:  "high-errors",
		Service:   "api",
		Namespace: "prod",
		Severity:  SeverityCritical,
		Value:     12.5,
		Condition: "Error Rate > 2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotURL != "/hook" {
		t.Fatalf("url path = %s", gotURL)
	}
	if !strings.Contains(gotBody, "high-errors") || !strings.Contains(gotBody, "12.5") {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestNotifier_RejectsNonHTTPTarget(t *testing.T) {
	n := NewNotifier(http.DefaultClient)
	err := n.Notify(context.Background(), Channel{Type: ChannelWebhook, Target: "javascript:alert(1)"}, ActiveAlert{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNotifier_PropagatesNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()
	n := NewNotifier(server.Client())
	err := n.Notify(context.Background(), Channel{Type: ChannelSlack, Target: server.URL}, ActiveAlert{RuleName: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMemoryStore_CRUD(t *testing.T) {
	s := NewMemoryStore()
	rule, err := s.UpsertRule(Rule{Name: "r", Metric: MetricErrorRate, Operator: "gt", Threshold: 1, Active: true})
	if err != nil {
		t.Fatal(err)
	}
	if rule.ID == "" || rule.Operator != ">" {
		t.Fatalf("rule = %+v", rule)
	}
	if len(s.ListRules()) != 1 {
		t.Fatal("expected 1 rule")
	}
	ch, err := s.UpsertChannel(Channel{Name: "hook", Type: ChannelWebhook, Target: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.ChannelByID(ch.ID); !ok {
		t.Fatal("missing channel")
	}
	s.ReplaceActive([]ActiveAlert{{ID: "a1", FiredAt: time.Now()}})
	if len(s.ListActive()) != 1 {
		t.Fatal("active")
	}
	if err := s.DeleteRule(rule.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteChannel(ch.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRule("missing"); err == nil {
		t.Fatal("expected missing rule error")
	}
}
