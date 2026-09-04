package store

import (
	"encoding/json"
	"testing"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestKafkaMessagesByTraceKeysEachTrace(t *testing.T) {
	batch := []*models.Span{
		{TraceID: "aaa", SpanID: "1"},
		{TraceID: "bbb", SpanID: "2"},
		{TraceID: "aaa", SpanID: "3"},
		nil,
	}
	msgs := kafkaMessagesByTrace(batch)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (one per trace)", len(msgs))
	}
	if string(msgs[0].Key) != "aaa" || string(msgs[1].Key) != "bbb" {
		t.Fatalf("keys = %q %q", msgs[0].Key, msgs[1].Key)
	}
	var first []*models.Span
	if err := json.Unmarshal(msgs[0].Value, &first); err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("trace aaa should carry 2 spans, got %d", len(first))
	}
}
