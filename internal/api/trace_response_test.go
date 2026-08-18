package api

import (
	"encoding/json"
	"testing"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

func TestTraceResponseJSONIncludesDiagnosisWithoutMutatingTrace(t *testing.T) {
	tr := &models.Trace{
		TraceID:     "abc",
		ServiceName: "gtm-preview",
		Spans:       []*models.Span{},
	}
	diag := &tracediag.Diagnosis{
		TraceID:        "abc",
		Classification: tracediag.ClassificationApplicationError,
		Title:          "HTTP 503 Service Unavailable",
		Confidence:     tracediag.ConfidenceHigh,
	}
	b, err := json.Marshal(traceResponse{Trace: tr, FailureDiagnosis: diag})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["traceId"] != "abc" {
		t.Fatalf("traceId missing in %s", b)
	}
	if m["serviceName"] != "gtm-preview" {
		t.Fatalf("serviceName missing in %s", b)
	}
	raw, ok := m["failureDiagnosis"].(map[string]any)
	if !ok {
		t.Fatalf("failureDiagnosis missing in %s", b)
	}
	if raw["classification"] != "APPLICATION_ERROR" {
		t.Fatalf("classification=%v", raw["classification"])
	}
}
