// Package tracediag is a deterministic Trace Failure Analyzer.
//
// It reads a reconstructed trace and produces a diagnosis explaining why the
// trace or a span appears failed or anomalous. It never mutates spans,
// attributes, status, timestamps, or IDs.
package tracediag

import "time"

// Classification is the primary explanation for a failed or anomalous trace.
type Classification string

const (
	ClassificationApplicationError         Classification = "APPLICATION_ERROR"
	ClassificationClientError              Classification = "CLIENT_ERROR"
	ClassificationDownstreamError          Classification = "DOWNSTREAM_ERROR"
	ClassificationNetworkError             Classification = "NETWORK_ERROR"
	ClassificationTimeout                  Classification = "TIMEOUT"
	ClassificationInstrumentationAnomaly   Classification = "INSTRUMENTATION_ANOMALY"
	ClassificationTraceContextAnomaly      Classification = "TRACE_CONTEXT_ANOMALY"
	ClassificationDuplicateInstrumentation Classification = "DUPLICATE_INSTRUMENTATION"
	ClassificationUnknown                  Classification = "UNKNOWN"
)

// Confidence is a coarse band derived from ConfidenceScore.
type Confidence string

const (
	ConfidenceLow    Confidence = "LOW"
	ConfidenceMedium Confidence = "MEDIUM"
	ConfidenceHigh   Confidence = "HIGH"
)

// Severity is the operational impact of the diagnosis, independent of how
// sure we are.
type Severity string

const (
	SeverityLow    Severity = "LOW"
	SeverityMedium Severity = "MEDIUM"
	SeverityHigh   Severity = "HIGH"
)

// Evidence is one transparent fact that contributed to the diagnosis.
type Evidence struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	SpanID  string `json:"spanId,omitempty"`
	Score   int    `json:"score,omitempty"`
}

// Diagnosis is a derived representation. It is never written back onto spans.
type Diagnosis struct {
	TraceID         string         `json:"traceId"`
	Classification  Classification `json:"classification"`
	Severity        Severity       `json:"severity"`
	Confidence      Confidence     `json:"confidence"`
	ConfidenceScore int            `json:"confidenceScore"`
	Title           string         `json:"title"`
	Summary         string         `json:"summary"`
	Evidence        []Evidence     `json:"evidence"`
	LikelyCauses    []string       `json:"likelyCauses"`
	AffectedSpanIDs []string       `json:"affectedSpanIds"`
	Rules           []string       `json:"rules,omitempty"`
	// Live is a planning hint only. It never waits on Kubernetes.
	Live *LivePlan `json:"live,omitempty"`
}

// LivePlan says whether an active Kubernetes investigation would add evidence.
// MaxLevel is 0 (telemetry only), 1 (API inspect), 2 (existing-pod exec),
// or 3 (reusable diagnostic worker). The analyzer never creates a pod.
type LivePlan struct {
	Recommended bool   `json:"recommended"`
	MaxLevel    int    `json:"maxLevel"`
	Reason      string `json:"reason"`
}

// Options supplies optional operator-known facts. Timeouts are never assumed.
type Options struct {
	// KnownTimeouts maps service name or span ID to a configured timeout.
	// Used only when the value is provided; the analyzer does not invent it.
	KnownTimeouts map[string]time.Duration
}
