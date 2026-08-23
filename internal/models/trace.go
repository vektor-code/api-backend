package models

import "time"

// Span represents a single unit of work in a distributed trace
type Span struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId,omitempty"`
	Name         string            `json:"name"`
	ServiceName  string            `json:"serviceName"`
	Namespace    string            `json:"namespace"`
	Cluster      string            `json:"cluster,omitempty"`
	PodName      string            `json:"podName,omitempty"`
	NodeName     string            `json:"nodeName,omitempty"`
	StartTime    time.Time         `json:"startTime"`
	EndTime      time.Time         `json:"endTime"`
	DurationMs   float64           `json:"durationMs"`
	Status       SpanStatus        `json:"status"`
	StatusCode   int               `json:"statusCode,omitempty"`
	Kind         SpanKind          `json:"kind"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []SpanEvent       `json:"events,omitempty"`
	Links        []SpanLink        `json:"links,omitempty"`
	Error        string            `json:"error,omitempty"`
}

type SpanLink struct {
	TraceID    string            `json:"traceId,omitempty"`
	SpanID     string            `json:"spanId,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type SpanStatus string

const (
	SpanStatusOK    SpanStatus = "OK"
	SpanStatusError SpanStatus = "ERROR"
	SpanStatusUnset SpanStatus = "UNSET"
)

type SpanKind string

const (
	SpanKindServer   SpanKind = "SERVER"
	SpanKindClient   SpanKind = "CLIENT"
	SpanKindProducer SpanKind = "PRODUCER"
	SpanKindConsumer SpanKind = "CONSUMER"
	SpanKindInternal SpanKind = "INTERNAL"
)

type SpanEvent struct {
	Name       string            `json:"name"`
	Timestamp  time.Time         `json:"timestamp"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Trace is a collection of spans sharing a traceId
type Trace struct {
	TraceID     string    `json:"traceId"`
	RootSpan    *Span     `json:"rootSpan"`
	Spans       []*Span   `json:"spans"`
	Namespace   string    `json:"namespace"`
	Cluster     string    `json:"cluster,omitempty"`
	ServiceName string    `json:"serviceName"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	DurationMs  float64   `json:"durationMs"`
	SpanCount   int       `json:"spanCount"`
	HasError    bool      `json:"hasError"`
	// Partial is set when the trace's real root span was never stored, so what
	// is shown begins part-way through the request.
	Partial bool `json:"partial,omitempty"`
}

// TraceListItem is a lightweight summary for list views
type TraceListItem struct {
	TraceID     string   `json:"traceId"`
	ServiceName string   `json:"serviceName"`
	Namespace   string   `json:"namespace"`
	Namespaces  []string `json:"namespaces,omitempty"` // all namespaces the trace crosses, in flow order
	Cluster     string   `json:"cluster,omitempty"`
	RootName    string   `json:"rootName"`
	// TransactionName is RootName reduced to a stable endpoint identity, used
	// for grouping. RootName stays as the SDK wrote it, for display.
	TransactionName string    `json:"transactionName,omitempty"`
	StartTime       time.Time `json:"startTime"`
	DurationMs      float64   `json:"durationMs"`
	SpanCount       int       `json:"spanCount"`
	HasError        bool      `json:"hasError"`
	Services        []string  `json:"services"`
	ThirdPartyTools []string  `json:"thirdPartyTools,omitempty"`
	ServiceFlow     []string  `json:"serviceFlow,omitempty"`
	ErrorType       string    `json:"errorType,omitempty"`
	ErrorSummary    string    `json:"errorSummary,omitempty"`
	// Partial is set when the trace's real root span was never stored.
	Partial bool `json:"partial,omitempty"`
}

// ServiceStats holds aggregated metrics per service
type ServiceStats struct {
	ServiceName      string    `json:"serviceName"`
	Namespace        string    `json:"namespace"`
	Cluster          string    `json:"cluster,omitempty"`
	RequestCount     int64     `json:"requestCount"`
	ErrorCount       int64     `json:"errorCount"`
	ErrorRate        float64   `json:"errorRate"`
	P50Ms            float64   `json:"p50Ms"`
	P95Ms            float64   `json:"p95Ms"`
	P99Ms            float64   `json:"p99Ms"`
	HealthScore      float64   `json:"healthScore,omitempty"`
	Apdex            float64   `json:"apdex,omitempty"`
	Status           string    `json:"status,omitempty"`
	LastSeen         time.Time `json:"lastSeen"`
	IsInfrastructure bool      `json:"isInfrastructure"`
	Language         string    `json:"language,omitempty"`
}

// NamespaceStats holds aggregated metrics per namespace
type NamespaceStats struct {
	Namespace     string         `json:"namespace"`
	Cluster       string         `json:"cluster,omitempty"`
	TraceCount    int64          `json:"traceCount"`
	ErrorCount    int64          `json:"errorCount"`
	ErrorRate     float64        `json:"errorRate"`
	AvgDurationMs float64        `json:"avgDurationMs"`
	Services      []ServiceStats `json:"services"`
	PodCount      int            `json:"podCount"`
	LastActivity  time.Time      `json:"lastActivity"`
}

// ServiceEdge represents a dependency between two services
type ServiceEdge struct {
	Source          string  `json:"source"`
	Target          string  `json:"target"`
	SourceNamespace string  `json:"sourceNamespace,omitempty"`
	TargetNamespace string  `json:"targetNamespace,omitempty"`
	CallCount       int64   `json:"callCount"`
	ErrorCount      int64   `json:"errorCount"`
	AvgDurationMs   float64 `json:"avgDurationMs"`
}

// ServiceMapData is the full service dependency graph
type ServiceMapData struct {
	Namespace string         `json:"namespace"`
	Nodes     []ServiceStats `json:"nodes"`
	Edges     []ServiceEdge  `json:"edges"`
}

// SearchQuery represents trace search parameters
type SearchQuery struct {
	Namespace     string    `json:"namespace"`
	Cluster       string    `json:"cluster"`
	ServiceName   string    `json:"serviceName"`
	Operation     string    `json:"operation"`
	TraceID       string    `json:"traceId"`
	MinSpans      int       `json:"minSpans"`
	HasError      *bool     `json:"hasError"`
	MinDurationMs float64   `json:"minDurationMs"`
	MaxDurationMs float64   `json:"maxDurationMs"`
	StartTime     time.Time `json:"startTime"`
	EndTime       time.Time `json:"endTime"`
	Limit         int       `json:"limit"`
	Offset        int       `json:"offset"`
}

// EndpointStat is a stable per-endpoint (root service + root operation)
// aggregation over the whole query window, used by the Traces "Top traces"
// view so counts and latencies don't jitter between refreshes.
type EndpointStat struct {
	ServiceName   string `json:"serviceName"`
	Namespace     string `json:"namespace,omitempty"`
	OperationName string `json:"operationName"`
	// Count is weighted by the sampling factor, so it estimates real traffic.
	Count         int64   `json:"count"`
	ErrorCount    int64   `json:"errorCount"`
	AvgDurationMs float64 `json:"avgDurationMs"`
	P95DurationMs float64 `json:"p95DurationMs"`
	// SampledCount is how many traces were actually stored. When it is below
	// Count, sampling was active and Count is an estimate.
	SampledCount int64 `json:"sampledCount"`
}

// LiveSpan is sent over WebSocket for real-time streaming
type LiveSpan struct {
	Type string `json:"type"`
	Data *Span  `json:"data"`
}

// DatabaseQueryMetric aggregates database spans that share a query shape.
//
// Rows are keyed by Fingerprint — a hash of the query with literals removed —
// rather than by raw statement text, so "WHERE id = 1" and "WHERE id = 2"
// aggregate into one row instead of two.
type DatabaseQueryMetric struct {
	Fingerprint  string `json:"fingerprint"`
	Query        string `json:"query"`
	Summary      string `json:"summary,omitempty"`
	System       string `json:"system"`
	Operation    string `json:"operation,omitempty"`
	Collection   string `json:"collection,omitempty"`
	DatabaseName string `json:"databaseName,omitempty"`
	Service      string `json:"service"`
	Namespace    string `json:"namespace"`

	CallCount  int64   `json:"callCount"`
	ErrorCount int64   `json:"errorCount"`
	ErrorRate  float64 `json:"errorRate"`

	AvgDurationMs float64 `json:"avgDurationMs"`
	P95DurationMs float64 `json:"p95DurationMs"`
	P99DurationMs float64 `json:"p99DurationMs"`
	MaxDurationMs float64 `json:"maxDurationMs"`
	// TotalDurationMs is call count times average latency. It is the ranking
	// that matters operationally: a fast query run constantly costs more than a
	// slow query run twice.
	TotalDurationMs float64 `json:"totalDurationMs"`

	RecentErrors []string `json:"recentErrors"`
}
