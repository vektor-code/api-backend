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
	Error        string            `json:"error,omitempty"`
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
}

// TraceListItem is a lightweight summary for list views
type TraceListItem struct {
	TraceID         string    `json:"traceId"`
	ServiceName     string    `json:"serviceName"`
	Namespace       string    `json:"namespace"`
	Namespaces      []string  `json:"namespaces,omitempty"` // all namespaces the trace crosses, in flow order
	Cluster         string    `json:"cluster,omitempty"`
	RootName        string    `json:"rootName"`
	StartTime       time.Time `json:"startTime"`
	DurationMs      float64   `json:"durationMs"`
	SpanCount       int       `json:"spanCount"`
	HasError        bool      `json:"hasError"`
	Services        []string  `json:"services"`
	ThirdPartyTools []string  `json:"thirdPartyTools,omitempty"`
	ServiceFlow     []string  `json:"serviceFlow,omitempty"`
	ErrorType       string    `json:"errorType,omitempty"`
	ErrorSummary    string    `json:"errorSummary,omitempty"`
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
	ServiceName   string  `json:"serviceName"`
	OperationName string  `json:"operationName"`
	Count         int64   `json:"count"`
	ErrorCount    int64   `json:"errorCount"`
	AvgDurationMs float64 `json:"avgDurationMs"`
	P95DurationMs float64 `json:"p95DurationMs"`
}

// LiveSpan is sent over WebSocket for real-time streaming
type LiveSpan struct {
	Type string `json:"type"`
	Data *Span  `json:"data"`
}
