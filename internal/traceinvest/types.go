package traceinvest

import "time"

const (
	StatusSkipped     = "skipped"
	StatusPending     = "pending"
	StatusComplete    = "complete"
	StatusPartial     = "partial"
	StatusUnavailable = "unavailable"
	StatusRateLimited = "rate_limited"
	StatusExpired     = "expired"
)

const (
	TypeDownstreamHTTPFailure = "downstream_http_failure"
	TypeNetworkTimeout        = "network_timeout"
	TypeClientError           = "client_error"
)

const (
	CheckPodStatus          = "pod_status"
	CheckServiceResolution  = "service_resolution"
	CheckEndpointHealth     = "endpoint_health"
	CheckEvents             = "events"
	CheckNetworkPolicy      = "network_policy"
	CheckHTTPRequest        = "http_request"
)

const (
	KindObserved  = "observed"
	KindInference = "inference"
)

const (
	jobTTL    = 2 * time.Minute
	resultTTL = 30 * time.Second
	claimTTL  = 45 * time.Second
)

// Intent is the structured investigation request the API may submit.
// It never includes a command, argv, or kubectl payload. The agent decides
// how (and whether) to perform each allowlisted check.
type Intent struct {
	InvestigationType string    `json:"investigationType"`
	ClusterID         string    `json:"clusterId"`
	Namespace         string    `json:"namespace"`
	SourceWorkload    string    `json:"sourceWorkload"`
	SourcePod         string    `json:"sourcePod,omitempty"`
	Destination       string    `json:"destination"`
	DestinationURL    string    `json:"destinationUrl,omitempty"`
	DestinationType   string    `json:"destinationType,omitempty"`
	RecordedHTTP      int       `json:"recordedHttp,omitempty"`
	Checks            []string  `json:"checks"`
	MaxLevel          int       `json:"maxLevel"`
	TraceID           string    `json:"traceId"`
	Fingerprint       string    `json:"fingerprint"`
	ExpiresAt         time.Time `json:"expiresAt"`
}

// Observation is one fact. KindObserved is something the agent saw.
// KindInference is a conclusion drawn from those facts.
type Observation struct {
	Kind    string `json:"kind"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Level   int    `json:"level,omitempty"`
	OK      *bool  `json:"ok,omitempty"`
	Pod     string `json:"pod,omitempty"`
}

// Check is a UI-facing projection of an observed fact. Kept so existing
// clients continue to render Kubernetes verification rows.
type Check struct {
	Level  int    `json:"level"`
	Code   string `json:"code"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	Pod    string `json:"pod,omitempty"`
	Cached bool   `json:"cached,omitempty"`
}

// Report is what the UI reads. GetTrace never waits for this.
type Report struct {
	TraceID       string         `json:"traceId"`
	Status        string         `json:"status"`
	LevelReached  int            `json:"levelReached"`
	SkipReason    string         `json:"skipReason,omitempty"`
	Conclusion    string         `json:"conclusion,omitempty"`
	Inference     string         `json:"inference,omitempty"`
	OriginalState string         `json:"originalState,omitempty"`
	CurrentState  string         `json:"currentState,omitempty"`
	Confidence    string         `json:"confidence,omitempty"`
	Observations  []Observation  `json:"observations,omitempty"`
	Checks        []Check        `json:"checks,omitempty"`
	Cached        bool           `json:"cached,omitempty"`
	CacheKey      string         `json:"cacheKey,omitempty"`
	Fingerprint   string         `json:"fingerprint,omitempty"`
	DurationMs    int64          `json:"durationMs,omitempty"`
	ReferencedBy  int            `json:"referencedBy,omitempty"`
}

// Result is what the agent posts back after executing allowlisted checks.
type Result struct {
	Fingerprint  string         `json:"fingerprint"`
	TraceID      string         `json:"traceId,omitempty"`
	Status       string         `json:"status"`
	LevelReached int            `json:"levelReached"`
	SkipReason   string         `json:"skipReason,omitempty"`
	Inference    string         `json:"inference,omitempty"`
	OriginalState string        `json:"originalState,omitempty"`
	CurrentState  string        `json:"currentState,omitempty"`
	Confidence   string         `json:"confidence,omitempty"`
	Observations []Observation  `json:"observations,omitempty"`
	DurationMs   int64          `json:"durationMs,omitempty"`
}
