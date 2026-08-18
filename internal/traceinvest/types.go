package traceinvest

import "time"

const (
	StatusSkipped     = "skipped"
	StatusComplete    = "complete"
	StatusPartial     = "partial"
	StatusUnavailable = "unavailable"
	StatusRateLimited = "rate_limited"
)

// Check is one live observation. It never replaces the original span.
type Check struct {
	Level  int    `json:"level"`
	Code   string `json:"code"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	Pod    string `json:"pod,omitempty"`
	Cached bool   `json:"cached,omitempty"`
}

// Report is the active-investigator result. GetTrace never waits for this.
type Report struct {
	TraceID      string  `json:"traceId"`
	Status       string  `json:"status"`
	LevelReached int     `json:"levelReached"`
	SkipReason   string  `json:"skipReason,omitempty"`
	Conclusion   string  `json:"conclusion,omitempty"`
	Checks       []Check `json:"checks,omitempty"`
	Cached       bool    `json:"cached,omitempty"`
	CacheKey     string  `json:"cacheKey,omitempty"`
	DurationMs   int64   `json:"durationMs,omitempty"`
}

// Limits cap concurrent live work. One failing service must not storm itself.
type Limits struct {
	Global      int
	Namespace   int
	Workload    int
	Destination int
}

func DefaultLimits() Limits {
	return Limits{Global: 10, Namespace: 3, Workload: 1, Destination: 1}
}

func DefaultTTL() time.Duration {
	return 20 * time.Second
}
