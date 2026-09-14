package alerts

import (
	"strings"
	"time"
)

type Metric string

const (
	MetricErrorRate  Metric = "Error Rate"
	MetricP95Latency Metric = "p95 Latency"
	MetricP99Latency Metric = "p99 Latency"
)

type Severity string

const (
	SeverityCritical Severity = "Critical"
	SeverityWarning  Severity = "Warning"
	SeverityInfo     Severity = "Info"
)

type ChannelType string

const (
	ChannelWebhook   ChannelType = "Webhook"
	ChannelSlack     ChannelType = "Slack"
	ChannelEmail     ChannelType = "Email"
	ChannelPagerDuty ChannelType = "PagerDuty"
)

type Rule struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	Service   string    `json:"service"`
	Metric    Metric    `json:"metric"`
	Operator  string    `json:"operator"` // ">" or ">="
	Threshold float64   `json:"threshold"`
	Window    string    `json:"window"`
	Severity  Severity  `json:"severity"`
	Active    bool      `json:"active"`
	Channels  []string  `json:"channels"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Channel struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Type      ChannelType `json:"type"`
	Target    string      `json:"target"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

type ActiveAlert struct {
	ID        string    `json:"id"`
	RuleID    string    `json:"ruleId"`
	RuleName  string    `json:"ruleName"`
	Service   string    `json:"service"`
	Namespace string    `json:"namespace"`
	Metric    Metric    `json:"metric"`
	Condition string    `json:"condition"`
	Value     float64   `json:"value"`
	Severity  Severity  `json:"severity"`
	Status    string    `json:"status"` // Firing
	FiredAt   time.Time `json:"firedAt"`
}

// ServiceSample is the minimal metric surface needed to evaluate rules.
type ServiceSample struct {
	ServiceName string
	Namespace   string
	ErrorRate   float64 // 0–100 percent
	P95Ms       float64
	P99Ms       float64
}

func NormalizeOperator(op string) string {
	op = strings.TrimSpace(op)
	switch op {
	case ">=", "gt=", "gte":
		return ">="
	case ">", "gt", "":
		return ">"
	default:
		return op
	}
}

func Compare(op string, value, threshold float64) bool {
	switch NormalizeOperator(op) {
	case ">=":
		return value >= threshold
	case ">":
		return value > threshold
	default:
		return false
	}
}

func SampleValue(s ServiceSample, metric Metric) (float64, bool) {
	switch metric {
	case MetricErrorRate:
		return s.ErrorRate, true
	case MetricP95Latency:
		return s.P95Ms, true
	case MetricP99Latency:
		return s.P99Ms, true
	default:
		return 0, false
	}
}

func RuleMatchesService(rule Rule, sample ServiceSample) bool {
	if rule.Namespace != "" && rule.Namespace != sample.Namespace {
		return false
	}
	if rule.Service != "" && !strings.EqualFold(rule.Service, "all") && rule.Service != sample.ServiceName {
		return false
	}
	return true
}
