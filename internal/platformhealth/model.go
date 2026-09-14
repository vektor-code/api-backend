package platformhealth

import (
	"regexp"
	"strings"
	"time"
)

type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
	SeverityInfo     Severity = "info"
	SeverityOK       Severity = "ok"
)

type Issue struct {
	Severity Severity  `json:"severity"`
	Source   string    `json:"source"` // status|event|log|container
	Code     string    `json:"code,omitempty"`
	Message  string    `json:"message"`
	Detail   string    `json:"detail,omitempty"`
	At       time.Time `json:"at,omitempty"`
}

type ContainerReport struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restartCount"`
	State        string `json:"state"` // running|waiting|terminated|unknown
	Reason       string `json:"reason,omitempty"`
	Message      string `json:"message,omitempty"`
	ExitCode     *int32 `json:"exitCode,omitempty"`
	StartedAt    string `json:"startedAt,omitempty"`
	FinishedAt   string `json:"finishedAt,omitempty"`
}

type EventReport struct {
	Type      string    `json:"type"`
	Reason    string    `json:"reason"`
	Message   string    `json:"message"`
	Count     int32     `json:"count"`
	FirstSeen time.Time `json:"firstSeen,omitempty"`
	LastSeen  time.Time `json:"lastSeen,omitempty"`
}

type LogLine struct {
	Severity Severity `json:"severity"`
	Line     string   `json:"line"`
}

type PodReport struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Cluster           string            `json:"cluster,omitempty"`
	Component         string            `json:"component"`
	Phase             string            `json:"phase"`
	NodeName          string            `json:"nodeName,omitempty"`
	Ready             bool              `json:"ready"`
	Restarts          int32             `json:"restarts"`
	Status            Severity          `json:"status"`
	CreatedAt         time.Time         `json:"createdAt,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Containers        []ContainerReport `json:"containers"`
	InitContainers    []ContainerReport `json:"initContainers,omitempty"`
	Conditions        []string          `json:"conditions,omitempty"`
	Events            []EventReport     `json:"events"`
	Issues            []Issue           `json:"issues"`
	LogErrors         []LogLine         `json:"logErrors"`
	RecentLogs        []string          `json:"recentLogs"`
	PreviousLogErrors []LogLine         `json:"previousLogErrors,omitempty"`
	LogFetchError     string            `json:"logFetchError,omitempty"`
}

type ComponentReport struct {
	ID       string      `json:"id"`
	Status   Severity    `json:"status"`
	PodCount int         `json:"podCount"`
	Pods     []PodReport `json:"pods"`
	Issues   []Issue     `json:"issues"`
}

type Summary struct {
	Status   Severity `json:"status"`
	Critical int      `json:"critical"`
	Warning  int      `json:"warning"`
	Info     int      `json:"info"`
	Healthy  int      `json:"healthy"`
	Pods     int      `json:"pods"`
}

type Report struct {
	Namespace   string            `json:"namespace"`
	Cluster     string            `json:"cluster,omitempty"`
	GeneratedAt time.Time         `json:"generatedAt"`
	Summary     Summary           `json:"summary"`
	Components  []ComponentReport `json:"components"`
	Notes       []string          `json:"notes,omitempty"`
	Sources     []string          `json:"sources,omitempty"`
	Clusters    []ClusterHealth   `json:"clusters,omitempty"`
}

var (
	errorLineRe = regexp.MustCompile(`(?i)\b(error|fatal|panic|oom|crash|failed|failure|refused|unavailable|timeout|deadline|denied|unauthorized|forbidden)\b`)
	warnLineRe  = regexp.MustCompile(`(?i)\b(warn|warning|retry|degraded|slow|backoff)\b`)
)

// ComponentIDFromPodName maps a pod name to an APM platform component id.
func ComponentIDFromPodName(name string) string {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "api-backend"):
		return "api-backend"
	case strings.Contains(n, "agent-backend"):
		return "agent-backend"
	case strings.Contains(n, "ingestor-backend"):
		return "ingestor-backend"
	case strings.Contains(n, "app-frontend"):
		return "app-frontend"
	case strings.Contains(n, "opentelemetry-operator") || strings.Contains(n, "otel-operator"):
		return "opentelemetry-operator"
	default:
		return ""
	}
}

// ClassifyLogLine returns severity for a single log line, or empty if uninteresting.
func ClassifyLogLine(line string) Severity {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return ""
	}
	if errorLineRe.MatchString(trimmed) {
		return SeverityCritical
	}
	if warnLineRe.MatchString(trimmed) {
		return SeverityWarning
	}
	return ""
}

// ScanLogLines extracts error/warning lines and keeps a short recent tail.
func ScanLogLines(raw string, recentLimit, issueLimit int) (recent []string, issues []LogLine) {
	if recentLimit <= 0 {
		recentLimit = 40
	}
	if issueLimit <= 0 {
		issueLimit = 80
	}
	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		recent = append(recent, line)
		if sev := ClassifyLogLine(line); sev != "" {
			issues = append(issues, LogLine{Severity: sev, Line: line})
		}
	}
	if len(recent) > recentLimit {
		recent = recent[len(recent)-recentLimit:]
	}
	if len(issues) > issueLimit {
		issues = issues[len(issues)-issueLimit:]
	}
	return recent, issues
}

// IssuesFromContainer builds issues from container state.
func IssuesFromContainer(c ContainerReport) []Issue {
	var out []Issue
	switch strings.ToLower(c.State) {
	case "waiting":
		sev := SeverityWarning
		code := c.Reason
		switch strings.ToLower(c.Reason) {
		case "crashloopbackoff", "imagepullbackoff", "errimagepull", "invalidimageName", "createcontainererror", "createcontainerconfigerror":
			sev = SeverityCritical
		}
		out = append(out, Issue{
			Severity: sev,
			Source:   "container",
			Code:     code,
			Message:  c.Name + " waiting: " + orDefault(c.Reason, "Waiting"),
			Detail:   c.Message,
		})
	case "terminated":
		if c.ExitCode != nil && *c.ExitCode != 0 {
			sev := SeverityCritical
			if strings.EqualFold(c.Reason, "Completed") {
				sev = SeverityInfo
			}
			out = append(out, Issue{
				Severity: sev,
				Source:   "container",
				Code:     orDefault(c.Reason, "Terminated"),
				Message:  c.Name + " terminated with exit " + itoa32(*c.ExitCode),
				Detail:   c.Message,
			})
		}
		if strings.EqualFold(c.Reason, "OOMKilled") {
			out = append(out, Issue{
				Severity: SeverityCritical,
				Source:   "container",
				Code:     "OOMKilled",
				Message:  c.Name + " was OOMKilled",
				Detail:   c.Message,
			})
		}
	}
	if c.RestartCount >= 5 {
		out = append(out, Issue{
			Severity: SeverityWarning,
			Source:   "container",
			Code:     "HighRestarts",
			Message:  c.Name + " restart count is high",
			Detail:   "restarts=" + itoa32(c.RestartCount),
		})
	} else if c.RestartCount >= 1 && !c.Ready {
		out = append(out, Issue{
			Severity: SeverityWarning,
			Source:   "container",
			Code:     "Restarts",
			Message:  c.Name + " has restarted and is not ready",
			Detail:   "restarts=" + itoa32(c.RestartCount),
		})
	}
	return out
}

// RollupSeverity picks the worst severity among issues.
func RollupSeverity(issues []Issue) Severity {
	worst := SeverityOK
	rank := map[Severity]int{
		SeverityOK:       0,
		SeverityInfo:     1,
		SeverityWarning:  2,
		SeverityCritical: 3,
	}
	for _, issue := range issues {
		if rank[issue.Severity] > rank[worst] {
			worst = issue.Severity
		}
	}
	return worst
}

func BuildSummary(components []ComponentReport) Summary {
	sum := Summary{Status: SeverityOK}
	for _, comp := range components {
		for _, pod := range comp.Pods {
			sum.Pods++
			switch pod.Status {
			case SeverityCritical:
				sum.Critical++
			case SeverityWarning:
				sum.Warning++
			case SeverityInfo:
				sum.Info++
			default:
				sum.Healthy++
			}
		}
		if rankSeverity(comp.Status) > rankSeverity(sum.Status) {
			sum.Status = comp.Status
		}
	}
	return sum
}

func rankSeverity(s Severity) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

func orDefault(v, d string) string {
	if strings.TrimSpace(v) == "" {
		return d
	}
	return v
}

func itoa32(v int32) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [12]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// KnownComponentIDs is the ordered list shown in the Admin UI.
func KnownComponentIDs() []string {
	return []string{
		"api-backend",
		"agent-backend",
		"ingestor-backend",
		"app-frontend",
		"opentelemetry-operator",
	}
}
