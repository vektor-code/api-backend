package api

import (
	"fmt"
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
)

type DiagnosticReport struct {
	TraceID              string    `json:"traceId"`
	RootCauseSpanID      string    `json:"rootCauseSpanId,omitempty"`
	RootCauseService     string    `json:"rootCauseService,omitempty"`
	RootCauseMessage     string    `json:"rootCauseMessage,omitempty"`
	BottleneckSpanID     string    `json:"bottleneckSpanId"`
	BottleneckService    string    `json:"bottleneckService"`
	BottleneckDurationMs float64   `json:"bottleneckDurationMs"`
	BottleneckPercent    float64   `json:"bottleneckPercent"`
	Summary              string    `json:"summary"`
	Issues               []string  `json:"issues"`
	Remediations         []string  `json:"remediations"`
}

type DatabaseQueryMetric struct {
	Query         string  `json:"query"`
	System        string  `json:"system"`
	Service       string  `json:"service"`
	Namespace     string  `json:"namespace"`
	CallCount     int64   `json:"callCount"`
	ErrorCount    int64   `json:"errorCount"`
	ErrorRate     float64 `json:"errorRate"`
	AvgDurationMs float64 `json:"avgDurationMs"`
	MaxDurationMs float64 `json:"maxDurationMs"`
}

// AnalyzeTrace parses the span structure to isolate the root error and CPU/wait bottleneck
func AnalyzeTrace(trace *models.Trace) *DiagnosticReport {
	// Reconstruct span hierarchy
	children := make(map[string][]*models.Span)
	spanMap := make(map[string]*models.Span)
	for _, sp := range trace.Spans {
		spanMap[sp.SpanID] = sp
		if sp.ParentSpanID != "" && sp.ParentSpanID != "0" && sp.ParentSpanID != "0000000000000000" {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp)
		}
	}

	// 1. Find Root Cause Error (deepest failing span in the call tree)
	var rootCauseSpan *models.Span
	maxDepth := -1

	var walkForError func(span *models.Span, depth int)
	walkForError = func(span *models.Span, depth int) {
		if span.Status == models.SpanStatusError {
			if depth > maxDepth {
				maxDepth = depth
				rootCauseSpan = span
			}
		}
		for _, child := range children[span.SpanID] {
			walkForError(child, depth+1)
		}
	}

	if trace.RootSpan != nil {
		walkForError(trace.RootSpan, 0)
	} else if len(trace.Spans) > 0 {
		for _, sp := range trace.Spans {
			if sp.Status == models.SpanStatusError {
				rootCauseSpan = sp
				break
			}
		}
	}

	// 2. Find Latency Bottleneck (highest self-execution duration = duration - sum of direct child durations)
	var bottleneckSpan *models.Span
	maxSelfDuration := 0.0

	for _, sp := range trace.Spans {
		childDurationSum := 0.0
		for _, child := range children[sp.SpanID] {
			childDurationSum += child.DurationMs
		}
		selfDuration := sp.DurationMs - childDurationSum
		if selfDuration < 0 {
			selfDuration = 0
		}
		if selfDuration > maxSelfDuration {
			maxSelfDuration = selfDuration
			bottleneckSpan = sp
		}
	}

	if bottleneckSpan == nil && len(trace.Spans) > 0 {
		bottleneckSpan = trace.Spans[0]
		maxSelfDuration = bottleneckSpan.DurationMs
	}

	// 3. Assemble Report
	report := &DiagnosticReport{
		TraceID: trace.TraceID,
	}

	if rootCauseSpan != nil {
		report.RootCauseSpanID = rootCauseSpan.SpanID
		report.RootCauseService = rootCauseSpan.ServiceName
		report.RootCauseMessage = rootCauseSpan.Error
		if report.RootCauseMessage == "" {
			report.RootCauseMessage = rootCauseSpan.Attributes["error.message"]
		}
		if report.RootCauseMessage == "" {
			report.RootCauseMessage = "Unknown execution error occurred"
		}
	}

	if bottleneckSpan != nil {
		report.BottleneckSpanID = bottleneckSpan.SpanID
		report.BottleneckService = bottleneckSpan.ServiceName
		report.BottleneckDurationMs = maxSelfDuration
		if trace.DurationMs > 0 {
			report.BottleneckPercent = (maxSelfDuration / trace.DurationMs) * 100.0
		}
	}

	// Generate summary & dynamic recommendations (Davis AI style)
	var summaryParts []string
	var issues []string
	var remediations []string

	if rootCauseSpan != nil {
		summaryParts = append(summaryParts, fmt.Sprintf("Vektor Davis AI isolated the root failure to service '%s' (Span ID: %s) with error: '%s'.", rootCauseSpan.ServiceName, rootCauseSpan.SpanID, report.RootCauseMessage))
		issues = append(issues, fmt.Sprintf("Error in service '%s': %s", rootCauseSpan.ServiceName, report.RootCauseMessage))
		
		msgLower := strings.ToLower(report.RootCauseMessage)
		if strings.Contains(msgLower, "timeout") || strings.Contains(msgLower, "deadline exceeded") {
			remediations = append(remediations, fmt.Sprintf("Adjust execution timeout configurations in service '%s'.", rootCauseSpan.ServiceName))
			remediations = append(remediations, "Verify database resource constraints (connection pools, read-write locks).")
		} else if strings.Contains(msgLower, "connection refused") || strings.Contains(msgLower, "dial tcp") {
			remediations = append(remediations, fmt.Sprintf("Service '%s' failed to reach its target. Check service discovery, DNS records, and network ingress policies.", rootCauseSpan.ServiceName))
		} else {
			remediations = append(remediations, fmt.Sprintf("Inspect application logs for pod '%s' inside service '%s'.", rootCauseSpan.PodName, rootCauseSpan.ServiceName))
		}
	}

	if bottleneckSpan != nil && report.BottleneckPercent > 10.0 {
		summaryParts = append(summaryParts, fmt.Sprintf("Service '%s' was the primary performance bottleneck, accounting for %.1f%% (%.2fms) of the total trace duration.", bottleneckSpan.ServiceName, report.BottleneckPercent, maxSelfDuration))
		issues = append(issues, fmt.Sprintf("High self-execution delay in service '%s': %.2fms spent executing code or waiting for un-instrumented resources.", bottleneckSpan.ServiceName, maxSelfDuration))
		
		if dbSystem, ok := bottleneckSpan.Attributes["db.system"]; ok {
			dbStmt := bottleneckSpan.Attributes["db.statement"]
			issues = append(issues, fmt.Sprintf("Slow database query in '%s': '%s'", dbSystem, dbStmt))
			remediations = append(remediations, fmt.Sprintf("Optimize SQL query in '%s'. Inspect database execution plans, add indexes, or introduce caching layers.", dbSystem))
		} else {
			remediations = append(remediations, fmt.Sprintf("Review CPU limit throttling or profiling for service '%s' to optimize computational performance.", bottleneckSpan.ServiceName))
		}
	}

	if len(summaryParts) == 0 {
		report.Summary = "Trace processed successfully. Vektor Davis AI detected no errors or performance anomalies."
		remediations = append(remediations, "No action required. Transaction execution is within healthy parameters.")
	} else {
		report.Summary = strings.Join(summaryParts, " Additionally, ")
	}

	report.Issues = issues
	report.Remediations = remediations

	return report
}
