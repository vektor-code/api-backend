package tracediag

import (
	"fmt"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/spantree"
)

const (
	scoreEmptyMethod      = 20
	scoreEmptyPath        = 15
	scoreInvalidStatus    = 25
	scoreServerVsChildren = 20
	scoreChildClientOK    = 10
	scoreExceedsTimeout   = 10
	scoreUnexplainedLong  = 15

	lifecycleMinParentMs = 1000.0
	lifecycleRatio       = 20.0
	unexplainedLongMs    = 10000.0
	timingSkewMs         = 5.0
	childOutsideMs       = 50.0
)

type spanTree struct {
	trace    *models.Trace
	byID     map[string]*models.Span
	children map[string][]*models.Span
	opts     Options
}

type finding struct {
	classification Classification
	score          int
	title          string
	summary        string
	evidence       []Evidence
	causes         []string
	spanIDs        []string
	rules          []string
	priority       int
}

// Analyze inspects a reconstructed trace and returns a diagnosis, or nil when
// there is nothing failed or anomalous to explain. The input trace is not
// modified.
func Analyze(trace *models.Trace, opts Options) *Diagnosis {
	if trace == nil || len(trace.Spans) == 0 {
		return nil
	}

	tree := buildTree(trace, opts)
	findings := make([]finding, 0, 8)
	findings = append(findings, ruleInstrumentation(tree)...)
	findings = append(findings, ruleTransport(tree)...)
	findings = append(findings, ruleTimeout(tree)...)
	findings = append(findings, ruleRPC(tree)...)
	findings = append(findings, ruleDatabase(tree)...)
	findings = append(findings, ruleMessaging(tree)...)
	findings = append(findings, ruleHTTPOutcome(tree)...)
	findings = append(findings, ruleDuplicates(tree)...)
	findings = append(findings, ruleTraceContext(tree)...)

	if len(findings) == 0 {
		if unexplainedError(tree) {
			return unknownDiagnosis(trace)
		}
		return nil
	}
	return toDiagnosis(trace, pickPrimary(findings))
}

func buildTree(trace *models.Trace, opts Options) spanTree {
	byID := make(map[string]*models.Span, len(trace.Spans))
	children := make(map[string][]*models.Span, len(trace.Spans))
	for _, sp := range trace.Spans {
		if sp == nil || sp.SpanID == "" {
			continue
		}
		byID[spantree.Normalize(sp.SpanID)] = sp
	}
	for _, sp := range trace.Spans {
		if sp == nil {
			continue
		}
		if spantree.IsRoot(sp.ParentSpanID) {
			continue
		}
		parentKey := spantree.Normalize(sp.ParentSpanID)
		if _, ok := byID[parentKey]; ok {
			children[parentKey] = append(children[parentKey], sp)
		}
	}
	return spanTree{trace: trace, byID: byID, children: children, opts: opts}
}

func pickPrimary(findings []finding) finding {
	best := findings[0]
	for _, f := range findings[1:] {
		if f.score > best.score || (f.score == best.score && f.priority > best.priority) {
			best = f
		}
	}
	return best
}

func toDiagnosis(trace *models.Trace, f finding) *Diagnosis {
	score := f.score
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	d := &Diagnosis{
		TraceID:         trace.TraceID,
		Classification:  f.classification,
		Severity:        severityFor(f),
		Confidence:      confidenceFor(score),
		ConfidenceScore: score,
		Title:           f.title,
		Summary:         f.summary,
		Evidence:        f.evidence,
		LikelyCauses:    f.causes,
		AffectedSpanIDs: uniqueIDs(f.spanIDs),
		Rules:           uniqueStrings(f.rules),
	}
	attachSpanTree(trace, d)
	plan := LivePlanFor(d)
	d.Live = &plan
	return d
}

func attachSpanTree(trace *models.Trace, d *Diagnosis) {
	if d == nil || trace == nil {
		return
	}
	missing := missingParentCount(trace)
	if missing == 0 {
		d.SpanTree = "complete"
		d.Evidence = append(d.Evidence, Evidence{
			Code:    "span_tree_complete",
			Message: "All captured spans have their parent in this trace.",
		})
		return
	}
	d.SpanTree = "broken"
	if !hasEvidenceCode(d.Evidence, "missing_parent") {
		d.Evidence = append(d.Evidence, Evidence{
			Code:    "missing_parent",
			Message: fmt.Sprintf("%d span(s) reference a parent that was not captured.", missing),
		})
	}
}

func missingParentCount(trace *models.Trace) int {
	if trace == nil {
		return 0
	}
	return len(spantree.Build(trace.Spans).MidTreeMissing)
}

func confidenceFor(score int) Confidence {
	switch {
	case score >= 70:
		return ConfidenceHigh
	case score >= 40:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

func severityFor(f finding) Severity {
	switch f.classification {
	case ClassificationApplicationError, ClassificationDownstreamError, ClassificationNetworkError, ClassificationTimeout:
		return SeverityHigh
	case ClassificationInstrumentationAnomaly:
		if f.score >= 70 {
			return SeverityHigh
		}
		if f.score >= 40 {
			return SeverityMedium
		}
		return SeverityLow
	case ClassificationClientError, ClassificationDuplicateInstrumentation, ClassificationTraceContextAnomaly:
		return SeverityMedium
	default:
		return SeverityLow
	}
}

func unknownDiagnosis(trace *models.Trace) *Diagnosis {
	ids := make([]string, 0, 4)
	for _, sp := range trace.Spans {
		if sp != nil && sp.Status == models.SpanStatusError {
			ids = append(ids, sp.SpanID)
		}
	}
	d := &Diagnosis{
		TraceID:         trace.TraceID,
		Classification:  ClassificationUnknown,
		Severity:        SeverityLow,
		Confidence:      ConfidenceLow,
		ConfidenceScore: 15,
		Title:           "Unexplained span failure",
		Summary:         "A span is marked failed, but the trace does not contain enough consistent evidence to explain why.",
		Evidence: []Evidence{{
			Code:    "insufficient_evidence",
			Message: "The failing span has no reliable HTTP, transport, or exception metadata.",
			Score:   15,
		}},
		LikelyCauses:    []string{"The instrumentation did not attach a usable error description.", "The failure may be internal to the service without exported details."},
		AffectedSpanIDs: ids,
		Rules:           []string{"unknown_insufficient_evidence"},
	}
	attachSpanTree(trace, d)
	plan := LivePlanFor(d)
	d.Live = &plan
	return d
}

func unexplainedError(tree spanTree) bool {
	for _, sp := range tree.trace.Spans {
		if sp != nil && sp.Status == models.SpanStatusError {
			return true
		}
	}
	return false
}

func ruleInstrumentation(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil || sp.Kind != models.SpanKindServer {
			continue
		}
		if !isHTTPSpan(sp) {
			continue
		}
		f, ok := instrumentationFinding(tree, sp)
		if ok {
			out = append(out, f)
		}
	}
	return out
}

func instrumentationFinding(tree spanTree, server *models.Span) (finding, bool) {
	var evidence []Evidence
	score := 0
	rules := make([]string, 0, 6)
	methodRaw, methodPresent := httpMethodRaw(server)
	status, statusPresent := httpStatus(server)
	path := httpPath(server)
	malformed := 0

	if !validHTTPMethod(server) && httpExpectedMethod(server, methodPresent, statusPresent, path) {
		msg := "SERVER HTTP method is empty"
		if strings.TrimSpace(methodRaw) != "" {
			msg = fmt.Sprintf("SERVER HTTP method %q is not a valid HTTP token", methodRaw)
		}
		evidence = append(evidence, Evidence{Code: "empty_http_method", Message: msg, SpanID: server.SpanID, Score: scoreEmptyMethod})
		score += scoreEmptyMethod
		malformed++
		rules = append(rules, "invalid_http_metadata")
	}

	httpExpected := isHTTPSpan(server)
	if httpExpected && !validHTTPPath(server) {
		evidence = append(evidence, Evidence{Code: "empty_url_path", Message: "SERVER URL path is empty where HTTP metadata is expected", SpanID: server.SpanID, Score: scoreEmptyPath})
		score += scoreEmptyPath
		malformed++
		rules = append(rules, "invalid_http_metadata")
	}

	if statusPresent && !validHTTPStatus(status) {
		evidence = append(evidence, Evidence{
			Code:    "invalid_http_status",
			Message: fmt.Sprintf("SERVER response status %d is not a valid HTTP status", status),
			SpanID:  server.SpanID,
			Score:   scoreInvalidStatus,
		})
		score += scoreInvalidStatus
		malformed++
		rules = append(rules, "invalid_http_metadata")
	}

	children := tree.children[spantree.Normalize(server.SpanID)]
	maxChild := 0.0
	var fastOKClient *models.Span
	explainingChild := false
	for _, ch := range children {
		if ch.DurationMs > maxChild {
			maxChild = ch.DurationMs
		}
		if ch.DurationMs >= server.DurationMs*0.10 {
			explainingChild = true
		}
		if ch.Kind == models.SpanKindClient {
			chStatus, chHasStatus := httpStatus(ch)
			if chHasStatus && chStatus >= 200 && chStatus < 400 && ch.DurationMs > 0 && ch.DurationMs*lifecycleRatio < server.DurationMs {
				fastOKClient = ch
			}
		}
	}

	lifecycle := false
	if server.DurationMs >= lifecycleMinParentMs && maxChild > 0 && !explainingChild &&
		server.DurationMs >= maxChild*lifecycleRatio && (server.DurationMs-maxChild) >= 1000 {
		lifecycle = true
		evidence = append(evidence, Evidence{
			Code:    "server_longer_than_children",
			Message: fmt.Sprintf("SERVER duration %s is far longer than its child spans (longest child %s)", formatDuration(server.DurationMs), formatDuration(maxChild)),
			SpanID:  server.SpanID,
			Score:   scoreServerVsChildren,
		})
		score += scoreServerVsChildren
		rules = append(rules, "parent_child_lifecycle")
		if server.DurationMs-maxChild >= unexplainedLongMs {
			evidence = append(evidence, Evidence{
				Code:    "unexplained_server_duration",
				Message: fmt.Sprintf("About %s of SERVER time is not explained by any child span", formatDuration(server.DurationMs-maxChild)),
				SpanID:  server.SpanID,
				Score:   scoreUnexplainedLong,
			})
			score += scoreUnexplainedLong
		}
	}

	if fastOKClient != nil {
		method := attr(fastOKClient, "http.request.method", "http.method")
		cpath := httpPath(fastOKClient)
		op := strings.TrimSpace(method + " " + cpath)
		if op == "" {
			op = fastOKClient.Name
		}
		st, _ := httpStatus(fastOKClient)
		evidence = append(evidence, Evidence{
			Code:    "child_client_succeeded",
			Message: fmt.Sprintf("child CLIENT %s completed in %s and returned HTTP %d", strings.TrimSpace(op), formatDuration(fastOKClient.DurationMs), st),
			SpanID:  fastOKClient.SpanID,
			Score:   scoreChildClientOK,
		})
		score += scoreChildClientOK
		rules = append(rules, "malformed_server_successful_client")
	}

	if d, ok := spanTimeout(server, tree.opts); ok {
		limitMs := float64(d) / float64(time.Millisecond)
		if server.DurationMs > limitMs && (lifecycle || malformed >= 2) {
			evidence = append(evidence, Evidence{
				Code:    "duration_exceeds_known_timeout",
				Message: fmt.Sprintf("SERVER exceeded configured %s proxy timeout", d),
				SpanID:  server.SpanID,
				Score:   scoreExceedsTimeout,
			})
			score += scoreExceedsTimeout
			rules = append(rules, "timeout_inconsistency")
		}
	}

	if score == 0 {
		return finding{}, false
	}

	// A legitimate 5xx with valid method/path/status and a reasonable duration
	// is an application failure, not instrumentation.
	if validHTTPMethod(server) && validHTTPPath(server) && statusPresent && validHTTPStatus(status) && status >= 400 && !lifecycle && malformed == 0 {
		return finding{}, false
	}
	if malformed == 0 && !lifecycle {
		return finding{}, false
	}
	// Weak single-signal instrumentation should not override a clean HTTP
	// application failure on a different span; this span still reports if
	// there is no competing application outcome. Credibility is enforced
	// at pick time via score/priority.
	if malformed == 0 && lifecycle && fastOKClient == nil && score < 40 {
		// Keep it: long SERVER vs short children is itself a lifecycle signal.
	}

	title := "Invalid HTTP span metadata"
	summary := "The SERVER span carries HTTP attributes that are internally inconsistent."
	if lifecycle {
		title = "HTTP SERVER span lifecycle inconsistency"
		childNote := "its child operations completed much earlier"
		if fastOKClient != nil {
			st, _ := httpStatus(fastOKClient)
			method := attr(fastOKClient, "http.request.method", "http.method")
			cpath := httpPath(fastOKClient)
			op := strings.TrimSpace(method + " " + cpath)
			if op == "" {
				op = fastOKClient.Name
			}
			childNote = fmt.Sprintf("its child CLIENT %s completed successfully in approximately %s", op, formatDuration(fastOKClient.DurationMs))
			_ = st
		}
		summary = fmt.Sprintf("The SERVER span remained open for %s even though %s.", formatDuration(server.DurationMs), childNote)
	}

	causes := []string{
		"The language agent's HTTP SERVER span closed with incomplete or inconsistent attributes.",
		"HTTP attribute extraction failed at span completion (empty method/path or a non-HTTP status).",
	}
	if malformed > 0 && lifecycle {
		causes = []string{
			"The SERVER span stayed open after the request work finished — a span lifecycle / instrumentation bug in the language agent, not necessarily a slow application.",
			"HTTP attribute extraction at span completion was incomplete or corrupted.",
		}
	}

	ids := []string{server.SpanID}
	if fastOKClient != nil {
		ids = append(ids, fastOKClient.SpanID)
	}
	if score > 100 {
		score = 100
	}
	return finding{
		classification: ClassificationInstrumentationAnomaly,
		score:          score,
		title:          title,
		summary:        summary,
		evidence:       evidence,
		causes:         causes,
		spanIDs:        ids,
		rules:          uniqueStrings(rules),
		priority:       100,
	}, true
}

func ruleTransport(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil {
			continue
		}
		text := errorText(sp)
		status, hasStatus := httpStatus(sp)
		reset := isResetText(text)
		refused := isRefusedText(text)
		clientZero := sp.Kind == models.SpanKindClient && hasStatus && !validHTTPStatus(status)
		missingResponse := sp.Kind == models.SpanKindClient && !hasStatus && (sp.Status == models.SpanStatusError || text != "") && isHTTPSpan(sp) && !isTimeoutText(text)

		if !reset && !refused && !clientZero && !missingResponse {
			continue
		}
		// Timeouts own the classification even when the CLIENT also has status 0.
		if isTimeoutText(text) && !reset && !refused {
			continue
		}

		score := 75
		title := "Transport failure"
		summary := fmt.Sprintf("%s failed to complete an HTTP request because the connection did not return a normal HTTP response.", sp.ServiceName)
		var evidence []Evidence
		var causes []string
		rules := []string{"transport_failure"}

		switch {
		case reset:
			title = "Connection reset"
			summary = fmt.Sprintf("%s had the connection reset before a complete HTTP response arrived.", sp.ServiceName)
			evidence = append(evidence, Evidence{Code: "connection_reset", Message: "The span reports a connection reset.", SpanID: sp.SpanID, Score: 40})
			causes = []string{"The peer closed the TCP connection (process crash, idle timeout, or LB reset).", "A proxy between the services reset the connection."}
		case refused:
			title = "Connection refused"
			summary = fmt.Sprintf("%s could not establish a connection to the target.", sp.ServiceName)
			evidence = append(evidence, Evidence{Code: "connection_refused", Message: "The span reports a connection refused or unreachable host.", SpanID: sp.SpanID, Score: 40})
			causes = []string{"The target is not listening on this host/port.", "DNS or NetworkPolicy is blocking the call."}
		default:
			title = "Missing HTTP response"
			summary = fmt.Sprintf("%s made a CLIENT call that ended without a valid HTTP status.", sp.ServiceName)
			causes = []string{"The request never received an HTTP response (transport-level failure).", "The instrumentation recorded status 0 because no HTTP status line was observed."}
		}
		if clientZero {
			evidence = append(evidence, Evidence{Code: "client_status_zero", Message: "CLIENT HTTP status is 0, which is not a valid HTTP response code.", SpanID: sp.SpanID, Score: 25})
			score += 10
		}
		if missingResponse && !clientZero {
			evidence = append(evidence, Evidence{Code: "missing_http_response", Message: "CLIENT span has no HTTP response status.", SpanID: sp.SpanID, Score: 20})
		}

		ids := []string{sp.SpanID}
		if parent, ok := tree.byID[spantree.Normalize(sp.ParentSpanID)]; ok && parent.Kind == models.SpanKindServer {
			if pst, okp := httpStatus(parent); okp && pst >= 500 {
				evidence = append(evidence, Evidence{
					Code:    "proxy_mapped_transport_failure",
					Message: fmt.Sprintf("Parent SERVER returned HTTP %d, which is consistent with a proxy mapping a transport failure rather than an application 5xx.", pst),
					SpanID:  parent.SpanID,
					Score:   10,
				})
				ids = append(ids, parent.SpanID)
				score += 10
			}
		}
		if score > 100 {
			score = 100
		}
		out = append(out, finding{
			classification: ClassificationNetworkError,
			score:          score,
			title:          title,
			summary:        summary,
			evidence:       evidence,
			causes:         causes,
			spanIDs:        ids,
			rules:          rules,
			priority:       90,
		})
	}
	return out
}

func ruleTimeout(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil {
			continue
		}
		text := errorText(sp)
		status, hasStatus := httpStatus(sp)
		httpTimeout := hasStatus && (status == 408 || status == 504)
		if !isTimeoutText(text) && !httpTimeout {
			continue
		}
		if isResetText(text) || isRefusedText(text) {
			continue
		}
		title := "Request timed out"
		if status == 504 {
			title = "HTTP 504 Gateway Timeout"
		} else if status == 408 {
			title = "HTTP 408 Request Timeout"
		}
		summary := fmt.Sprintf("%s did not receive a timely response%s.", sp.ServiceName, targetSuffix(sp))
		evidence := []Evidence{{
			Code:    "timeout",
			Message: fmt.Sprintf("Span duration %s matches a timeout failure.", formatDuration(sp.DurationMs)),
			SpanID:  sp.SpanID,
			Score:   50,
		}}
		if text != "" {
			evidence = append(evidence, Evidence{Code: "timeout_message", Message: "The span error text reports a timeout or deadline exceeded.", SpanID: sp.SpanID, Score: 25})
		}
		for _, ev := range connectFailureEvidence(tree, sp) {
			evidence = append(evidence, ev)
		}
		ids := []string{sp.SpanID}
		for _, ev := range evidence {
			if ev.SpanID != "" && ev.SpanID != sp.SpanID {
				ids = append(ids, ev.SpanID)
			}
		}
		score := 75
		if len(evidence) > 2 {
			score += 10
		}
		if score > 100 {
			score = 100
		}
		causes := []string{
			"The target did not answer within the caller or proxy timeout.",
			"The client timeout may be lower than the normal processing time of this operation.",
		}
		if hasEvidenceCode(evidence, "connect_failure") {
			causes = []string{
				"The client timed out while connecting or completing TLS to the destination.",
				"The destination may be unreachable, dropping packets, or too slow to accept the connection.",
			}
		}
		out = append(out, finding{
			classification: ClassificationTimeout,
			score:          score,
			title:          title,
			summary:        summary,
			evidence:       evidence,
			causes:         causes,
			spanIDs:        uniqueIDs(ids),
			rules:          []string{"timeout"},
			priority:       92,
		})
	}
	return out
}

func ruleHTTPOutcome(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil {
			continue
		}
		if sp.Kind != models.SpanKindServer && sp.Kind != models.SpanKindClient {
			continue
		}
		if !isHTTPSpan(sp) || !validHTTPMethod(sp) || !validHTTPPath(sp) {
			continue
		}
		status, ok := httpStatus(sp)
		if !ok || !validHTTPStatus(status) || status < 400 {
			continue
		}
		if status == 408 || status == 504 {
			continue
		}

		down := (*models.Span)(nil)
		if sp.Kind == models.SpanKindServer {
			down = downstream5xx(tree, sp)
		}
		method, _ := httpMethodRaw(sp)
		path := httpPath(sp)
		op := strings.TrimSpace(strings.ToUpper(strings.TrimSpace(method)) + " " + path)
		name := httpStatusName(status)
		title := fmt.Sprintf("HTTP %d", status)
		if name != "" {
			title = fmt.Sprintf("HTTP %d %s", status, name)
		}

		ids := []string{sp.SpanID}
		role := "SERVER"
		if sp.Kind == models.SpanKindClient {
			role = "CLIENT"
		}
		var evidence []Evidence
		evidence = append(evidence, Evidence{
			Code:    fmt.Sprintf("http_%d", status),
			Message: fmt.Sprintf("%s %s returned HTTP %d.", role, op, status),
			SpanID:  sp.SpanID,
			Score:   50,
		})
		if validHTTPMethod(sp) {
			evidence = append(evidence, Evidence{Code: "valid_http_method", Message: fmt.Sprintf("HTTP method %s is valid.", strings.TrimSpace(method)), SpanID: sp.SpanID, Score: 10})
		}
		if validHTTPPath(sp) {
			evidence = append(evidence, Evidence{Code: "valid_url_path", Message: fmt.Sprintf("URL path %s is present.", path), SpanID: sp.SpanID, Score: 10})
		}
		evidence = append(evidence, Evidence{Code: "reasonable_duration", Message: fmt.Sprintf("Duration %s is internally consistent with an HTTP response.", formatDuration(sp.DurationMs)), SpanID: sp.SpanID, Score: 10})

		excMsg := ""
		if status >= 500 {
			excMsg = exceptionMessage(sp)
			if excMsg != "" {
				evidence = append(evidence, Evidence{
					Code:    "exception_message",
					Message: truncateRunes(excMsg, 240),
					SpanID:  sp.SpanID,
					Score:   20,
				})
			}
		}

		if down != nil {
			dst, _ := httpStatus(down)
			dname := down.ServiceName
			if dname == "" {
				dname = "a downstream service"
			}
			evidence = append(evidence, Evidence{
				Code:    "downstream_5xx",
				Message: fmt.Sprintf("Downstream span %s returned HTTP %d.", strings.TrimSpace(dname+" "+down.Name), dst),
				SpanID:  down.SpanID,
				Score:   15,
			})
			ids = append(ids, down.SpanID)
			downSummary := fmt.Sprintf("Application request failed because downstream service returned HTTP %d.", dst)
			if downExc := exceptionMessage(down); downExc != "" {
				downSummary = fmt.Sprintf("Application request failed because downstream service returned HTTP %d: %s", dst, truncateRunes(downExc, 180))
			}
			out = append(out, finding{
				classification: ClassificationDownstreamError,
				score:          85,
				title:          title,
				summary:        downSummary,
				evidence:       evidence,
				causes: []string{
					fmt.Sprintf("A downstream service returned HTTP %d; this service propagated the failure.", dst),
					"Inspect the downstream service's own logs and traces for the application-level cause.",
				},
				spanIDs:  ids,
				rules:    []string{"downstream_5xx"},
				priority: 80,
			})
			continue
		}

		class := ClassificationApplicationError
		score := 80
		summary := fmt.Sprintf("The target service returned HTTP %d for %s.", status, op)
		causes := []string{fmt.Sprintf("The application itself returned HTTP %d.", status)}
		rules := []string{"http_5xx"}
		if sp.Kind == models.SpanKindClient && status >= 500 {
			class = ClassificationDownstreamError
			score = 82
			rules = []string{"client_http_5xx"}
			summary = fmt.Sprintf("%s received HTTP %d from a remote dependency for %s.", sp.ServiceName, status, op)
			causes = []string{
				fmt.Sprintf("A remote HTTP dependency returned %d; this is not a local application crash.", status),
				"Inspect that dependency's traces and logs around this timestamp.",
			}
		} else if status >= 400 && status < 500 {
			class = ClassificationClientError
			score = 70
			rules = []string{"http_4xx"}
			causes = []string{fmt.Sprintf("The server rejected the request with HTTP %d.", status)}
			summary = fmt.Sprintf("The target service returned HTTP %d for %s.", status, op)
		}

		if status >= 500 && excMsg != "" {
			clipped := truncateRunes(excMsg, 180)
			if sp.Kind == models.SpanKindClient {
				summary = fmt.Sprintf("%s received HTTP %d from a remote dependency for %s: %s", sp.ServiceName, status, op, clipped)
			} else {
				summary = fmt.Sprintf("The target service returned HTTP %d for %s: %s", status, op, clipped)
			}
			causes = append([]string{clipped}, causes...)
		}

		out = append(out, finding{
			classification: class,
			score:          score,
			title:          title,
			summary:        summary,
			evidence:       evidence,
			causes:         causes,
			spanIDs:        ids,
			rules:          rules,
			priority:       70,
		})
	}
	return out
}

func ruleRPC(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil || !isRPCSpan(sp) {
			continue
		}
		code, hasCode := grpcStatus(sp)
		if hasCode && code == 0 {
			continue
		}
		text := errorText(sp)
		if !hasCode && !failedSpan(sp) {
			continue
		}
		if isHTTPSpan(sp) && !hasCode {
			continue
		}
		name := grpcStatusName(code)
		title := "RPC failure"
		if name != "" {
			title = "gRPC " + name
		} else if hasCode {
			title = fmt.Sprintf("gRPC status %d", code)
		}
		class := ClassificationApplicationError
		score := 78
		priority := 80
		rules := []string{"rpc_failure"}
		causes := []string{"The remote RPC/gRPC handler returned a non-OK status.", "Inspect that service's traces for the application-level cause."}
		switch {
		case code == 4 || code == 1 || isTimeoutText(text):
			class = ClassificationTimeout
			score = 86
			priority = 92
			rules = []string{"rpc_timeout"}
			if title == "RPC failure" {
				title = "RPC deadline exceeded"
			}
			causes = []string{"The RPC did not complete before the caller or server deadline.", "The callee may be slow, overloaded, or blocked on its own downstreams."}
		case code == 14 || isResetText(text) || isRefusedText(text):
			class = ClassificationNetworkError
			score = 84
			priority = 90
			rules = []string{"rpc_unavailable"}
			if title == "RPC failure" {
				title = "RPC unavailable"
			}
			causes = []string{"The RPC endpoint was unavailable (not listening, no healthy backends, or a transport failure).", "Check the destination workload, Service, and NetworkPolicy."}
		case code == 3 || code == 5 || code == 6 || code == 7 || code == 8 || code == 9 || code == 16:
			class = ClassificationClientError
			score = 72
			priority = 70
			rules = []string{"rpc_client_error"}
			causes = []string{"The RPC was rejected as an invalid, unauthorized, or not-found request.", "Fix the caller arguments, metadata, or destination method."}
		}
		svc := attr(sp, "rpc.service")
		method := attr(sp, "rpc.method")
		op := strings.TrimSpace(svc + "/" + method)
		if op == "/" {
			op = sp.Name
		}
		summary := fmt.Sprintf("%s RPC %s failed", sp.ServiceName, op)
		if name != "" {
			summary = fmt.Sprintf("%s RPC %s failed with %s.", sp.ServiceName, op, name)
		}
		evidence := []Evidence{{
			Code:    "rpc_status",
			Message: fmt.Sprintf("RPC status %d (%s) on %s.", code, name, op),
			SpanID:  sp.SpanID,
			Score:   40,
		}}
		if sys := attr(sp, "rpc.system"); sys != "" {
			evidence = append(evidence, Evidence{Code: "rpc_system", Message: "RPC system is " + sys + ".", SpanID: sp.SpanID, Score: 10})
		}
		out = append(out, finding{
			classification: class,
			score:          score,
			title:          title,
			summary:        summary,
			evidence:       evidence,
			causes:         causes,
			spanIDs:        []string{sp.SpanID},
			rules:          rules,
			priority:       priority,
		})
	}
	return out
}

func ruleDatabase(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil || !isDBSpan(sp) || !failedSpan(sp) {
			continue
		}
		text := errorText(sp)
		sys := dbSystem(sp)
		if sys == "" {
			sys = "database"
		}
		title := "Database error"
		class := ClassificationApplicationError
		score := 76
		priority := 75
		rules := []string{"database_error"}
		causes := []string{
			"The query failed (syntax, missing object, constraint, or permission) — see the reported error.",
			"The database may be unreachable or its connection pool exhausted.",
		}
		switch {
		case isTimeoutText(text):
			class = ClassificationTimeout
			score = 84
			priority = 92
			title = "Database timeout"
			rules = []string{"database_timeout"}
			causes = []string{"The database did not answer within the client or statement timeout.", "A lock, sequential scan, or saturated connection pool can produce this."}
		case isResetText(text) || isRefusedText(text) || containsAny(text, "too many connections", "remaining connection slots", "connection pool", "could not translate host"):
			class = ClassificationNetworkError
			score = 83
			priority = 90
			title = "Database connection failed"
			rules = []string{"database_connection"}
			causes = []string{"The process could not open or keep a connection to the database.", "Check host/port, NetworkPolicy, and whether the database pods are Ready."}
		}
		summary := fmt.Sprintf("%s failed a %s operation.", sp.ServiceName, sys)
		evidence := []Evidence{{
			Code:    "db_system",
			Message: "Database system is " + sys + ".",
			SpanID:  sp.SpanID,
			Score:   20,
		}}
		if stmt := attr(sp, "db.statement", "db.query.text"); stmt != "" {
			if len(stmt) > 180 {
				stmt = stmt[:180] + "…"
			}
			evidence = append(evidence, Evidence{Code: "db_statement", Message: "Query: " + stmt, SpanID: sp.SpanID, Score: 15})
		}
		out = append(out, finding{
			classification: class,
			score:          score,
			title:          title,
			summary:        summary,
			evidence:       evidence,
			causes:         causes,
			spanIDs:        []string{sp.SpanID},
			rules:          rules,
			priority:       priority,
		})
	}
	return out
}

func ruleMessaging(tree spanTree) []finding {
	var out []finding
	for _, sp := range tree.trace.Spans {
		if sp == nil || !isMessagingSpan(sp) || !failedSpan(sp) {
			continue
		}
		text := errorText(sp)
		sys := attr(sp, "messaging.system")
		if sys == "" {
			sys = "message broker"
		}
		title := "Messaging error"
		class := ClassificationApplicationError
		score := 74
		priority := 75
		rules := []string{"messaging_error"}
		causes := []string{"Publish or consume failed — the topic/queue may be missing or the payload rejected.", "The broker may be unreachable."}
		switch {
		case isTimeoutText(text):
			class = ClassificationTimeout
			score = 82
			priority = 92
			title = "Messaging timeout"
			rules = []string{"messaging_timeout"}
			causes = []string{"The broker did not acknowledge the operation in time."}
		case isResetText(text) || isRefusedText(text):
			class = ClassificationNetworkError
			score = 82
			priority = 90
			title = "Messaging connection failed"
			rules = []string{"messaging_connection"}
			causes = []string{"The process could not connect to the broker. Check bootstrap servers and NetworkPolicy."}
		}
		op := "publish to"
		if sp.Kind == models.SpanKindConsumer {
			op = "consume from"
		}
		dest := attr(sp, "messaging.destination.name", "messaging.destination")
		summary := fmt.Sprintf("%s failed to %s %s", sp.ServiceName, op, sys)
		if dest != "" {
			summary += " destination " + dest
		}
		summary += "."
		out = append(out, finding{
			classification: class,
			score:          score,
			title:          title,
			summary:        summary,
			evidence: []Evidence{{
				Code:    "messaging_system",
				Message: "Messaging system is " + sys + ".",
				SpanID:  sp.SpanID,
				Score:   20,
			}},
			causes:   causes,
			spanIDs:  []string{sp.SpanID},
			rules:    rules,
			priority: priority,
		})
	}
	return out
}

func downstream5xx(tree spanTree, server *models.Span) *models.Span {
	for _, child := range tree.children[spantree.Normalize(server.SpanID)] {
		if child.Kind != models.SpanKindClient {
			if hit := findDescendant5xx(tree, child, server.ServiceName); hit != nil {
				return hit
			}
			continue
		}
		st, ok := httpStatus(child)
		if ok && st >= 500 && st <= 599 {
			// Prefer a downstream SERVER if present.
			if hit := findDescendant5xx(tree, child, server.ServiceName); hit != nil {
				return hit
			}
			if child.ServiceName != "" && child.ServiceName != server.ServiceName {
				return child
			}
			return child
		}
		if hit := findDescendant5xx(tree, child, server.ServiceName); hit != nil {
			return hit
		}
	}
	return nil
}

func findDescendant5xx(tree spanTree, sp *models.Span, originService string) *models.Span {
	for _, ch := range tree.children[spantree.Normalize(sp.SpanID)] {
		st, ok := httpStatus(ch)
		if ch.Kind == models.SpanKindServer && ok && st >= 500 && st <= 599 && ch.ServiceName != originService {
			return ch
		}
		if hit := findDescendant5xx(tree, ch, originService); hit != nil {
			return hit
		}
	}
	return nil
}

func ruleDuplicates(tree spanTree) []finding {
	spans := tree.trace.Spans
	type key struct {
		kind    models.SpanKind
		service string
		op      string
		parent  string
	}
	groups := map[key][]*models.Span{}
	for _, sp := range spans {
		if sp == nil || !isHTTPSpan(sp) {
			continue
		}
		if sp.Kind != models.SpanKindServer && sp.Kind != models.SpanKindClient {
			continue
		}
		op := operationIdentity(sp)
		if strings.TrimSpace(op) == "" {
			continue
		}
		k := key{kind: sp.Kind, service: sp.ServiceName, op: op, parent: sp.ParentSpanID}
		groups[k] = append(groups[k], sp)
	}

	var out []finding
	for k, group := range groups {
		if len(group) < 2 {
			continue
		}
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				a, b := group[i], group[j]
				if !overlapsHeavily(a, b) {
					continue
				}
				if nearlyIdentical(a, b) {
					title := "Duplicate CLIENT spans"
					rule := "duplicate_client_spans"
					if k.kind == models.SpanKindServer {
						title = "Duplicate SERVER spans"
						rule = "duplicate_server_spans"
					}
					out = append(out, finding{
						classification: ClassificationDuplicateInstrumentation,
						score:          60,
						title:          title,
						summary:        "Two spans represent the same HTTP operation with overlapping timestamps and the same parent. This is consistent with duplicate instrumentation, not two real calls.",
						evidence: []Evidence{
							{Code: "duplicate_operation", Message: fmt.Sprintf("Repeated operation %s on service %s.", strings.TrimSpace(k.op), k.service), SpanID: a.SpanID, Score: 30},
							{Code: "overlapping_timestamps", Message: "The duplicate spans overlap in time rather than running as sequential retries.", SpanID: b.SpanID, Score: 20},
							{Code: "shared_parent", Message: "Both spans share the same parent span.", SpanID: a.ParentSpanID, Score: 10},
						},
						causes: []string{
							"The same HTTP library was instrumented twice (auto-instrumentation plus a manual wrapper).",
							"Two exporters or sidecars recorded the same operation.",
						},
						spanIDs:  []string{a.SpanID, b.SpanID},
						rules:    []string{rule, "duplicate_http_operation"},
						priority: 50,
					})
				}
			}
		}
	}
	return out
}

func overlapsHeavily(a, b *models.Span) bool {
	start := a.StartTime
	if b.StartTime.After(start) {
		start = b.StartTime
	}
	end := a.EndTime
	if b.EndTime.Before(end) {
		end = b.EndTime
	}
	if !end.After(start) {
		return false
	}
	overlap := end.Sub(start).Seconds() * 1000
	minDur := a.DurationMs
	if b.DurationMs < minDur {
		minDur = b.DurationMs
	}
	if minDur <= 0 {
		return overlap > 0
	}
	return overlap/minDur >= 0.5
}

func nearlyIdentical(a, b *models.Span) bool {
	if a.SpanID == b.SpanID {
		return false
	}
	if a.Kind != b.Kind || a.ServiceName != b.ServiceName {
		return false
	}
	da, db := a.DurationMs, b.DurationMs
	if da <= 0 || db <= 0 {
		return true
	}
	ratio := da / db
	if ratio < 1 {
		ratio = db / da
	}
	return ratio <= 2
}

func ruleTraceContext(tree spanTree) []finding {
	var out []finding
	forest := spantree.Build(tree.trace.Spans)
	seenMissing := map[string]bool{}
	for _, sp := range forest.MidTreeMissing {
		if sp == nil || seenMissing[sp.SpanID] {
			continue
		}
		seenMissing[sp.SpanID] = true
		out = append(out, finding{
			classification: ClassificationTraceContextAnomaly,
			score:          50,
			title:          "Missing parent span",
			summary:        fmt.Sprintf("Span %s references parent %s, which is not present in this trace.", sp.Name, sp.ParentSpanID),
			evidence: []Evidence{{
				Code:    "missing_parent",
				Message: fmt.Sprintf("Parent span %s was not captured.", sp.ParentSpanID),
				SpanID:  sp.SpanID,
				Score:   50,
			}},
			causes: []string{
				"The parent span was sampled out, dropped, or never exported.",
				"Trace context was propagated without the corresponding parent span.",
			},
			spanIDs:  []string{sp.SpanID},
			rules:    []string{"missing_parent"},
			priority: 40,
		})
	}
	for _, sp := range tree.trace.Spans {
		if sp == nil || spantree.IsRoot(sp.ParentSpanID) {
			continue
		}
		parent := tree.byID[spantree.Normalize(sp.ParentSpanID)]
		if parent == nil {
			continue
		}
		if sp.StartTime.After(parent.EndTime.Add(time.Duration(timingSkewMs) * time.Millisecond)) {
			out = append(out, finding{
				classification: ClassificationTraceContextAnomaly,
				score:          55,
				title:          "Impossible parent/child timing",
				summary:        fmt.Sprintf("Child span %s starts after parent %s has already ended.", sp.Name, parent.Name),
				evidence: []Evidence{{
					Code:    "child_starts_after_parent_end",
					Message: fmt.Sprintf("Child start %s is after parent end %s.", sp.StartTime.UTC().Format(time.RFC3339Nano), parent.EndTime.UTC().Format(time.RFC3339Nano)),
					SpanID:  sp.SpanID,
					Score:   55,
				}},
				causes: []string{
					"Clock skew between processes, or a broken parent/child relationship.",
					"The child was attached to the wrong parent span.",
				},
				spanIDs:  []string{parent.SpanID, sp.SpanID},
				rules:    []string{"broken_parent_child_timing"},
				priority: 40,
			})
		} else if sp.EndTime.After(parent.EndTime.Add(time.Duration(childOutsideMs) * time.Millisecond)) {
			out = append(out, finding{
				classification: ClassificationTraceContextAnomaly,
				score:          45,
				title:          "Child ends outside parent lifetime",
				summary:        fmt.Sprintf("Child span %s ends well after parent %s finished.", sp.Name, parent.Name),
				evidence: []Evidence{{
					Code:    "child_ends_outside_parent",
					Message: fmt.Sprintf("Child end %s is after parent end %s.", sp.EndTime.UTC().Format(time.RFC3339Nano), parent.EndTime.UTC().Format(time.RFC3339Nano)),
					SpanID:  sp.SpanID,
					Score:   45,
				}},
				causes: []string{
					"The parent span closed before the child finished, which is inconsistent with a real causal wait.",
					"Clock skew or an instrumentation lifecycle bug.",
				},
				spanIDs:  []string{parent.SpanID, sp.SpanID},
				rules:    []string{"broken_parent_child_timing"},
				priority: 40,
			})
		}
	}
	return out
}

func targetSuffix(sp *models.Span) string {
	if t := httpURL(sp); t != "" {
		return " from " + t
	}
	if p := httpPath(sp); p != "" {
		return " for " + p
	}
	return ""
}

func connectFailureEvidence(tree spanTree, sp *models.Span) []Evidence {
	if sp == nil {
		return nil
	}
	var out []Evidence
	seen := map[string]bool{}
	add := func(c *models.Span) {
		if c == nil || c.SpanID == sp.SpanID || seen[c.SpanID] {
			return
		}
		name := strings.ToLower(strings.TrimSpace(c.Name))
		if !isConnectSpanName(name) {
			return
		}
		kind := name
		switch {
		case strings.Contains(name, "tls") || strings.Contains(name, "ssl"):
			kind = "tls.connect"
		case strings.Contains(name, "tcp") || strings.Contains(name, "net.") || strings.Contains(name, "socket"):
			kind = "tcp.connect"
		case strings.Contains(name, "dns"):
			kind = "dns.lookup"
		}
		if c.Status != models.SpanStatusError && errorText(c) == "" {
			return
		}
		seen[c.SpanID] = true
		out = append(out, Evidence{
			Code:    "connect_failure",
			Message: fmt.Sprintf("Related %s span failed while reaching the destination.", kind),
			SpanID:  c.SpanID,
			Score:   15,
		})
	}
	for _, c := range tree.children[spantree.Normalize(sp.SpanID)] {
		add(c)
	}
	if !spantree.IsRoot(sp.ParentSpanID) {
		for _, sib := range tree.children[spantree.Normalize(sp.ParentSpanID)] {
			add(sib)
		}
	}
	if len(out) > 0 {
		return out
	}
	for _, other := range tree.trace.Spans {
		if other == nil || other.ServiceName != sp.ServiceName {
			continue
		}
		if other.Kind != models.SpanKindInternal && other.Kind != models.SpanKindClient {
			continue
		}
		if !overlapsLoose(sp, other) {
			continue
		}
		add(other)
	}
	return out
}

func overlapsLoose(a, b *models.Span) bool {
	if a == nil || b == nil {
		return false
	}
	const skew = 50 * time.Millisecond
	if a.EndTime.Add(skew).Before(b.StartTime) || b.EndTime.Add(skew).Before(a.StartTime) {
		return false
	}
	return true
}

func hasEvidenceCode(ev []Evidence, code string) bool {
	for _, e := range ev {
		if e.Code == code {
			return true
		}
	}
	return false
}

func uniqueIDs(ids []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func httpExpectedMethod(server *models.Span, methodPresent, statusPresent bool, path string) bool {
	if methodPresent {
		return true
	}
	return statusPresent || path != "" || httpURL(server) != "" || isHTTPLibrary(server)
}
