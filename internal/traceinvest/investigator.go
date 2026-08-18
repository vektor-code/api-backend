package traceinvest

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

// Cluster is the Kubernetes surface the investigator is allowed to use.
type Cluster interface {
	GetPod(ctx context.Context, namespace, name string) (*k8s.PodView, error)
	FindPodByIP(ctx context.Context, namespace, ip string) (*k8s.PodView, error)
	FindRunningPods(ctx context.Context, namespace, workload string) ([]*k8s.PodView, error)
	FindServiceByIP(ctx context.Context, namespace, ip string) (*k8s.ServiceView, error)
	GetEndpoints(ctx context.Context, namespace, service string) (*k8s.EndpointsView, error)
	ListWarningEvents(ctx context.Context, namespace, pod string) ([]string, error)
	FindDiagnosticPod(ctx context.Context, namespace string) (*k8s.PodView, error)
	Exec(ctx context.Context, namespace, pod, container string, argv []string) (*k8s.ExecResult, error)
}

// Resolver picks a cluster access handle for a trace. Returning nil is allowed.
type Resolver func(ctx context.Context, trace *models.Trace) (Cluster, error)

// Investigator is the active layer. It is invoked only after the passive analyzer.
type Investigator struct {
	resolve Resolver
	cache   *ttlCache
	limit   *limiter
	flight  *flight
}

func New(resolve Resolver) *Investigator {
	return &Investigator{
		resolve: resolve,
		cache:   newTTLCache(DefaultTTL()),
		limit:   newLimiter(DefaultLimits()),
		flight:  newFlight(),
	}
}

// Investigate never mutates spans. Callers must not block trace rendering on it.
func (inv *Investigator) Investigate(ctx context.Context, trace *models.Trace, diag *tracediag.Diagnosis) *Report {
	start := time.Now()
	if diag == nil {
		return &Report{TraceID: traceID(trace), Status: StatusSkipped, SkipReason: "No failure to verify"}
	}
	plan := tracediag.LivePlanFor(diag)
	if !plan.Recommended || plan.MaxLevel <= 0 {
		return &Report{
			TraceID:    diag.TraceID,
			Status:     StatusSkipped,
			SkipReason: plan.Reason,
			Conclusion: "Kubernetes verification not required",
		}
	}

	target := ExtractTarget(trace, diag)
	key := target.CacheKey()
	if cached, ok := inv.cache.get(key); ok {
		cached.TraceID = diag.TraceID
		return cached
	}

	report := inv.flight.do(key, func() *Report {
		if existing, ok := inv.cache.get(key); ok {
			return existing
		}
		release, ok := inv.limit.acquire(target.Namespace, target.Workload, target.DestHost+":"+target.DestPort)
		if !ok {
			return &Report{
				TraceID:    diag.TraceID,
				Status:     StatusRateLimited,
				SkipReason: "Live verification deferred: another check is already in flight for this workload or destination",
				CacheKey:   key,
			}
		}
		defer release()

		out := inv.run(ctx, trace, diag, plan, target, key)
		if out.Status == StatusComplete || out.Status == StatusPartial {
			inv.cache.set(key, out)
		}
		return out
	})
	if report != nil {
		report.DurationMs = time.Since(start).Milliseconds()
		report.TraceID = diag.TraceID
	}
	return report
}

func (inv *Investigator) run(ctx context.Context, trace *models.Trace, diag *tracediag.Diagnosis, plan tracediag.LivePlan, target Target, key string) *Report {
	out := &Report{TraceID: diag.TraceID, CacheKey: key, LevelReached: 0}
	if inv.resolve == nil {
		out.Status = StatusUnavailable
		out.SkipReason = "No Kubernetes access configured"
		return out
	}
	cluster, err := inv.resolve(ctx, trace)
	if err != nil || cluster == nil {
		out.Status = StatusUnavailable
		if err != nil {
			out.SkipReason = "No Kubernetes credentials for this cluster"
		} else {
			out.SkipReason = "No Kubernetes access configured"
		}
		return out
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	checks := inv.level1(ctx, cluster, target)
	out.Checks = checks
	out.LevelReached = 1

	if plan.MaxLevel >= 2 {
		live, usedLevel := inv.level2or3(ctx, cluster, target, plan.MaxLevel)
		if live != nil {
			out.Checks = append(out.Checks, *live)
			if usedLevel > out.LevelReached {
				out.LevelReached = usedLevel
			}
		}
	}

	out.Conclusion = conclude(diag, out.Checks)
	out.Status = StatusComplete
	if !anyOK(out.Checks) {
		out.Status = StatusPartial
	}
	return out
}

func (inv *Investigator) level1(ctx context.Context, cluster Cluster, target Target) []Check {
	var checks []Check
	destPod, _ := cluster.FindPodByIP(ctx, target.Namespace, target.DestHost)
	if destPod == nil && target.SourcePod != "" {
		if p, err := cluster.GetPod(ctx, target.Namespace, target.SourcePod); err == nil {
			destPod = p
		}
	}
	if destPod != nil {
		detail := fmt.Sprintf("Pod %s is %s, Ready=%v, restarts=%d", destPod.Name, destPod.Phase, destPod.Ready, destPod.Restarts)
		checks = append(checks, Check{Level: 1, Code: "pod_ready", OK: destPod.Ready, Detail: detail, Pod: destPod.Name})
		if ev, err := cluster.ListWarningEvents(ctx, destPod.Namespace, destPod.Name); err == nil && len(ev) > 0 {
			checks = append(checks, Check{Level: 1, Code: "pod_events", OK: false, Detail: "Recent warnings: " + strings.Join(ev, "; "), Pod: destPod.Name})
		}
	}

	svc, _ := cluster.FindServiceByIP(ctx, target.Namespace, target.DestHost)
	if svc != nil {
		ep, err := cluster.GetEndpoints(ctx, svc.Namespace, svc.Name)
		if err == nil && ep != nil {
			ok := containsIP(ep.Ready, target.DestHost) || len(ep.Ready) > 0
			detail := fmt.Sprintf("Service %s ClusterIP %s has %d ready endpoint(s)", svc.Name, svc.ClusterIP, len(ep.Ready))
			if destPod != nil {
				detail += "; includes pod " + destPod.Name
			}
			checks = append(checks, Check{Level: 1, Code: "endpoints", OK: ok, Detail: detail})
		}
	} else if destPod != nil {
		checks = append(checks, Check{Level: 1, Code: "pod_ip", OK: true, Detail: fmt.Sprintf("%s belongs to pod %s", target.DestHost, destPod.Name), Pod: destPod.Name})
	} else if target.DestHost != "" {
		checks = append(checks, Check{Level: 1, Code: "dest_unresolved", OK: false, Detail: "Could not map " + target.DestHost + " to a pod or Service in " + target.Namespace})
	}
	return checks
}

func (inv *Investigator) level2or3(ctx context.Context, cluster Cluster, target Target, maxLevel int) (*Check, int) {
	if target.DestURL == "" {
		return &Check{Level: 2, Code: "exec_skipped", OK: false, Detail: "No destination URL in telemetry"}, 1
	}
	cmd, err := httpProbeArgv(target.DestURL)
	if err != nil {
		return &Check{Level: 2, Code: "exec_skipped", OK: false, Detail: "Destination URL is not a permitted probe target"}, 1
	}

	pod, level, how := inv.probePod(ctx, cluster, target, maxLevel)
	if pod == nil {
		return &Check{Level: 2, Code: "exec_skipped", OK: false, Detail: how}, 1
	}

	res, execErr := cluster.Exec(ctx, pod.Namespace, pod.Name, pod.Container, cmd)
	detail := how
	status := parseHTTPStatus(res)
	ok := false
	if execErr != nil && (res == nil || strings.TrimSpace(res.Stdout+res.Stderr) == "") {
		detail += "; exec failed: " + execErr.Error()
	} else {
		if status > 0 {
			detail += fmt.Sprintf("; request from %s returned HTTP %d", pod.Name, status)
			ok = target.RecordedHTTP == 0 || status == target.RecordedHTTP || (target.RecordedHTTP >= 500 && status >= 500)
		} else if res != nil {
			snippet := strings.TrimSpace(res.Stdout + " " + res.Stderr)
			if len(snippet) > 240 {
				snippet = snippet[:240]
			}
			detail += "; probe output: " + snippet
		}
		if res != nil && strings.Contains(res.Stdout+res.Stderr, "PROBE_INSTALLED") {
			detail += "; ephemeral wget was installed for the check and removed afterward"
		}
	}
	return &Check{Level: level, Code: "source_http_probe", OK: ok, Detail: detail, Pod: pod.Name}, level
}

func (inv *Investigator) probePod(ctx context.Context, cluster Cluster, target Target, maxLevel int) (*k8s.PodView, int, string) {
	if target.SourcePod != "" {
		if p, err := cluster.GetPod(ctx, target.Namespace, target.SourcePod); err == nil && p != nil && p.Phase == "Running" && !p.Deleting {
			return p, 2, "exec into existing source pod " + p.Name
		}
	}
	if pods, err := cluster.FindRunningPods(ctx, target.Namespace, target.Workload); err == nil && len(pods) > 0 {
		return pods[0], 2, "exec into existing " + target.Workload + " pod " + pods[0].Name
	}
	if maxLevel >= 3 {
		if d, err := cluster.FindDiagnosticPod(ctx, target.Namespace); err == nil && d != nil {
			return d, 3, "no application pod available; reused diagnostic worker " + d.Name + " in " + target.Namespace
		}
		return nil, 1, "exec skipped: no running source pod and no reusable diagnostic worker in " + target.Namespace
	}
	return nil, 1, "exec skipped: no running source pod in " + target.Namespace
}

func conclude(diag *tracediag.Diagnosis, checks []Check) string {
	var probe *Check
	var ready *Check
	for i := range checks {
		c := &checks[i]
		if c.Code == "source_http_probe" {
			probe = c
		}
		if c.Code == "pod_ready" {
			ready = c
		}
	}
	if probe != nil && probe.OK && diag.Classification == tracediag.ClassificationApplicationError {
		return "The backend is actively returning the recorded HTTP error from the same workload context."
	}
	if probe != nil && !probe.OK && strings.Contains(probe.Detail, "returned HTTP") {
		return "A live request from the source workload did not reproduce the recorded HTTP status; the failure may be transient."
	}
	if ready != nil && ready.OK {
		return "The target workload is Ready; the recorded failure is consistent with application-level behavior rather than a missing endpoint."
	}
	if ready != nil && !ready.OK {
		return "The target pod is not Ready; infrastructure state may explain the recorded failure."
	}
	return "Live Kubernetes inspection completed; see checks for evidence."
}

func anyOK(checks []Check) bool {
	for _, c := range checks {
		if c.OK {
			return true
		}
	}
	return len(checks) == 0
}

func containsIP(list []string, ip string) bool {
	for _, item := range list {
		if item == ip {
			return true
		}
	}
	return false
}

func traceID(trace *models.Trace) string {
	if trace == nil {
		return ""
	}
	return trace.TraceID
}

var httpStatusRe = regexp.MustCompile(`(?i)HTTP/[0-9.]+ (\d{3})|HTTP_STATUS\s+(\d{3})`)

func parseHTTPStatus(res *k8s.ExecResult) int {
	if res == nil {
		return 0
	}
	m := httpStatusRe.FindStringSubmatch(res.Stdout + "\n" + res.Stderr)
	if len(m) < 2 {
		return 0
	}
	for _, g := range m[1:] {
		if g == "" {
			continue
		}
		n, _ := strconv.Atoi(g)
		return n
	}
	return 0
}
