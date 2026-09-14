package platformhealth

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Options struct {
	Namespace  string
	TailLines  int64
	RecentLogs int
	IssueLogs  int
}

// Collect builds a detailed health report for APM platform pods in namespace.
func Collect(ctx context.Context, client kubernetes.Interface, opts Options) (*Report, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client unavailable")
	}
	ns := strings.TrimSpace(opts.Namespace)
	if ns == "" {
		ns = DefaultNamespace()
	}
	if opts.TailLines <= 0 {
		opts.TailLines = 200
	}
	if opts.RecentLogs <= 0 {
		opts.RecentLogs = 40
	}
	if opts.IssueLogs <= 0 {
		opts.IssueLogs = 80
	}

	pods, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	byComponent := map[string][]PodReport{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		comp := ComponentIDFromPodName(pod.Name)
		if comp == "" {
			// Also match by app label.
			comp = ComponentIDFromPodName(pod.Labels["app.kubernetes.io/name"])
		}
		if comp == "" {
			comp = ComponentIDFromPodName(pod.Labels["app"])
		}
		if comp == "" {
			continue
		}
		report := inspectPod(ctx, client, ns, pod, opts)
		byComponent[comp] = append(byComponent[comp], report)
	}

	components := make([]ComponentReport, 0, len(KnownComponentIDs()))
	for _, id := range KnownComponentIDs() {
		podList := byComponent[id]
		sort.Slice(podList, func(i, j int) bool { return podList[i].Name < podList[j].Name })
		compIssues := []Issue{}
		if len(podList) == 0 {
			compIssues = append(compIssues, Issue{
				Severity: SeverityWarning,
				Source:   "status",
				Code:     "Missing",
				Message:  id + " has no pods in namespace " + ns,
			})
		}
		for _, p := range podList {
			compIssues = append(compIssues, p.Issues...)
		}
		components = append(components, ComponentReport{
			ID:       id,
			Status:   RollupSeverity(compIssues),
			PodCount: len(podList),
			Pods:     podList,
			Issues:   dedupeIssues(compIssues),
		})
	}

	notes := []string{}
	if len(pods.Items) == 0 {
		notes = append(notes, "No pods found in namespace "+ns)
	}

	return &Report{
		Namespace:   ns,
		GeneratedAt: time.Now().UTC(),
		Summary:     BuildSummary(components),
		Components:  components,
		Notes:       notes,
	}, nil
}

func DefaultNamespace() string {
	for _, key := range []string{"PLATFORM_NAMESPACE", "POD_NAMESPACE", "APM_NAMESPACE"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return "apm-tracing"
}

func inspectPod(ctx context.Context, client kubernetes.Interface, ns string, pod *corev1.Pod, opts Options) PodReport {
	containers := make([]ContainerReport, 0, len(pod.Status.ContainerStatuses))
	var restarts int32
	ready := true
	issues := []Issue{}

	for _, st := range pod.Status.ContainerStatuses {
		cr := containerReport(st)
		containers = append(containers, cr)
		restarts += cr.RestartCount
		if !cr.Ready {
			ready = false
		}
		issues = append(issues, IssuesFromContainer(cr)...)
	}
	initContainers := make([]ContainerReport, 0, len(pod.Status.InitContainerStatuses))
	for _, st := range pod.Status.InitContainerStatuses {
		cr := containerReport(st)
		initContainers = append(initContainers, cr)
		issues = append(issues, IssuesFromContainer(cr)...)
	}

	phase := string(pod.Status.Phase)
	if phase == "" {
		phase = "Unknown"
	}
	switch phase {
	case string(corev1.PodFailed):
		issues = append(issues, Issue{Severity: SeverityCritical, Source: "status", Code: "PodFailed", Message: "Pod phase is Failed", Detail: pod.Status.Message})
		ready = false
	case string(corev1.PodPending):
		issues = append(issues, Issue{Severity: SeverityWarning, Source: "status", Code: "PodPending", Message: "Pod is Pending", Detail: pod.Status.Message})
		ready = false
	case string(corev1.PodUnknown):
		issues = append(issues, Issue{Severity: SeverityWarning, Source: "status", Code: "PodUnknown", Message: "Pod phase is Unknown"})
		ready = false
	}

	conds := []string{}
	for _, c := range pod.Status.Conditions {
		if c.Status == corev1.ConditionTrue {
			continue
		}
		conds = append(conds, fmt.Sprintf("%s=%s (%s)", c.Type, c.Status, c.Reason))
		sev := SeverityWarning
		if c.Type == corev1.PodReady || c.Type == corev1.ContainersReady {
			sev = SeverityCritical
			ready = false
		}
		issues = append(issues, Issue{
			Severity: sev,
			Source:   "status",
			Code:     string(c.Type),
			Message:  fmt.Sprintf("condition %s is %s", c.Type, c.Status),
			Detail:   strings.TrimSpace(c.Reason + ": " + c.Message),
			At:       c.LastTransitionTime.Time,
		})
	}

	events := listPodEvents(ctx, client, ns, pod.Name)
	for _, ev := range events {
		if !strings.EqualFold(ev.Type, "Warning") {
			continue
		}
		issues = append(issues, Issue{
			Severity: SeverityWarning,
			Source:   "event",
			Code:     ev.Reason,
			Message:  ev.Reason + ": " + ev.Message,
			At:       ev.LastSeen,
		})
	}

	recent, logIssues, prevIssues, logErr := fetchLogs(ctx, client, ns, pod, opts)
	for _, li := range logIssues {
		issues = append(issues, Issue{
			Severity: li.Severity,
			Source:   "log",
			Message:  truncate(li.Line, 400),
		})
	}
	for _, li := range prevIssues {
		issues = append(issues, Issue{
			Severity: li.Severity,
			Source:   "log",
			Code:     "PreviousContainer",
			Message:  truncate(li.Line, 400),
			Detail:   "from previous crashed container",
		})
	}

	issues = dedupeIssues(issues)
	status := RollupSeverity(issues)
	if status == SeverityOK && ready && phase == string(corev1.PodRunning) {
		status = SeverityOK
	} else if status == SeverityOK && !ready {
		status = SeverityWarning
	}

	return PodReport{
		Name:              pod.Name,
		Namespace:         ns,
		Component:         ComponentIDFromPodName(pod.Name),
		Phase:             phase,
		NodeName:          pod.Spec.NodeName,
		Ready:             ready && phase == string(corev1.PodRunning),
		Restarts:          restarts,
		Status:            status,
		CreatedAt:         pod.CreationTimestamp.Time,
		Labels:            pod.Labels,
		Containers:        containers,
		InitContainers:    initContainers,
		Conditions:        conds,
		Events:            events,
		Issues:            issues,
		LogErrors:         logIssues,
		RecentLogs:        recent,
		PreviousLogErrors: prevIssues,
		LogFetchError:     logErr,
	}
}

func containerReport(st corev1.ContainerStatus) ContainerReport {
	cr := ContainerReport{
		Name:         st.Name,
		Ready:        st.Ready,
		RestartCount: st.RestartCount,
		State:        "unknown",
	}
	switch {
	case st.State.Running != nil:
		cr.State = "running"
		if !st.State.Running.StartedAt.IsZero() {
			cr.StartedAt = st.State.Running.StartedAt.Time.UTC().Format(time.RFC3339)
		}
	case st.State.Waiting != nil:
		cr.State = "waiting"
		cr.Reason = st.State.Waiting.Reason
		cr.Message = st.State.Waiting.Message
	case st.State.Terminated != nil:
		cr.State = "terminated"
		cr.Reason = st.State.Terminated.Reason
		cr.Message = st.State.Terminated.Message
		code := st.State.Terminated.ExitCode
		cr.ExitCode = &code
		if !st.State.Terminated.StartedAt.IsZero() {
			cr.StartedAt = st.State.Terminated.StartedAt.Time.UTC().Format(time.RFC3339)
		}
		if !st.State.Terminated.FinishedAt.IsZero() {
			cr.FinishedAt = st.State.Terminated.FinishedAt.Time.UTC().Format(time.RFC3339)
		}
	}
	return cr
}

func listPodEvents(ctx context.Context, client kubernetes.Interface, ns, pod string) []EventReport {
	ev, err := client.CoreV1().Events(ns).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + pod + ",involvedObject.kind=Pod",
		Limit:         50,
	})
	if err != nil {
		return nil
	}
	out := make([]EventReport, 0, len(ev.Items))
	for _, item := range ev.Items {
		out = append(out, EventReport{
			Type:      item.Type,
			Reason:    item.Reason,
			Message:   item.Message,
			Count:     item.Count,
			FirstSeen: item.FirstTimestamp.Time,
			LastSeen:  item.LastTimestamp.Time,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.After(out[j].LastSeen) })
	if len(out) > 20 {
		out = out[:20]
	}
	return out
}

func fetchLogs(ctx context.Context, client kubernetes.Interface, ns string, pod *corev1.Pod, opts Options) (recent []string, issues []LogLine, prev []LogLine, errMsg string) {
	if len(pod.Spec.Containers) == 0 {
		return nil, nil, nil, "no containers"
	}
	container := pod.Spec.Containers[0].Name
	// Prefer first non-ready / restarting container for diagnosis.
	for _, st := range pod.Status.ContainerStatuses {
		if !st.Ready || st.RestartCount > 0 || st.State.Waiting != nil || (st.State.Terminated != nil && st.State.Terminated.ExitCode != 0) {
			container = st.Name
			break
		}
	}

	raw, err := readLogs(ctx, client, ns, pod.Name, container, opts.TailLines, false)
	if err != nil {
		errMsg = err.Error()
	} else {
		recent, issues = ScanLogLines(raw, opts.RecentLogs, opts.IssueLogs)
	}

	// Previous instance logs are invaluable for CrashLoopBackOff.
	prevRaw, prevErr := readLogs(ctx, client, ns, pod.Name, container, opts.TailLines, true)
	if prevErr == nil && strings.TrimSpace(prevRaw) != "" {
		_, prev = ScanLogLines(prevRaw, 0, opts.IssueLogs)
	}
	return recent, issues, prev, errMsg
}

func readLogs(ctx context.Context, client kubernetes.Interface, ns, pod, container string, tail int64, previous bool) (string, error) {
	opts := &corev1.PodLogOptions{
		Container: container,
		TailLines: &tail,
		Previous:  previous,
	}
	req := client.CoreV1().Pods(ns).GetLogs(pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	b, err := io.ReadAll(io.LimitReader(stream, 512*1024))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func dedupeIssues(in []Issue) []Issue {
	seen := map[string]bool{}
	out := make([]Issue, 0, len(in))
	for _, issue := range in {
		key := string(issue.Severity) + "|" + issue.Source + "|" + issue.Code + "|" + issue.Message
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, issue)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
