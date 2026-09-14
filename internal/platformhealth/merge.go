package platformhealth

import (
	"sort"
	"strings"
	"time"
)

// SourceReport is one health snapshot from a cluster (local API, remote token, or agent push).
type SourceReport struct {
	Cluster  string
	Priority int // lower wins when the same pod appears in multiple sources
	Report   *Report
	Note     string
	// Optional cluster reachability metadata for Admin UI.
	ClusterStatus string // ok|stale|unreachable|error
	Mode          string // local|remote-token|agent-push
	LastSeen      time.Time
}

// ClusterHealth summarizes reachability for one cluster source.
type ClusterHealth struct {
	ID       string    `json:"id"`
	Status   string    `json:"status"` // ok|stale|unreachable|error
	Mode     string    `json:"mode"`   // local|remote-token|agent-push
	Detail   string    `json:"detail,omitempty"`
	LastSeen time.Time `json:"lastSeen,omitempty"`
}

// TagCluster stamps cluster on the report and every pod.
func TagCluster(r *Report, cluster string) *Report {
	if r == nil {
		return nil
	}
	if cluster == "" {
		cluster = "local"
	}
	r.Cluster = cluster
	for i := range r.Components {
		for j := range r.Components[i].Pods {
			r.Components[i].Pods[j].Cluster = cluster
			if r.Components[i].Pods[j].Component == "" {
				r.Components[i].Pods[j].Component = ComponentIDFromPodName(r.Components[i].Pods[j].Name)
			}
		}
	}
	return r
}

// OnlyPresentComponents drops empty component slots (used for agent push payloads).
func OnlyPresentComponents(r *Report) *Report {
	if r == nil {
		return nil
	}
	out := *r
	comps := make([]ComponentReport, 0, len(r.Components))
	for _, c := range r.Components {
		if c.PodCount == 0 && len(c.Pods) == 0 {
			continue
		}
		comps = append(comps, c)
	}
	out.Components = comps
	out.Summary = BuildSummary(comps)
	return &out
}

// Merge combines multiple source reports. Prefer lower Priority when the same
// namespace/name pod appears twice (local k8s beats agent push).
// Sources with nil Report still contribute Notes and Clusters (partial outage).
func Merge(sources []SourceReport) *Report {
	type podKey struct{ ns, name string }
	best := map[podKey]struct {
		priority int
		pod      PodReport
	}{}
	notes := []string{}
	sourceLabels := []string{}
	clusters := []ClusterHealth{}
	ns := ""
	generated := time.Time{}

	for _, src := range sources {
		label := strings.TrimSpace(src.Cluster)
		if label == "" {
			label = "local"
		}
		if src.Note != "" {
			notes = append(notes, src.Note)
		}
		status := src.ClusterStatus
		if status == "" {
			if src.Report != nil {
				status = "ok"
			} else {
				status = "error"
			}
		}
		mode := src.Mode
		if mode == "" {
			mode = "local"
		}
		clusters = append(clusters, ClusterHealth{
			ID:       label,
			Status:   status,
			Mode:     mode,
			Detail:   src.Note,
			LastSeen: src.LastSeen,
		})

		if src.Report == nil {
			sourceLabels = append(sourceLabels, label)
			continue
		}

		r := TagCluster(src.Report, label)
		if ns == "" && r.Namespace != "" {
			ns = r.Namespace
		}
		if r.GeneratedAt.After(generated) {
			generated = r.GeneratedAt
		}
		sourceLabels = append(sourceLabels, label)
		notes = append(notes, r.Notes...)
		for _, comp := range r.Components {
			for _, pod := range comp.Pods {
				if pod.Component == "" {
					pod.Component = comp.ID
				}
				if pod.Cluster == "" {
					pod.Cluster = label
				}
				key := podKey{ns: pod.Namespace, name: pod.Name}
				prev, ok := best[key]
				if !ok || src.Priority < prev.priority {
					best[key] = struct {
						priority int
						pod      PodReport
					}{priority: src.Priority, pod: pod}
				}
			}
		}
	}

	byComp := map[string][]PodReport{}
	for _, item := range best {
		comp := item.pod.Component
		if comp == "" {
			comp = ComponentIDFromPodName(item.pod.Name)
		}
		if comp == "" {
			continue
		}
		byComp[comp] = append(byComp[comp], item.pod)
	}

	components := make([]ComponentReport, 0, len(KnownComponentIDs()))
	for _, id := range KnownComponentIDs() {
		pods := byComp[id]
		sort.Slice(pods, func(i, j int) bool {
			if pods[i].Cluster != pods[j].Cluster {
				return pods[i].Cluster < pods[j].Cluster
			}
			return pods[i].Name < pods[j].Name
		})
		issues := []Issue{}
		if len(pods) == 0 {
			issues = append(issues, Issue{
				Severity: SeverityWarning,
				Source:   "status",
				Code:     "Missing",
				Message:  id + " has no pods across collected clusters",
			})
		}
		for _, p := range pods {
			issues = append(issues, p.Issues...)
		}
		components = append(components, ComponentReport{
			ID:       id,
			Status:   RollupSeverity(issues),
			PodCount: len(pods),
			Pods:     pods,
			Issues:   dedupeIssues(issues),
		})
	}

	if ns == "" {
		ns = DefaultNamespace()
	}
	if generated.IsZero() {
		generated = time.Now().UTC()
	}

	sort.Strings(sourceLabels)
	sourceLabels = uniqueStrings(sourceLabels)
	notes = uniqueStrings(notes)
	clusters = dedupeClusters(clusters)

	// Any unreachable/stale/error cluster raises overall warning at least.
	for _, ch := range clusters {
		if ch.Status == "ok" {
			continue
		}
		sev := SeverityWarning
		if ch.Status == "unreachable" {
			sev = SeverityCritical
		}
		notes = append(notes, "cluster "+ch.ID+" is "+ch.Status+": "+ch.Detail)
		// Attach a synthetic issue onto agent-backend when the remote agent cluster is bad.
		if ch.Mode == "agent-push" || ch.Mode == "remote-token" {
			for i := range components {
				if components[i].ID != "agent-backend" {
					continue
				}
				components[i].Issues = dedupeIssues(append(components[i].Issues, Issue{
					Severity: sev,
					Source:   "status",
					Code:     "Cluster" + capitalize(ch.Status),
					Message:  "cluster " + ch.ID + " is " + ch.Status,
					Detail:   ch.Detail,
				}))
				components[i].Status = RollupSeverity(components[i].Issues)
			}
		}
	}
	notes = uniqueStrings(notes)

	return &Report{
		Namespace:   ns,
		GeneratedAt: generated,
		Summary:     BuildSummary(components),
		Components:  components,
		Notes:       notes,
		Sources:     sourceLabels,
		Clusters:    clusters,
	}
}

func dedupeClusters(in []ClusterHealth) []ClusterHealth {
	best := map[string]ClusterHealth{}
	rank := map[string]int{"ok": 0, "stale": 1, "error": 2, "unreachable": 3}
	for _, c := range in {
		prev, ok := best[c.ID]
		if !ok || rank[c.Status] > rank[prev.Status] {
			best[c.ID] = c
		} else if rank[c.Status] == rank[prev.Status] && c.Detail != "" && prev.Detail == "" {
			best[c.ID] = c
		}
	}
	out := make([]ClusterHealth, 0, len(best))
	for _, c := range best {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func capitalize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
