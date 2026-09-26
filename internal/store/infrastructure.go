package store

import (
	"sort"
	"strings"
)

// Resource-pressure thresholds (percent of limit) at which a pod is flagged.
const (
	oomRiskThreshold     = 85.0 // memory usage this close to its limit risks OOM-kill
	cpuThrottleThreshold = 90.0 // CPU usage this close to its limit is being throttled
)

// InfraPod is one pod's live resource footprint (usage vs limit) with pressure flags.
type InfraPod struct {
	Name        string  `json:"name"`
	Namespace   string  `json:"namespace"`
	Node        string  `json:"node"`
	Phase       string  `json:"phase"`
	CpuUsage    float64 `json:"cpuUsage"` // millicores
	CpuLimit    float64 `json:"cpuLimit"` // millicores; 0 = unlimited
	MemUsage    float64 `json:"memUsage"` // MiB
	MemLimit    float64 `json:"memLimit"` // MiB; 0 = unlimited
	CpuPct      float64 `json:"cpuPct"`
	MemPct      float64 `json:"memPct"`
	Restarts    int     `json:"restarts"`
	OomRisk     bool    `json:"oomRisk"`
	CpuThrottle bool    `json:"cpuThrottle"`
}

// InfraNamespace rolls resource usage up per namespace.
type InfraNamespace struct {
	Namespace string  `json:"namespace"`
	Pods      int     `json:"pods"`
	CpuUsage  float64 `json:"cpuUsage"`
	MemUsage  float64 `json:"memUsage"`
	Restarts  int     `json:"restarts"`
	AtRisk    int     `json:"atRisk"`
}

// InfraNode rolls resource usage up per node. CpuUsage/MemUsage are the pods in
// the selected scope; TotalCpuUsage/TotalMemUsage are whole-node metrics when
// metrics-server reports them.
type InfraNode struct {
	Name             string  `json:"name"`
	Role             string  `json:"role"`
	Pods             int     `json:"pods"`
	Namespaces       int     `json:"namespaces"`
	CpuUsage         float64 `json:"cpuUsage"`
	CpuCapacity      float64 `json:"cpuCapacity"`
	CpuAllocatable   float64 `json:"cpuAllocatable"`
	TotalCpuUsage    float64 `json:"totalCpuUsage"`
	CpuPct           float64 `json:"cpuPct"`
	MemUsage         float64 `json:"memUsage"`
	MemCapacity      float64 `json:"memCapacity"`
	MemAllocatable   float64 `json:"memAllocatable"`
	TotalMemUsage    float64 `json:"totalMemUsage"`
	MemPct           float64 `json:"memPct"`
	PodCapacity      int     `json:"podCapacity"`
	PodAllocatable   int     `json:"podAllocatable"`
	PodPct           float64 `json:"podPct"`
	Restarts         int     `json:"restarts"`
	AtRisk           int     `json:"atRisk"`
	MetricsAvailable bool    `json:"metricsAvailable"`
	CloudProvider    string  `json:"cloudProvider,omitempty"`
	Region           string  `json:"region,omitempty"`
	Zone             string  `json:"zone,omitempty"`
	InstanceType     string  `json:"instanceType,omitempty"`
}

// InfraSummary is the cluster-wide rollup for the header tiles.
type InfraSummary struct {
	Pods             int     `json:"pods"`
	Nodes            int     `json:"nodes"`
	Namespaces       int     `json:"namespaces"`
	CpuUsage         float64 `json:"cpuUsage"`
	CpuLimit         float64 `json:"cpuLimit"`
	CpuCapacity      float64 `json:"cpuCapacity"`
	CpuAllocatable   float64 `json:"cpuAllocatable"`
	TotalCpuUsage    float64 `json:"totalCpuUsage"`
	MemUsage         float64 `json:"memUsage"`
	MemLimit         float64 `json:"memLimit"`
	MemCapacity      float64 `json:"memCapacity"`
	MemAllocatable   float64 `json:"memAllocatable"`
	TotalMemUsage    float64 `json:"totalMemUsage"`
	PodCapacity      int     `json:"podCapacity"`
	PodAllocatable   int     `json:"podAllocatable"`
	Restarts         int     `json:"restarts"`
	AtRisk           int     `json:"atRisk"`
	MetricsAvailable bool    `json:"metricsAvailable"`
}

// InfrastructureMetrics is the Infrastructure page payload, aggregated from the
// per-pod resource data the agent already reports (metrics.k8s.io).
type InfrastructureMetrics struct {
	Summary    InfraSummary     `json:"summary"`
	Namespaces []InfraNamespace `json:"namespaces"`
	Nodes      []InfraNode      `json:"nodes"`
	Pods       []InfraPod       `json:"pods"`
}

// GetInfrastructureMetrics aggregates agent-reported pod resource usage into a
// cluster/namespace/pod view, flagging pods that are near their limits.
func (s *Store) GetInfrastructureMetrics(namespace string, allowedNs []string) *InfrastructureMetrics {
	allowed := map[string]bool{}
	for _, ns := range allowedNs {
		allowed[ns] = true
	}

	pods := dedupeReportedPods(s.GetReportedPods(namespace)) // "" => all namespaces
	reportedNodes := dedupeReportedNodes(s.GetReportedNodes(""))
	out := &InfrastructureMetrics{Namespaces: []InfraNamespace{}, Nodes: []InfraNode{}, Pods: []InfraPod{}}
	nsMap := map[string]*InfraNamespace{}
	nodeMap := map[string]*InfraNode{}
	nodeNamespaces := map[string]map[string]bool{}

	for _, rn := range reportedNodes {
		if rn.Name == "" {
			continue
		}
		nodeMap[rn.Name] = &InfraNode{
			Name:             rn.Name,
			Role:             firstNonEmpty(rn.Role, inferNodeRole(rn.Name)),
			CpuCapacity:      rn.CpuCapacity,
			CpuAllocatable:   rn.CpuAllocatable,
			TotalCpuUsage:    rn.CpuUsage,
			MemCapacity:      rn.MemoryCapacity,
			MemAllocatable:   rn.MemoryAllocatable,
			TotalMemUsage:    rn.MemoryUsage,
			PodCapacity:      rn.PodCapacity,
			PodAllocatable:   rn.PodAllocatable,
			MetricsAvailable: rn.MetricsAvailable,
			CloudProvider:    rn.CloudProvider,
			Region:           rn.Region,
			Zone:             rn.Zone,
			InstanceType:     rn.InstanceType,
		}
		out.Summary.CpuCapacity += rn.CpuCapacity
		out.Summary.CpuAllocatable += rn.CpuAllocatable
		out.Summary.TotalCpuUsage += rn.CpuUsage
		out.Summary.MemCapacity += rn.MemoryCapacity
		out.Summary.MemAllocatable += rn.MemoryAllocatable
		out.Summary.TotalMemUsage += rn.MemoryUsage
		out.Summary.PodCapacity += rn.PodCapacity
		out.Summary.PodAllocatable += rn.PodAllocatable
		if rn.MetricsAvailable {
			out.Summary.MetricsAvailable = true
		}
	}

	ensureNode := func(name string) *InfraNode {
		if name == "" {
			name = "unscheduled"
		}
		node := nodeMap[name]
		if node == nil {
			node = &InfraNode{Name: name, Role: inferNodeRole(name)}
			nodeMap[name] = node
		}
		return node
	}

	for _, p := range pods {
		if s.IsNamespaceDisabled(p.Namespace) {
			continue
		}
		if namespace == "" && allowedNs != nil && !allowed[p.Namespace] {
			continue
		}

		cpuPct := resourcePct(p.CpuUsage, p.CpuLimit)
		memPct := resourcePct(p.MemoryUsage, p.MemoryLimit)
		oom := p.MemoryLimit > 0 && memPct >= oomRiskThreshold
		throttle := p.CpuLimit > 0 && cpuPct >= cpuThrottleThreshold

		out.Pods = append(out.Pods, InfraPod{
			Name: p.Name, Namespace: p.Namespace, Node: p.NodeName, Phase: p.Phase,
			CpuUsage: p.CpuUsage, CpuLimit: p.CpuLimit,
			MemUsage: p.MemoryUsage, MemLimit: p.MemoryLimit,
			CpuPct: cpuPct, MemPct: memPct, Restarts: p.RestartCount,
			OomRisk: oom, CpuThrottle: throttle,
		})

		ns := nsMap[p.Namespace]
		if ns == nil {
			ns = &InfraNamespace{Namespace: p.Namespace}
			nsMap[p.Namespace] = ns
		}
		ns.Pods++
		ns.CpuUsage += p.CpuUsage
		ns.MemUsage += p.MemoryUsage
		ns.Restarts += p.RestartCount
		atRisk := oom || throttle
		if atRisk {
			ns.AtRisk++
		}

		node := ensureNode(p.NodeName)
		node.Pods++
		node.CpuUsage += p.CpuUsage
		node.MemUsage += p.MemoryUsage
		node.Restarts += p.RestartCount
		if atRisk {
			node.AtRisk++
		}
		if p.Namespace != "" {
			nodeNs := nodeNamespaces[node.Name]
			if nodeNs == nil {
				nodeNs = map[string]bool{}
				nodeNamespaces[node.Name] = nodeNs
			}
			nodeNs[p.Namespace] = true
		}

		out.Summary.CpuUsage += p.CpuUsage
		out.Summary.CpuLimit += p.CpuLimit
		out.Summary.MemUsage += p.MemoryUsage
		out.Summary.MemLimit += p.MemoryLimit
		out.Summary.Restarts += p.RestartCount
		if atRisk {
			out.Summary.AtRisk++
		}
	}

	out.Summary.Pods = len(out.Pods)
	out.Summary.Namespaces = len(nsMap)

	for _, v := range nsMap {
		out.Namespaces = append(out.Namespaces, *v)
	}
	sort.Slice(out.Namespaces, func(i, j int) bool {
		return out.Namespaces[i].CpuUsage > out.Namespaces[j].CpuUsage
	})
	for _, node := range nodeMap {
		node.Namespaces = len(nodeNamespaces[node.Name])
		cpuUsage := node.CpuUsage
		memUsage := node.MemUsage
		if node.MetricsAvailable {
			cpuUsage = node.TotalCpuUsage
			memUsage = node.TotalMemUsage
		}
		node.CpuPct = resourcePct(cpuUsage, firstPositive(node.CpuAllocatable, node.CpuCapacity))
		node.MemPct = resourcePct(memUsage, firstPositive(node.MemAllocatable, node.MemCapacity))
		node.PodPct = resourcePct(float64(node.Pods), float64(firstPositiveInt(node.PodAllocatable, node.PodCapacity)))
		out.Nodes = append(out.Nodes, *node)
	}
	sort.Slice(out.Nodes, func(i, j int) bool {
		a, b := out.Nodes[i], out.Nodes[j]
		if a.AtRisk != b.AtRisk {
			return a.AtRisk > b.AtRisk
		}
		if a.CpuPct != b.CpuPct {
			return a.CpuPct > b.CpuPct
		}
		if a.MemPct != b.MemPct {
			return a.MemPct > b.MemPct
		}
		if a.Pods != b.Pods {
			return a.Pods > b.Pods
		}
		return a.Name < b.Name
	})
	out.Summary.Nodes = len(out.Nodes)
	// Worst-first: OOM risk, then memory pressure, then CPU pressure.
	sort.Slice(out.Pods, func(i, j int) bool {
		a, b := out.Pods[i], out.Pods[j]
		if a.OomRisk != b.OomRisk {
			return a.OomRisk
		}
		if a.MemPct != b.MemPct {
			return a.MemPct > b.MemPct
		}
		return a.CpuPct > b.CpuPct
	})
	return out
}

func resourcePct(usage, limit float64) float64 {
	if limit <= 0 {
		return 0
	}
	return (usage / limit) * 100
}

func dedupeReportedPods(pods []ReportedPod) []ReportedPod {
	byKey := map[string]ReportedPod{}
	for _, pod := range pods {
		key := pod.Namespace + "/" + pod.Name + "/" + pod.NodeName
		current, ok := byKey[key]
		if !ok {
			byKey[key] = pod
			continue
		}
		if current.Phase == "" {
			current.Phase = pod.Phase
		}
		current.CpuUsage = maxFloat(current.CpuUsage, pod.CpuUsage)
		current.CpuLimit = maxFloat(current.CpuLimit, pod.CpuLimit)
		current.MemoryUsage = maxFloat(current.MemoryUsage, pod.MemoryUsage)
		current.MemoryLimit = maxFloat(current.MemoryLimit, pod.MemoryLimit)
		if pod.RestartCount > current.RestartCount {
			current.RestartCount = pod.RestartCount
		}
		byKey[key] = current
	}

	out := make([]ReportedPod, 0, len(byKey))
	for _, pod := range byKey {
		out = append(out, pod)
	}
	return out
}

func dedupeReportedNodes(nodes []ReportedNode) []ReportedNode {
	byName := map[string]ReportedNode{}
	for _, node := range nodes {
		if node.Name == "" {
			continue
		}
		current, ok := byName[node.Name]
		if !ok {
			byName[node.Name] = node
			continue
		}
		current.CpuCapacity = maxFloat(current.CpuCapacity, node.CpuCapacity)
		if current.Role == "" {
			current.Role = node.Role
		}
		current.CloudProvider = firstNonEmpty(current.CloudProvider, node.CloudProvider)
		current.Region = firstNonEmpty(current.Region, node.Region)
		current.Zone = firstNonEmpty(current.Zone, node.Zone)
		current.InstanceType = firstNonEmpty(current.InstanceType, node.InstanceType)
		current.CpuAllocatable = maxFloat(current.CpuAllocatable, node.CpuAllocatable)
		current.MemoryCapacity = maxFloat(current.MemoryCapacity, node.MemoryCapacity)
		current.MemoryAllocatable = maxFloat(current.MemoryAllocatable, node.MemoryAllocatable)
		if node.PodCapacity > current.PodCapacity {
			current.PodCapacity = node.PodCapacity
		}
		if node.PodAllocatable > current.PodAllocatable {
			current.PodAllocatable = node.PodAllocatable
		}
		if node.MetricsAvailable || !current.MetricsAvailable {
			current.CpuUsage = maxFloat(current.CpuUsage, node.CpuUsage)
			current.MemoryUsage = maxFloat(current.MemoryUsage, node.MemoryUsage)
			current.MetricsAvailable = current.MetricsAvailable || node.MetricsAvailable
		}
		byName[node.Name] = current
	}

	out := make([]ReportedNode, 0, len(byName))
	for _, node := range byName {
		out = append(out, node)
	}
	return out
}

func firstPositive(a, b float64) float64 {
	if a > 0 {
		return a
	}
	return b
}

func firstPositiveInt(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func inferNodeRole(name string) string {
	lower := strings.ToLower(name)
	if strings.Contains(lower, "control-plane") || strings.Contains(lower, "master") {
		return "master"
	}
	parts := strings.Split(lower, "-")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if len(last) > 1 && last[0] == 'm' && allDigits(last[1:]) {
			return "master"
		}
	}
	return "worker"
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
