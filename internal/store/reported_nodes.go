package store

type ReportedNode struct {
	Name              string  `json:"name"`
	Role              string  `json:"role"`
	CpuCapacity       float64 `json:"cpuCapacity"`
	CpuAllocatable    float64 `json:"cpuAllocatable"`
	MemoryCapacity    float64 `json:"memoryCapacity"`
	MemoryAllocatable float64 `json:"memoryAllocatable"`
	PodCapacity       int     `json:"podCapacity"`
	PodAllocatable    int     `json:"podAllocatable"`
	CpuUsage          float64 `json:"cpuUsage"`
	MemoryUsage       float64 `json:"memoryUsage"`
	MetricsAvailable  bool    `json:"metricsAvailable"`
}

func (s *Store) SetReportedNodes(nodes []ReportedNode) {
	s.SetReportedNodesForCluster("", nodes)
}

func (s *Store) SetReportedNodesForCluster(clusterID string, nodes []ReportedNode) {
	s.reportedNodesMu.Lock()
	defer s.reportedNodesMu.Unlock()
	if s.reportedNodes == nil {
		s.reportedNodes = make(map[string][]ReportedNode)
	}
	s.reportedNodes[clusterID] = nodes
}

func (s *Store) GetReportedNodes(clusterID string) []ReportedNode {
	s.reportedNodesMu.RLock()
	defer s.reportedNodesMu.RUnlock()
	if s.reportedNodes == nil {
		return nil
	}
	if clusterID != "" {
		return s.reportedNodes[clusterID]
	}
	var all []ReportedNode
	for _, nodes := range s.reportedNodes {
		all = append(all, nodes...)
	}
	return all
}
