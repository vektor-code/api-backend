package store

// ReportedWorkload is a Deployment/StatefulSet/DaemonSet the agent observed on
// its own cluster. It carries the controller's own replica/ready counts, which
// are authoritative — aggregating reported pods undercounts whenever a pod is
// missing from the report, so Admin prefers these when present.
type ReportedWorkload struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	Kind          string `json:"kind"`
	Replicas      int32  `json:"replicas"`
	Ready         int32  `json:"ready"`
	Language      string `json:"language"`
	Instrumented  bool   `json:"instrumented"`
	StatusReason  string `json:"statusReason,omitempty"`
	StatusMessage string `json:"statusMessage,omitempty"`
}

// reportedWorkloadKey mirrors the cluster/namespace keying used for reported
// pods so a single agent-less deployment ("" cluster) still works.
func reportedWorkloadKey(clusterID, ns string) string {
	if clusterID == "" {
		return ns
	}
	return clusterID + "/" + ns
}

func (s *Store) SetReportedWorkloadsForCluster(clusterID, ns string, workloads []ReportedWorkload) {
	s.reportedWorkloadsMu.Lock()
	defer s.reportedWorkloadsMu.Unlock()
	if s.reportedWorkloads == nil {
		s.reportedWorkloads = make(map[string][]ReportedWorkload)
	}
	copied := make([]ReportedWorkload, len(workloads))
	copy(copied, workloads)
	s.reportedWorkloads[reportedWorkloadKey(clusterID, ns)] = copied
}

func (s *Store) GetReportedWorkloadsForCluster(clusterID, ns string) []ReportedWorkload {
	s.reportedWorkloadsMu.RLock()
	defer s.reportedWorkloadsMu.RUnlock()
	if s.reportedWorkloads == nil {
		return nil
	}
	if ns == "" && clusterID != "" {
		prefix := clusterID + "/"
		var all []ReportedWorkload
		for key, workloads := range s.reportedWorkloads {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				all = append(all, workloads...)
			}
		}
		return all
	}
	items := s.reportedWorkloads[reportedWorkloadKey(clusterID, ns)]
	out := make([]ReportedWorkload, len(items))
	copy(out, items)
	return out
}
