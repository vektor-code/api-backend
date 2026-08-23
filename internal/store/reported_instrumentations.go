package store

import "sort"

// ReportedInstrumentation is an Instrumentation CR the agent listed on its cluster.
type ReportedInstrumentation struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Endpoint  string `json:"endpoint"`
	Sampler   string `json:"sampler"`
}

func (s *Store) SetReportedInstrumentationsForCluster(clusterID string, items []ReportedInstrumentation) {
	if clusterID == "" {
		clusterID = "default"
	}
	s.reportedInstrumentationsMu.Lock()
	defer s.reportedInstrumentationsMu.Unlock()
	if s.reportedInstrumentations == nil {
		s.reportedInstrumentations = make(map[string][]ReportedInstrumentation)
	}
	copied := make([]ReportedInstrumentation, len(items))
	copy(copied, items)
	s.reportedInstrumentations[clusterID] = copied
}

func (s *Store) GetReportedInstrumentations(clusterID string) []ReportedInstrumentation {
	s.reportedInstrumentationsMu.RLock()
	defer s.reportedInstrumentationsMu.RUnlock()
	if s.reportedInstrumentations == nil {
		return nil
	}
	items := s.reportedInstrumentations[clusterID]
	out := make([]ReportedInstrumentation, len(items))
	copy(out, items)
	return out
}

func (s *Store) ReportedInstrumentationClusterIDs() []string {
	s.reportedInstrumentationsMu.RLock()
	defer s.reportedInstrumentationsMu.RUnlock()
	ids := make([]string, 0, len(s.reportedInstrumentations))
	for id, items := range s.reportedInstrumentations {
		if len(items) == 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
