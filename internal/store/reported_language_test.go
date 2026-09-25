package store

import "testing"

func TestMergePreserveWorkloadLanguage(t *testing.T) {
	prev := []ReportedWorkload{
		{Name: "api", Namespace: "dev", Language: "java"},
		{Name: "worker", Namespace: "dev", Language: "python"},
	}
	incoming := []ReportedWorkload{
		{Name: "api", Namespace: "dev", Language: ""},
		{Name: "worker", Namespace: "dev", Language: "go"},
		{Name: "new", Namespace: "dev", Language: ""},
	}
	got := mergePreserveWorkloadLanguage(prev, incoming)
	if got[0].Language != "java" {
		t.Fatalf("api language = %q, want preserved java", got[0].Language)
	}
	if got[1].Language != "go" {
		t.Fatalf("worker language = %q, want incoming go", got[1].Language)
	}
	if got[2].Language != "" {
		t.Fatalf("new workload language = %q, want empty", got[2].Language)
	}
}

func TestMergePreservePodLanguage(t *testing.T) {
	prev := []ReportedPod{
		{Name: "api-abc", Namespace: "dev", Language: "java"},
	}
	incoming := []ReportedPod{
		{Name: "api-abc", Namespace: "dev", Language: ""},
	}
	got := mergePreservePodLanguage(prev, incoming)
	if got[0].Language != "java" {
		t.Fatalf("pod language = %q, want preserved java", got[0].Language)
	}
}

func TestSetReportedWorkloadsForClusterPreservesLanguage(t *testing.T) {
	s := &Store{}
	s.SetReportedWorkloadsForCluster("cluster-a", "dev", []ReportedWorkload{
		{Name: "api", Namespace: "dev", Kind: "Deployment", Language: "java"},
	})
	s.SetReportedWorkloadsForCluster("cluster-a", "dev", []ReportedWorkload{
		{Name: "api", Namespace: "dev", Kind: "Deployment", Language: ""},
	})
	got := s.GetReportedWorkloadsForCluster("cluster-a", "dev")
	if len(got) != 1 || got[0].Language != "java" {
		t.Fatalf("got %+v, want java preserved across sync", got)
	}
}

func TestRememberDetectedLanguageSkipsInvalidInput(t *testing.T) {
	s := &Store{}
	cases := []struct {
		cluster, ns, kind, name, lang string
	}{
		{"", "dev", "Deployment", "api", "java"},
		{"cluster-a", "dev", "Deployment", "api", ""},
		{"cluster-a", "dev", "Deployment", "api", "unknown"},
		{"cluster-a", "dev", "Deployment", "", "java"},
	}
	for _, tc := range cases {
		if err := s.RememberDetectedLanguage(tc.cluster, tc.ns, tc.kind, tc.name, tc.lang); err != nil {
			t.Fatalf("RememberDetectedLanguage(%q,%q,%q,%q,%q): %v", tc.cluster, tc.ns, tc.kind, tc.name, tc.lang, err)
		}
	}
}
