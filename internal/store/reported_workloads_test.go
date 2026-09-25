package store

import "testing"

func TestReportedWorkloadsAreClusterScoped(t *testing.T) {
	s := &Store{}
	s.SetReportedWorkloadsForCluster("cluster-a", "dev", []ReportedWorkload{
		{Name: "api", Namespace: "dev", Kind: "Deployment", Replicas: 3, Ready: 2},
	})
	s.SetReportedWorkloadsForCluster("cluster-b", "dev", []ReportedWorkload{
		{Name: "other", Namespace: "dev", Kind: "Deployment", Replicas: 1, Ready: 1},
	})

	got := s.GetReportedWorkloadsForCluster("cluster-a", "dev")
	if len(got) != 1 || got[0].Name != "api" || got[0].Ready != 2 {
		t.Fatalf("cluster-a/dev = %+v, want single api workload with Ready 2", got)
	}
	if len(s.GetReportedWorkloadsForCluster("cluster-a", "prod")) != 0 {
		t.Fatal("unreported namespace should be empty")
	}
	if all := s.GetReportedWorkloadsForCluster("cluster-b", ""); len(all) != 1 {
		t.Fatalf("cluster-b all namespaces = %d workloads, want 1", len(all))
	}
}

func TestSetReportedWorkloadsForClusterClearsNamespace(t *testing.T) {
	s := &Store{}
	s.SetReportedWorkloadsForCluster("cluster-a", "dev", []ReportedWorkload{{Name: "api", Namespace: "dev"}})
	s.SetReportedWorkloadsForCluster("cluster-a", "dev", nil)
	if got := s.GetReportedWorkloadsForCluster("cluster-a", "dev"); len(got) != 0 {
		t.Fatalf("got %+v, want cleared", got)
	}
}
