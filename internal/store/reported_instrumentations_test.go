package store

import "testing"

func TestReportedInstrumentationsRoundTrip(t *testing.T) {
	s := &Store{}
	s.SetReportedInstrumentationsForCluster("default", []ReportedInstrumentation{{
		Name:      "shop-instrumentation",
		Namespace: "shop",
		Endpoint:  "http://agent-backend.crnet-apm.svc:4317",
		Sampler:   "parentbased_always_on",
	}})
	got := s.GetReportedInstrumentations("default")
	if len(got) != 1 || got[0].Namespace != "shop" {
		t.Fatalf("got %#v", got)
	}
	ids := s.ReportedInstrumentationClusterIDs()
	if len(ids) != 1 || ids[0] != "default" {
		t.Fatalf("cluster ids = %#v", ids)
	}
}
