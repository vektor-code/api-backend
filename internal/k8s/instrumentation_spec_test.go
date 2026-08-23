package k8s

import "testing"

func TestBuildInstrumentationObjectAddsPythonEnvOverrides(t *testing.T) {
	inst := BuildInstrumentationObject("troni-dev", "crnet-apm")
	spec := inst["spec"].(map[string]interface{})
	python := spec["python"].(map[string]interface{})
	env := python["env"].([]interface{})

	if got := envValue(env, "OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://agent-backend.crnet-apm.svc.cluster.local:4318" {
		t.Fatalf("python OTEL_EXPORTER_OTLP_ENDPOINT = %q, want HTTP endpoint", got)
	}
	if got := envValue(env, "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"); got != "http/protobuf" {
		t.Fatalf("python OTEL_EXPORTER_OTLP_TRACES_PROTOCOL = %q, want http/protobuf", got)
	}
	if got := envValue(env, "OTEL_METRICS_EXPORTER"); got != "none" {
		t.Fatalf("python OTEL_METRICS_EXPORTER = %q, want none", got)
	}
}

func TestBuildInstrumentationObjectUsesFastExport(t *testing.T) {
	inst := BuildInstrumentationObject("troni-dev", "crnet-apm")
	spec := inst["spec"].(map[string]interface{})
	if got := envValue(spec["env"].([]interface{}), "OTEL_BSP_SCHEDULE_DELAY"); got != "500" {
		t.Fatalf("OTEL_BSP_SCHEDULE_DELAY = %q, want 500", got)
	}
	goSpec := spec["go"].(map[string]interface{})
	if got := envValue(goSpec["env"].([]interface{}), "OTEL_EXPORTER_OTLP_PROTOCOL"); got != "http/protobuf" {
		t.Fatalf("go protocol = %q, want http/protobuf", got)
	}
}

func TestBuildCompatibleInstrumentationObjectOmitsOptionalFields(t *testing.T) {
	spec := BuildCompatibleInstrumentationObject("troni-dev", "crnet-apm")["spec"].(map[string]interface{})
	if _, ok := spec["apacheHttpd"]; ok {
		t.Fatal("compatible spec should omit apacheHttpd")
	}
	if _, ok := spec["java"]; !ok {
		t.Fatal("compatible spec should still include java")
	}
}

func envValue(env []interface{}, name string) string {
	for _, item := range env {
		kv, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if kv["name"] == name {
			value, _ := kv["value"].(string)
			return value
		}
	}
	return ""
}
