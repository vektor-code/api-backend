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
