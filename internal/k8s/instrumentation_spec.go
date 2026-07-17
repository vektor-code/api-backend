package k8s

import "fmt"

// InstrumentationName returns the standard Instrumentation CR name for a namespace.
func InstrumentationName(namespace string) string {
	return namespace + "-instrumentation"
}

// BuildInstrumentationObject returns an OpenTelemetry Instrumentation CR matching the platform standard.
func BuildInstrumentationObject(namespace, agentNamespace string) map[string]interface{} {
	if agentNamespace == "" {
		agentNamespace = "trace-prod"
	}
	name := InstrumentationName(namespace)
	endpoint := fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4317", agentNamespace)

	return map[string]interface{}{
		"apiVersion": "opentelemetry.io/v1alpha1",
		"kind":       "Instrumentation",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]interface{}{
			"exporter": map[string]interface{}{
				"endpoint": endpoint,
			},
			"propagators": []interface{}{
				"tracecontext",
				"baggage",
				"b3",
			},
			"sampler": map[string]interface{}{
				"type": "parentbased_always_on",
			},
			"env": []interface{}{
				map[string]interface{}{"name": "OTEL_EXPORTER_OTLP_PROTOCOL", "value": "grpc"},
				map[string]interface{}{"name": "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "value": "grpc"},
				map[string]interface{}{"name": "OTEL_METRICS_EXPORTER", "value": "none"},
				map[string]interface{}{"name": "OTEL_LOGS_EXPORTER", "value": "none"},
			},
			"java":   map[string]interface{}{},
			"nodejs": map[string]interface{}{},
			"python": map[string]interface{}{},
			"dotnet": map[string]interface{}{},
			"go": map[string]interface{}{
				"image": "ghcr.io/open-telemetry/opentelemetry-go-instrumentation/autoinstrumentation-go:v0.24.0",
				"env": []interface{}{
					map[string]interface{}{
						"name":  "OTEL_EXPORTER_OTLP_ENDPOINT",
						"value": fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4317", agentNamespace),
					},
				},
				"resourceRequirements": map[string]interface{}{
					"limits": map[string]interface{}{
						"cpu":    "500m",
						"memory": "256Mi",
					},
					"requests": map[string]interface{}{
						"cpu":    "50m",
						"memory": "64Mi",
					},
				},
			},
		},
	}
}
