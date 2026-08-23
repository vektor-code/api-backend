package k8s

import (
	"fmt"
	"os"
)

// instrumentationImages pins the OpenTelemetry auto-instrumentation images used
// for injection on remote (token-managed) clusters. Defaults are current, fast,
// stable releases and are overridable per-language via env vars. Old Python
// images bundle a typing_extensions without Sentinel, which breaks pydantic apps.
var instrumentationImages = struct {
	java, nodejs, python, dotnet, golang string
}{
	java:   envOr("OTEL_JAVA_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-java:2.30.0"),
	nodejs: envOr("OTEL_NODEJS_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-nodejs:0.78.0"),
	python: envOr("OTEL_PYTHON_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-python:0.64b0"),
	dotnet: envOr("OTEL_DOTNET_IMAGE", "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-dotnet:1.16.0"),
	golang: envOr("OTEL_GO_IMAGE", "ghcr.io/open-telemetry/opentelemetry-go-instrumentation/autoinstrumentation-go:v0.24.0"),
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func kv(name, value string) map[string]interface{} {
	return map[string]interface{}{"name": name, "value": value}
}

// otelSampler defaults to sampling every trace (parent-respecting) — full
// detection. Override with OTEL_SAMPLER_TYPE + OTEL_SAMPLER_ARG to subsample.
func otelSampler() map[string]interface{} {
	s := map[string]interface{}{"type": envOr("OTEL_SAMPLER_TYPE", "parentbased_always_on")}
	if arg := os.Getenv("OTEL_SAMPLER_ARG"); arg != "" {
		s["argument"] = arg
	}
	return s
}

// optimizedInstrumentationEnv is the low-overhead SDK profile: cheap gRPC
// transport, metrics/logs off, background batched export (no request-path
// latency), and bounded per-span memory. All knobs are env-overridable.
func optimizedInstrumentationEnv() []interface{} {
	env := []interface{}{
		kv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
		kv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc"),
		kv("OTEL_METRICS_EXPORTER", "none"),
		kv("OTEL_LOGS_EXPORTER", "none"),
	}
	return append(env, batchAndLimitEnv()...)
}

func batchAndLimitEnv() []interface{} {
	return []interface{}{
		kv("OTEL_BSP_SCHEDULE_DELAY", envOr("OTEL_BSP_SCHEDULE_DELAY", "500")),
		kv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", envOr("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "512")),
		kv("OTEL_BSP_MAX_QUEUE_SIZE", envOr("OTEL_BSP_MAX_QUEUE_SIZE", "2048")),
		kv("OTEL_BSP_EXPORT_TIMEOUT", envOr("OTEL_BSP_EXPORT_TIMEOUT", "30000")),
		kv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", envOr("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "128")),
		kv("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", envOr("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", "4096")),
	}
}

func languageInstrumentationSpec(image string) map[string]interface{} {
	return map[string]interface{}{
		"image": image,
		"env":   optimizedInstrumentationEnv(),
	}
}

func pythonInstrumentationEnv(endpoint string) []interface{} {
	env := []interface{}{
		kv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint),
		kv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
		kv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"),
		kv("OTEL_METRICS_EXPORTER", "none"),
		kv("OTEL_LOGS_EXPORTER", "none"),
	}
	return append(env, batchAndLimitEnv()...)
}

func pythonInstrumentationSpec(image, endpoint string) map[string]interface{} {
	return map[string]interface{}{
		"image": image,
		"env":   pythonInstrumentationEnv(endpoint),
	}
}

func goInstrumentationSpec(image, httpEndpoint string) map[string]interface{} {
	return map[string]interface{}{
		"image": image,
		"env":   pythonInstrumentationEnv(httpEndpoint),
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
		"securityContext": map[string]interface{}{
			"privileged":               true,
			"allowPrivilegeEscalation": true,
			"capabilities": map[string]interface{}{
				"add": []interface{}{"SYS_PTRACE"},
			},
		},
	}
}

// InstrumentationName returns the standard Instrumentation CR name for a namespace.
func InstrumentationName(namespace string) string {
	return namespace + "-instrumentation"
}

// BuildInstrumentationObject returns an OpenTelemetry Instrumentation CR matching the platform standard.
func BuildInstrumentationObject(namespace, agentNamespace string) map[string]interface{} {
	if agentNamespace == "" {
		agentNamespace = AgentNamespaceFallback()
	}
	name := InstrumentationName(namespace)
	grpcEndpoint := fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4317", agentNamespace)
	httpEndpoint := fmt.Sprintf("http://agent-backend.%s.svc.cluster.local:4318", agentNamespace)

	return map[string]interface{}{
		"apiVersion": "opentelemetry.io/v1alpha1",
		"kind":       "Instrumentation",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]interface{}{
			"exporter": map[string]interface{}{
				"endpoint": grpcEndpoint,
			},
			"propagators": []interface{}{
				"tracecontext",
				"baggage",
				"b3",
				"jaeger",
			},
			"sampler": otelSampler(),
			"env":     optimizedInstrumentationEnv(),
			"resource": map[string]interface{}{
				"addK8sUIDAttributes": true,
			},
			"java":        languageInstrumentationSpec(instrumentationImages.java),
			"nodejs":      languageInstrumentationSpec(instrumentationImages.nodejs),
			"python":      pythonInstrumentationSpec(instrumentationImages.python, httpEndpoint),
			"dotnet":      pythonInstrumentationSpec(instrumentationImages.dotnet, httpEndpoint),
			"nginx":       map[string]interface{}{"env": pythonInstrumentationEnv(httpEndpoint)},
			"apacheHttpd": map[string]interface{}{"env": pythonInstrumentationEnv(httpEndpoint)},
			"go":          goInstrumentationSpec(instrumentationImages.golang, httpEndpoint),
		},
	}
}
