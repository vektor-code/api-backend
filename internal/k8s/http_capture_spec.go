package k8s

import (
	"os"
	"strings"
)

// Safe HTTP headers the official OTel SDK will copy onto spans. Authorization
// and Cookie are intentionally absent.
const httpCaptureHeaderList = "content-type,content-length,user-agent,accept,accept-language,origin,referer,x-request-id,x-correlation-id,x-forwarded-for,x-forwarded-proto,location,retry-after"

func httpCaptureEnv() []interface{} {
	return []interface{}{
		kv("CRNET_HTTP_CAPTURE", envOr("CRNET_HTTP_CAPTURE", "true")),
		kv("CRNET_HTTP_CAPTURE_MAX_BYTES", envOr("CRNET_HTTP_CAPTURE_MAX_BYTES", "4096")),
		kv("OTEL_INSTRUMENTATION_HTTP_SERVER_CAPTURE_REQUEST_HEADERS", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_SERVER_CAPTURE_RESPONSE_HEADERS", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CLIENT_CAPTURE_REQUEST_HEADERS", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CLIENT_CAPTURE_RESPONSE_HEADERS", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_REQUEST", httpCaptureHeaderList),
		kv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_RESPONSE", httpCaptureHeaderList),
	}
}

func withHTTPCapture(env []interface{}) []interface{} {
	return append(append([]interface{}{}, env...), httpCaptureEnv()...)
}

func crnetAgentImage(kind, official string) string {
	var key string
	switch kind {
	case "java":
		key = "CRNET_AGENT_JAVA_IMAGE"
	case "python":
		key = "CRNET_AGENT_PYTHON_IMAGE"
	case "nodejs":
		key = "CRNET_AGENT_NODEJS_IMAGE"
	}
	if key != "" {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return official
}

func javaInstrumentationSpec(image string) map[string]interface{} {
	return map[string]interface{}{
		"image": crnetAgentImage("java", image),
		"env":   optimizedInstrumentationEnv(),
	}
}

func nodejsInstrumentationSpec(image string) map[string]interface{} {
	return map[string]interface{}{
		"image": crnetAgentImage("nodejs", image),
		"env":   optimizedInstrumentationEnv(),
	}
}

func pythonCaptureInstrumentationSpec(image, endpoint string) map[string]interface{} {
	return pythonInstrumentationSpec(crnetAgentImage("python", image), endpoint)
}

func stripJavaExtensions(obj map[string]interface{}) map[string]interface{} {
	spec, _ := obj["spec"].(map[string]interface{})
	if spec == nil {
		return obj
	}
	java, _ := spec["java"].(map[string]interface{})
	if java == nil {
		return obj
	}
	delete(java, "extensions")
	return obj
}
