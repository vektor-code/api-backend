package tracediag

import "strings"

// LivePlanFor decides whether live Kubernetes work could add evidence.
// It is pure policy: no cluster calls, no waiting.
func LivePlanFor(d *Diagnosis) LivePlan {
	if d == nil {
		return LivePlan{Recommended: false, MaxLevel: 0, Reason: "No failure to verify"}
	}
	hasEvidence := func(codes ...string) bool {
		if len(d.Evidence) == 0 {
			return false
		}
		seen := map[string]bool{}
		for _, e := range d.Evidence {
			if e.Code != "" {
				seen[e.Code] = true
			}
		}
		for _, code := range codes {
			if !seen[code] {
				return false
			}
		}
		return true
	}
	hasHTTPEvidence := func(prefix string) bool {
		if len(d.Evidence) == 0 {
			return false
		}
		for _, e := range d.Evidence {
			if strings.HasPrefix(e.Code, prefix) {
				return true
			}
		}
		return false
	}

	switch d.Classification {
	case ClassificationInstrumentationAnomaly, ClassificationTraceContextAnomaly, ClassificationDuplicateInstrumentation:
		return LivePlan{
			Recommended: false,
			MaxLevel:    0,
			Reason:      "Kubernetes verification not required: telemetry is internally sufficient",
		}
	case ClassificationUnknown:
		return LivePlan{
			Recommended: false,
			MaxLevel:    0,
			Reason:      "Live probes would not explain this span; telemetry is already inconclusive",
		}
	case ClassificationClientError:
		return LivePlan{
			Recommended: false,
			MaxLevel:    0,
			Reason:      "Not performed — telemetry was sufficient to classify this as an application-level HTTP 4xx error",
		}
	case ClassificationApplicationError, ClassificationDownstreamError:
		// If the HTTP SERVER span looks internally coherent (method/path/status/duration) we treat it
		// as application evidence and avoid live Kubernetes work.
		coherent := d.Confidence == ConfidenceHigh &&
			hasEvidence("valid_http_method", "valid_url_path", "reasonable_duration") &&
			hasHTTPEvidence("http_")

		if coherent {
			return LivePlan{
				Recommended: false,
				MaxLevel:    0,
				Reason:      "Not performed — telemetry was sufficient to classify this as an application-level HTTP error",
			}
		}

		// Otherwise, do a cheap existence/state check (pod readiness & service/endpoints where applicable).
		// Progressive deeper probing is reserved for true transport/infrastructure-shaped diagnoses.
		if d.Confidence == ConfidenceMedium {
			return LivePlan{
				Recommended: true,
				MaxLevel:    1,
				Reason:      "HTTP failure needs confirmation of target workload state",
			}
		}

		return LivePlan{
			Recommended: true,
			MaxLevel:    1,
			Reason:      "HTTP failure needs confirmation of target workload state",
		}
	case ClassificationNetworkError, ClassificationTimeout:
		return LivePlan{
			Recommended: true,
			MaxLevel:    3,
			Reason:      "Telemetry suggests transport or infrastructure; verify from the source workload",
		}
	default:
		return LivePlan{Recommended: false, MaxLevel: 0, Reason: "Kubernetes verification not required"}
	}
}
