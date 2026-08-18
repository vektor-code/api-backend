package tracediag

// LivePlanFor decides whether live Kubernetes work could add evidence.
// It is pure policy: no cluster calls, no waiting.
func LivePlanFor(d *Diagnosis) LivePlan {
	if d == nil {
		return LivePlan{Recommended: false, MaxLevel: 0, Reason: "No failure to verify"}
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
			Recommended: true,
			MaxLevel:    1,
			Reason:      "HTTP 4xx is explained by telemetry; confirm the target workload still exists",
		}
	case ClassificationApplicationError, ClassificationDownstreamError:
		return LivePlan{
			Recommended: true,
			MaxLevel:    3,
			Reason:      "Confirm the recorded HTTP failure from the same workload context",
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
