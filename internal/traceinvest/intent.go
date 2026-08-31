package traceinvest

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

// BuildIntent turns a Level-0 diagnosis into a structured investigation
// request. It never includes a command; the agent chooses the implementation.
func BuildIntent(trace *models.Trace, diag *tracediag.Diagnosis, clusterID string, now time.Time) (Intent, bool) {
	plan := tracediag.LivePlanFor(diag)
	if !plan.Recommended || plan.MaxLevel <= 0 {
		return Intent{}, false
	}
	target := ExtractTarget(trace, diag)
	invType := investigationType(diag)
	if invType == "" {
		return Intent{}, false
	}
	if clusterID == "" {
		clusterID = "default"
	}
	in := Intent{
		InvestigationType:   invType,
		ClusterID:           clusterID,
		Namespace:           target.Namespace,
		SourceWorkload:      target.Workload,
		SourcePod:           target.SourcePod,
		Destination:         destination(target),
		DestinationURL:      target.DestURL,
		DestinationType:     target.DestType,
		DestinationProtocol: target.DestProtocol,
		RecordedHTTP:        target.RecordedHTTP,
		Checks:              ChecksFor(target, plan.MaxLevel),
		MaxLevel:            plan.MaxLevel,
		TraceID:             traceID(trace, diag),
		ExpiresAt:           now.Add(jobTTL),
	}
	in.Fingerprint = Fingerprint(in)
	return in, true
}

func investigationType(diag *tracediag.Diagnosis) string {
	if diag == nil {
		return ""
	}
	switch diag.Classification {
	case tracediag.ClassificationApplicationError, tracediag.ClassificationDownstreamError:
		return TypeDownstreamHTTPFailure
	case tracediag.ClassificationNetworkError, tracediag.ClassificationTimeout:
		return TypeNetworkTimeout
	case tracediag.ClassificationClientError:
		return TypeClientError
	default:
		return ""
	}
}

// ChecksFor is the API's requested set. The agent still allowlists and may
// refuse http_request when MaxLevel is 1.
func ChecksFor(target Target, maxLevel int) []string {
	checks := []string{CheckPodStatus, CheckEvents}
	switch target.DestType {
	case "localhost":
		// Same network namespace/pod context. Service mapping is not applicable.
	case "external_dns", "external_ip":
		// External upstreams are not Kubernetes Services in this cluster.
	default:
		checks = append(checks, CheckServiceResolution, CheckEndpointHealth, CheckNetworkPolicy)
	}
	if maxLevel >= 2 {
		checks = append(checks, CheckHTTPRequest)
	}
	return checks
}

func Fingerprint(in Intent) string {
	checks := append([]string(nil), in.Checks...)
	sort.Strings(checks)
	return strings.Join([]string{
		in.ClusterID,
		in.Namespace,
		in.SourceWorkload,
		in.Destination,
		in.InvestigationType,
		strings.Join(checks, ","),
		strconv.Itoa(in.MaxLevel),
	}, "|")
}

func destination(t Target) string {
	if t.DestHost == "" {
		return ""
	}
	if t.DestPort == "" {
		return t.DestHost
	}
	return t.DestHost + ":" + t.DestPort
}

func traceID(trace *models.Trace, diag *tracediag.Diagnosis) string {
	if diag != nil && diag.TraceID != "" {
		return diag.TraceID
	}
	if trace != nil {
		return trace.TraceID
	}
	return ""
}
