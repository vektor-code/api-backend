package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/store"
	"github.com/kubetrace/shared/w3c"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// SpanHandler is a callback invoked for every processed span (for live streaming)
type SpanHandler func(span *models.Span)

// Receiver handles OTLP/HTTP trace ingestion
type Receiver struct {
	store   *store.Store
	sampler *AdaptiveSampler
	onSpan  SpanHandler
}

// NewReceiver creates a new OTLP HTTP receiver
func NewReceiver(s *store.Store, sampler *AdaptiveSampler, onSpan SpanHandler) *Receiver {
	return &Receiver{store: s, sampler: sampler, onSpan: onSpan}
}

// HandleHTTP handles OTLP/HTTP POST /v1/traces (protobuf or JSON)
func (r *Receiver) HandleHTTP(c *fiber.Ctx) error {
	contentType := string(c.Request().Header.ContentType())
	body := c.Body()

	var req colpb.ExportTraceServiceRequest

	switch {
	case contentType == "application/x-protobuf" || contentType == "application/proto":
		if err := proto.Unmarshal(body, &req); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid protobuf"})
		}
	default:
		// Try JSON OTLP
		if err := json.Unmarshal(body, &req); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON"})
		}
	}

	go r.processRequest(&req)
	return c.Status(http.StatusOK).JSON(fiber.Map{"partialSuccess": fiber.Map{}})
}

func (r *Receiver) processRequest(req *colpb.ExportTraceServiceRequest) {
	for _, rs := range req.ResourceSpans {
		// Extract service name and namespace from resource attributes
		serviceName := "unknown"
		namespace := "default"
		clusterName := os.Getenv("CLUSTER_NAME")
		if clusterName == "" {
			clusterName = os.Getenv("KUBERNETES_CLUSTER_NAME")
		}
		if clusterName == "" {
			clusterName = "default"
		}
		podName := ""
		nodeName := ""

		resAttrs := make(map[string]string)
		for _, attr := range rs.Resource.GetAttributes() {
			k := attr.Key
			v := stringVal(attr.Value)
			resAttrs[k] = v
			switch k {
			case "service.name":
				serviceName = v
			case "k8s.namespace.name":
				namespace = v
			case "k8s.cluster.name":
				clusterName = v
			case "k8s.pod.name":
				podName = v
			case "k8s.node.name":
				nodeName = v
			}
		}

		serviceName = resolveServiceName(resAttrs)
		if namespace == "default" {
			if ns := strings.TrimSpace(resAttrs["service.namespace"]); ns != "" {
				namespace = ns
			}
		}

		if r.store.IsNamespaceDisabled(namespace) {
			continue
		}

		for _, ss := range rs.ScopeSpans {
			scopeName := ""
			scopeVersion := ""
			if ss.Scope != nil {
				scopeName = ss.Scope.Name
				scopeVersion = ss.Scope.Version
			}
			for _, sp := range ss.Spans {
				span := convertSpan(sp, serviceName, namespace, clusterName, podName, nodeName)
				if span.Attributes == nil {
					span.Attributes = make(map[string]string)
				}
				if scopeName != "" {
					span.Attributes["otel.library.name"] = scopeName
				}
				if scopeVersion != "" {
					span.Attributes["otel.library.version"] = scopeVersion
				}
				for k, v := range resAttrs {
					if _, ok := span.Attributes[k]; !ok {
						span.Attributes[k] = v
					}
				}
				isError := span.Status == models.SpanStatusError
				if !r.applySampling(span, isError) {
					continue
				}
				if err := r.store.SaveSpan(span); err != nil {
					log.Printf("[collector] save span error: %v", err)
					continue
				}
				if r.onSpan != nil {
					r.onSpan(span)
				}
			}
		}
	}
}

func convertSpan(pb *tracepb.Span, svc, ns, cluster, pod, node string) *models.Span {
	startTime := time.Unix(0, int64(pb.StartTimeUnixNano))
	endTime := time.Unix(0, int64(pb.EndTimeUnixNano))
	// Unsigned subtraction underflows when end <= start (clock skew, or an
	// unfinished span with end=0), producing a ~1.8e10 ms outlier that inflates
	// every latency percentile. Clamp to zero instead.
	var durationMs float64
	if pb.EndTimeUnixNano > pb.StartTimeUnixNano {
		durationMs = float64(pb.EndTimeUnixNano-pb.StartTimeUnixNano) / 1e6
	}

	status := models.SpanStatusUnset
	if pb.Status != nil {
		switch pb.Status.Code {
		case tracepb.Status_STATUS_CODE_OK:
			status = models.SpanStatusOK
		case tracepb.Status_STATUS_CODE_ERROR:
			status = models.SpanStatusError
		}
	}

	kind := models.SpanKindInternal
	switch pb.Kind {
	case tracepb.Span_SPAN_KIND_SERVER:
		kind = models.SpanKindServer
	case tracepb.Span_SPAN_KIND_CLIENT:
		kind = models.SpanKindClient
	case tracepb.Span_SPAN_KIND_PRODUCER:
		kind = models.SpanKindProducer
	case tracepb.Span_SPAN_KIND_CONSUMER:
		kind = models.SpanKindConsumer
	}

	attrs := make(map[string]string, len(pb.Attributes))
	for _, a := range pb.Attributes {
		attrs[a.Key] = stringVal(a.Value)
	}

	var events []models.SpanEvent
	for _, ev := range pb.Events {
		evAttrs := make(map[string]string)
		for _, a := range ev.Attributes {
			evAttrs[a.Key] = stringVal(a.Value)
		}
		events = append(events, models.SpanEvent{
			Name:       ev.Name,
			Timestamp:  time.Unix(0, int64(ev.TimeUnixNano)),
			Attributes: evAttrs,
		})
	}

	errMsg := ""
	if pb.Status != nil && pb.Status.Message != "" {
		errMsg = pb.Status.Message
	}

	var links []models.SpanLink
	for _, ln := range pb.Links {
		linkAttrs := make(map[string]string, len(ln.Attributes))
		for _, a := range ln.Attributes {
			linkAttrs[a.Key] = stringVal(a.Value)
		}
		links = append(links, models.SpanLink{
			TraceID:    fmt.Sprintf("%x", ln.TraceId),
			SpanID:     fmt.Sprintf("%x", ln.SpanId),
			Attributes: linkAttrs,
		})
	}

	traceID := fmt.Sprintf("%x", pb.TraceId)
	spanID := fmt.Sprintf("%x", pb.SpanId)

	// Persist the trace context from the span itself. The UI previously
	// assembled a traceparent out of the stored IDs and a hardcoded "01",
	// which is wrong whenever the span was not sampled and unparseable when an
	// ID is short, so the header is captured here at ingest instead.
	sampled := w3c.SampledFromOTLPFlags(pb.Flags)
	if tp := w3c.FormatTraceparent(traceID, spanID, sampled); tp != "" {
		attrs["w3c.traceparent"] = tp
		attrs["w3c.trace_flags"] = w3c.TraceFlags(sampled)
	}

	span := &models.Span{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: fmt.Sprintf("%x", pb.ParentSpanId),
		Name:         pb.Name,
		ServiceName:  svc,
		Namespace:    ns,
		Cluster:      cluster,
		PodName:      pod,
		NodeName:     node,
		StartTime:    startTime,
		EndTime:      endTime,
		DurationMs:   durationMs,
		Status:       status,
		Kind:         kind,
		Attributes:   attrs,
		Events:       events,
		Links:        links,
		Error:        errMsg,
	}
	if status != models.SpanStatusError && spanLooksErrored(attrs, events) {
		span.Status = models.SpanStatusError
	}
	if len(links) > 0 {
		if raw, err := json.Marshal(links); err == nil {
			if span.Attributes == nil {
				span.Attributes = map[string]string{}
			}
			span.Attributes["otel.span.links"] = string(raw)
		}
	}
	return span
}

func resolveServiceName(resAttrs map[string]string) string {
	name := strings.TrimSpace(resAttrs["service.name"])
	if !isPlaceholderServiceName(name) {
		return name
	}
	for _, k := range []string{
		"k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name",
		"k8s.cronjob.name", "k8s.job.name", "k8s.container.name",
	} {
		if v := strings.TrimSpace(resAttrs[k]); v != "" {
			return v
		}
	}
	if name != "" {
		return name
	}
	return "unknown"
}

func isPlaceholderServiceName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return n == "" || n == "unknown" || n == "unknown_service" || strings.HasPrefix(n, "unknown_service:")
}

func spanLooksErrored(attrs map[string]string, events []models.SpanEvent) bool {
	for _, key := range []string{"http.response.status_code", "http.status_code", "http.status"} {
		if code, err := strconv.Atoi(strings.TrimSpace(attrs[key])); err == nil && code >= 500 {
			return true
		}
	}
	for _, key := range []string{"rpc.grpc.status_code", "grpc.status_code"} {
		if code, err := strconv.Atoi(strings.TrimSpace(attrs[key])); err == nil && code != 0 {
			return true
		}
	}
	for _, ev := range events {
		if strings.EqualFold(ev.Name, "exception") {
			return true
		}
	}
	return false
}

func stringVal(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch vv := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return vv.StringValue
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", vv.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", vv.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		if vv.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

// IngestJSON accepts a simplified JSON span for testing / non-OTel clients
func (r *Receiver) IngestJSON(c *fiber.Ctx) error {
	var spans []*models.Span
	if err := c.BodyParser(&spans); err != nil {
		// Try single span
		var span models.Span
		if err2 := c.BodyParser(&span); err2 != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
		}
		spans = []*models.Span{&span}
	}

	for _, sp := range spans {
		if sp.StartTime.IsZero() {
			sp.StartTime = time.Now()
		}
		if sp.EndTime.IsZero() {
			sp.EndTime = sp.StartTime.Add(time.Duration(sp.DurationMs) * time.Millisecond)
		}
		isError := sp.Status == models.SpanStatusError
		if !r.applySampling(sp, isError) {
			continue
		}
		if err := r.store.SaveSpan(sp); err != nil {
			log.Printf("[collector] ingest error: %v", err)
		}
		if r.onSpan != nil {
			r.onSpan(sp)
		}
	}
	return c.JSON(fiber.Map{"ingested": len(spans)})
}

// ProbeHTTP is a simple liveness probe for the collector
func ProbeHTTP(w http.ResponseWriter, _ *http.Request) {
	io.WriteString(w, "ok")
}

// applySampling decides whether to keep a span and, when it survives a
// probabilistic decision, records the weight it carries.
//
// Recording the weight is what makes counts honest. Under load the adaptive
// sampler drops to as little as 1%, and without this every "calls" figure
// derived from stored spans would under-report by up to 100x with nothing on
// screen to say so.
func (r *Receiver) applySampling(span *models.Span, isError bool) bool {
	if r.sampler == nil {
		return true
	}
	forceKeep := isError || span.DurationMs >= slowTraceKeepMs()
	decision := r.sampler.Sample(span.TraceID, forceKeep)
	if !decision.Keep {
		return false
	}
	if decision.AdjustedCount > 1 {
		if span.Attributes == nil {
			span.Attributes = make(map[string]string)
		}
		span.Attributes[AttrSampleProbability] = strconv.FormatFloat(decision.Probability, 'g', 6, 64)
		span.Attributes[AttrAdjustedCount] = strconv.FormatFloat(decision.AdjustedCount, 'g', 6, 64)
	}
	return true
}
