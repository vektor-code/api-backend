package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/store"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// SpanHandler is a callback invoked for every processed span (for live streaming)
type SpanHandler func(span *models.Span)

// Receiver handles OTLP/HTTP trace ingestion
type Receiver struct {
	store      *store.Store
	sampler    *AdaptiveSampler
	onSpan     SpanHandler
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
				if r.sampler != nil && !r.sampler.ShouldSample(span.TraceID, isError) {
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
	durationMs := float64(pb.EndTimeUnixNano-pb.StartTimeUnixNano) / 1e6

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

	return &models.Span{
		TraceID:      fmt.Sprintf("%x", pb.TraceId),
		SpanID:       fmt.Sprintf("%x", pb.SpanId),
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
		Error:        errMsg,
	}
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
		if r.sampler != nil && !r.sampler.ShouldSample(sp.TraceID, isError) {
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
