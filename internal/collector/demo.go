package collector

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/store"
)

// DemoGenerator generates realistic demo traces
type DemoGenerator struct {
	store  *store.Store
	onSpan SpanHandler
}

func NewDemoGenerator(s *store.Store, onSpan SpanHandler) *DemoGenerator {
	return &DemoGenerator{store: s, onSpan: onSpan}
}

var demoOperations = []string{
	"GET /api/v1/documents",
	"POST /api/v1/documents",
	"GET /api/v1/users",
	"POST /api/v1/auth/login",
	"GET /api/v1/dashboard",
	"POST /api/v1/submit",
	"GET /api/v1/reports",
	"PUT /api/v1/documents/:id",
	"DELETE /api/v1/documents/:id",
	"GET /api/v1/notifications",
	"POST /api/v1/payments",
	"GET /api/v1/status",
}

// Start begins generating demo traces at regular intervals
func (d *DemoGenerator) Start(interval time.Duration) {
	log.Printf("[demo] generating demo traces every %v", interval)

	// Generate initial batch
	for i := 0; i < 30; i++ {
		d.generateTrace()
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			d.generateTrace()
		}
	}()
}

func (d *DemoGenerator) generateTrace() {
	// Dynamically discover namespaces from Kubernetes reported pods
	namespaces := d.store.GetReportedNamespaces()
	if len(namespaces) == 0 {
		return
	}

	ns := namespaces[rand.Intn(len(namespaces))]

	// Dynamically discover services from Kubernetes pod labels
	rawServices := d.store.GetServicesForNamespace(ns)
	var services []string
	for _, svc := range rawServices {
		svcLower := strings.ToLower(svc)
		if strings.Contains(svcLower, "frontend") || strings.Contains(svcLower, "ui") || strings.Contains(svcLower, "client") {
			continue
		}
		services = append(services, svc)
	}
	if len(services) == 0 {
		return
	}

	traceID := uuid.New().String()
	traceID = fmt.Sprintf("%s%s", traceID[:8], traceID[9:13])

	now := time.Now()
	operation := demoOperations[rand.Intn(len(demoOperations))]

	rootSvc := services[0]
	rootDuration := 50 + rand.Float64()*450
	isError := rand.Float64() < 0.08

	rootSpanID := shortID()
	rootSpan := &models.Span{
		TraceID:     traceID,
		SpanID:      rootSpanID,
		Name:        operation,
		ServiceName: rootSvc,
		Namespace:   ns,
		PodName:     d.getRealPodName(ns, rootSvc),
		StartTime:   now,
		EndTime:     now.Add(time.Duration(rootDuration) * time.Millisecond),
		DurationMs:  rootDuration,
		Status:      models.SpanStatusOK,
		Kind:        models.SpanKindServer,
		Attributes: map[string]string{
			"http.method":            operation[:3],
			"http.url":               operation[4:],
			"telemetry.sdk.language": d.getServiceLanguageFromK8s(ns, rootSvc),
		},
	}

	if isError {
		rootSpan.Status = models.SpanStatusError
		rootSpan.StatusCode = 500
		rootSpan.Error = "Internal Server Error"
	} else {
		rootSpan.StatusCode = 200
	}

	d.saveAndBroadcast(rootSpan)

	currentOffset := 5.0
	parentID := rootSpanID
	numChildren := 0
	if len(services) > 1 {
		numChildren = 1 + rand.Intn(len(services)-1)
	}

	for i := 0; i < numChildren; i++ {
		svcIdx := 1 + (i % (len(services) - 1))
		svc := services[svcIdx]
		childDuration := 5 + rand.Float64()*float64(rootDuration)/float64(numChildren+1)
		childStart := now.Add(time.Duration(currentOffset) * time.Millisecond)

		childSpanID := shortID()
		child := &models.Span{
			TraceID:      traceID,
			SpanID:       childSpanID,
			ParentSpanID: parentID,
			Name:         fmt.Sprintf("%s.process", svc),
			ServiceName:  svc,
			Namespace:    ns,
			PodName:      d.getRealPodName(ns, svc),
			StartTime:    childStart,
			EndTime:      childStart.Add(time.Duration(childDuration) * time.Millisecond),
			DurationMs:   childDuration,
			Status:       models.SpanStatusOK,
			Kind:         models.SpanKindServer,
			Attributes: map[string]string{
				"component":              svc,
				"telemetry.sdk.language": d.getServiceLanguageFromK8s(ns, svc),
			},
		}

		if isError && i == numChildren-1 {
			child.Status = models.SpanStatusError
			child.Error = "connection refused"
		}

		d.saveAndBroadcast(child)

		if rand.Float64() < 0.6 && !strings.Contains(svc, "-frontend") {
			dbDuration := 1 + rand.Float64()*childDuration*0.6
			dbStart := childStart.Add(2 * time.Millisecond)
			dbSpan := &models.Span{
				TraceID:      traceID,
				SpanID:       shortID(),
				ParentSpanID: childSpanID,
				Name:         randomDBOp(),
				ServiceName:  svc,
				Namespace:    ns,
				PodName:      child.PodName,
				StartTime:    dbStart,
				EndTime:      dbStart.Add(time.Duration(dbDuration) * time.Millisecond),
				DurationMs:   dbDuration,
				Status:       models.SpanStatusOK,
				Kind:         models.SpanKindClient,
				Attributes: map[string]string{
					"db.system":    "postgresql",
					"db.statement": randomDBOp(),
				},
			}
			d.saveAndBroadcast(dbSpan)
		}

		currentOffset += childDuration + 3
		parentID = childSpanID
	}
}

func (d *DemoGenerator) saveAndBroadcast(span *models.Span) {
	if err := d.store.SaveSpan(span); err != nil {
		log.Printf("[demo] save error: %v", err)
	}
	if d.onSpan != nil {
		d.onSpan(span)
	}
}

func shortID() string {
	id := uuid.New()
	return fmt.Sprintf("%x", id[:8])
}

func randomDBOp() string {
	ops := []string{
		"SELECT * FROM documents WHERE id = $1",
		"INSERT INTO audit_log (action, user_id) VALUES ($1, $2)",
		"UPDATE documents SET status = $1 WHERE id = $2",
		"SELECT COUNT(*) FROM notifications WHERE read = false",
		"SELECT u.*, r.name FROM users u JOIN roles r ON u.role_id = r.id",
		"DELETE FROM sessions WHERE expires_at < NOW()",
	}
	return ops[rand.Intn(len(ops))]
}

// getServiceLanguageFromK8s uses the reported pod language from Kubernetes,
// falling back to name-based heuristics only when K8s data is not available.
func (d *DemoGenerator) getServiceLanguageFromK8s(ns, svc string) string {
	// First try the dynamically detected language from Kubernetes pod inspection
	lang := d.store.GetReportedLanguageForService(ns, svc)
	if lang != "" {
		// Normalize common language names
		l := strings.ToLower(lang)
		if l == "nodejs" || l == "js" || l == "typescript" {
			return "javascript"
		}
		return l
	}

	// Fallback: name-based heuristics
	s := strings.ToLower(svc)
	if strings.Contains(s, "frontend") || strings.Contains(s, "ui") || strings.Contains(s, "client") {
		return ""
	}
	return "go"
}

func (d *DemoGenerator) getRealPodName(ns, svc string) string {
	pods := d.store.GetReportedPods(ns)
	for _, p := range pods {
		if p.MatchesService(svc) {
			return p.Name
		}
	}
	// If no matching pod found, return the service name itself (no random generation)
	return svc
}
