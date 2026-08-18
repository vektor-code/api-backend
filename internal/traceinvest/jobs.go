package traceinvest

import (
	"sort"
	"sync"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/api-backend/internal/tracediag"
)

type jobRecord struct {
	Intent      Intent
	Status      string
	TraceIDs    map[string]struct{}
	Result      *Result
	CreatedAt   time.Time
	ClaimedAt   time.Time
	ClaimUntil  time.Time
	ResultUntil time.Time
}

// Store holds investigation intents and agent results. The API never executes
// Kubernetes checks; it only records what should be verified and what came back.
type Store struct {
	mu   sync.Mutex
	jobs map[string]*jobRecord
}

func NewStore() *Store {
	return &Store{jobs: map[string]*jobRecord{}}
}

// Request enqueues or coalesces a live investigation. It never blocks on the agent.
func (s *Store) Request(trace *models.Trace, diag *tracediag.Diagnosis, clusterID string, agentOnline bool, now time.Time) *Report {
	if diag == nil {
		return &Report{TraceID: traceID(trace, nil), Status: StatusSkipped, SkipReason: "No failure to verify"}
	}
	plan := tracediag.LivePlanFor(diag)
	if !plan.Recommended || plan.MaxLevel <= 0 {
		return &Report{
			TraceID:    diag.TraceID,
			Status:     StatusSkipped,
			SkipReason: plan.Reason,
			Conclusion: "Kubernetes verification not required",
		}
	}
	intent, ok := BuildIntent(trace, diag, clusterID, now)
	if !ok {
		return &Report{
			TraceID:    diag.TraceID,
			Status:     StatusSkipped,
			SkipReason: plan.Reason,
			Conclusion: "Kubernetes verification not required",
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked(now)

	rec := s.jobs[intent.Fingerprint]
	if rec != nil && rec.Result != nil && now.Before(rec.ResultUntil) {
		if intent.TraceID != "" {
			rec.TraceIDs[intent.TraceID] = struct{}{}
		}
		return reportFrom(rec, intent.TraceID, true)
	}
	if rec != nil && rec.Result != nil && !now.Before(rec.ResultUntil) {
		rec.Result = nil
	}
	if !agentOnline {
		return &Report{
			TraceID:     diag.TraceID,
			Status:      StatusUnavailable,
			SkipReason:  "Agent is not currently reporting from this cluster",
			Fingerprint: intent.Fingerprint,
			CacheKey:    intent.Fingerprint,
		}
	}
	if rec == nil {
		rec = &jobRecord{
			Intent:    intent,
			Status:    StatusPending,
			TraceIDs:  map[string]struct{}{},
			CreatedAt: now,
		}
		s.jobs[intent.Fingerprint] = rec
	}
	if intent.TraceID != "" {
		if rec.TraceIDs == nil {
			rec.TraceIDs = map[string]struct{}{}
		}
		rec.TraceIDs[intent.TraceID] = struct{}{}
	}
	rec.Intent.TraceID = intent.TraceID
	if rec.Intent.ExpiresAt.Before(now) && rec.Result == nil {
		rec.Status = StatusExpired
		return &Report{
			TraceID:     intent.TraceID,
			Status:      StatusExpired,
			SkipReason:  "Agent did not complete investigation before the job expired",
			Fingerprint: intent.Fingerprint,
			CacheKey:    intent.Fingerprint,
		}
	}
	if rec.Status == StatusExpired {
		return &Report{
			TraceID:     intent.TraceID,
			Status:      StatusExpired,
			SkipReason:  "Agent did not complete investigation before the job expired",
			Fingerprint: intent.Fingerprint,
			CacheKey:    intent.Fingerprint,
		}
	}
	if rec.Status != StatusPending && rec.Status != "running" {
		rec.Status = StatusPending
		rec.ClaimUntil = time.Time{}
	}
	return reportFrom(rec, intent.TraceID, false)
}

// Claim returns pending jobs for one cluster and marks them running.
// The agent is the only caller; the UI never claims.
func (s *Store) Claim(clusterID string, limit int, now time.Time) []Intent {
	if clusterID == "" {
		clusterID = "default"
	}
	if limit <= 0 {
		limit = 5
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked(now)

	out := make([]Intent, 0, limit)
	for _, rec := range s.jobs {
		if len(out) >= limit {
			break
		}
		if rec.Intent.ClusterID != clusterID {
			continue
		}
		if rec.Result != nil && now.Before(rec.ResultUntil) {
			continue
		}
		if rec.Intent.ExpiresAt.Before(now) {
			rec.Status = StatusExpired
			continue
		}
		leased := rec.Status == StatusPending || rec.ClaimUntil.Before(now)
		if !leased {
			continue
		}
		rec.Status = "running"
		rec.ClaimedAt = now
		rec.ClaimUntil = now.Add(claimTTL)
		cp := rec.Intent
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fingerprint < out[j].Fingerprint })
	return out
}

// Submit stores an agent result against the fingerprint. All traces that
// requested the same evidence share this one result.
func (s *Store) Submit(res Result, now time.Time) *Report {
	if res.Fingerprint == "" {
		return &Report{Status: StatusUnavailable, SkipReason: "Missing fingerprint"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked(now)
	rec := s.jobs[res.Fingerprint]
	if rec == nil {
		rec = &jobRecord{
			Intent:    Intent{Fingerprint: res.Fingerprint, TraceID: res.TraceID},
			TraceIDs:  map[string]struct{}{},
			CreatedAt: now,
		}
		s.jobs[res.Fingerprint] = rec
	}
	if res.TraceID != "" {
		rec.TraceIDs[res.TraceID] = struct{}{}
	}
	cp := res
	rec.Result = &cp
	rec.Status = res.Status
	if rec.Status == "" {
		rec.Status = StatusComplete
	}
	rec.ResultUntil = now.Add(resultTTL)
	return reportFrom(rec, res.TraceID, false)
}

// Lookup returns the current report for a trace/fingerprint without enqueueing.
func (s *Store) Lookup(fingerprint, traceID string, now time.Time) *Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked(now)
	rec := s.jobs[fingerprint]
	if rec == nil {
		return nil
	}
	return reportFrom(rec, traceID, rec.Result != nil && now.Before(rec.ResultUntil))
}

func (s *Store) gcLocked(now time.Time) {
	for key, rec := range s.jobs {
		if rec.Result != nil && now.After(rec.ResultUntil) && rec.Intent.ExpiresAt.Before(now) {
			delete(s.jobs, key)
			continue
		}
		if rec.Result == nil && rec.Intent.ExpiresAt.Before(now.Add(-time.Minute)) {
			delete(s.jobs, key)
		}
	}
}

func reportFrom(rec *jobRecord, traceID string, cached bool) *Report {
	if rec == nil {
		return nil
	}
	if traceID == "" {
		traceID = rec.Intent.TraceID
	}
	out := &Report{
		TraceID:      traceID,
		Status:       rec.Status,
		Fingerprint:  rec.Intent.Fingerprint,
		CacheKey:     rec.Intent.Fingerprint,
		Cached:       cached,
		ReferencedBy: len(rec.TraceIDs),
	}
	if out.Status == "running" {
		out.Status = StatusPending
	}
	if rec.Result == nil {
		if out.Status == StatusPending {
			out.SkipReason = ""
		}
		return out
	}
	out.Status = rec.Result.Status
	out.LevelReached = rec.Result.LevelReached
	out.SkipReason = rec.Result.SkipReason
	out.Inference = rec.Result.Inference
	out.Conclusion = rec.Result.Inference
	out.Confidence = rec.Result.Confidence
	out.Observations = rec.Result.Observations
	out.DurationMs = rec.Result.DurationMs
	out.Checks = checksFrom(rec.Result.Observations)
	return out
}

func checksFrom(obs []Observation) []Check {
	var out []Check
	for _, o := range obs {
		if o.Kind != KindObserved {
			continue
		}
		ok := false
		if o.OK != nil {
			ok = *o.OK
		}
		out = append(out, Check{
			Level:  o.Level,
			Code:   o.Code,
			OK:     ok,
			Detail: o.Message,
			Pod:    o.Pod,
		})
	}
	return out
}
