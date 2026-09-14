package store

import (
	"sync"
	"time"

	"github.com/kubetrace/api-backend/internal/platformhealth"
)

type platformHealthEntry struct {
	Report   *platformhealth.Report
	Received time.Time
}

type platformHealthRegistry struct {
	mu        sync.RWMutex
	byCluster map[string]platformHealthEntry
}

// AgentPlatformHealthSnapshot is a stored agent push, optionally marked stale.
type AgentPlatformHealthSnapshot struct {
	ClusterID string
	Report    *platformhealth.Report
	Received  time.Time
	Stale     bool
}

func (s *Store) initPlatformHealth() {
	if s.platformHealth == nil {
		s.platformHealth = &platformHealthRegistry{
			byCluster: make(map[string]platformHealthEntry),
		}
	}
}

// SetAgentPlatformHealth stores a health snapshot pushed by agent-backend.
func (s *Store) SetAgentPlatformHealth(clusterID string, report *platformhealth.Report) {
	if report == nil {
		return
	}
	if clusterID == "" {
		clusterID = "default"
	}
	s.initPlatformHealth()
	s.platformHealth.mu.Lock()
	defer s.platformHealth.mu.Unlock()
	s.platformHealth.byCluster[clusterID] = platformHealthEntry{
		Report:   platformhealth.TagCluster(platformhealth.OnlyPresentComponents(report), clusterID),
		Received: time.Now().UTC(),
	}
}

// GetAgentPlatformHealthSnapshots returns fresh and recently-stale agent pushes.
// Fresh: age <= freshMax. Stale: freshMax < age <= staleMax (kept so Admin still
// shows last logs when the apps cluster or agent is temporarily down).
func (s *Store) GetAgentPlatformHealthSnapshots(freshMax, staleMax time.Duration) []AgentPlatformHealthSnapshot {
	s.initPlatformHealth()
	s.platformHealth.mu.RLock()
	defer s.platformHealth.mu.RUnlock()
	if freshMax <= 0 {
		freshMax = 2 * time.Minute
	}
	if staleMax <= 0 {
		staleMax = 30 * time.Minute
	}
	now := time.Now().UTC()
	out := make([]AgentPlatformHealthSnapshot, 0, len(s.platformHealth.byCluster))
	for id, entry := range s.platformHealth.byCluster {
		if entry.Report == nil {
			continue
		}
		age := now.Sub(entry.Received)
		if age > staleMax {
			continue
		}
		out = append(out, AgentPlatformHealthSnapshot{
			ClusterID: id,
			Report:    entry.Report,
			Received:  entry.Received,
			Stale:     age > freshMax,
		})
	}
	return out
}

// GetAgentPlatformHealthReports returns only fresh agent-pushed health snapshots.
func (s *Store) GetAgentPlatformHealthReports(maxAge time.Duration) map[string]*platformhealth.Report {
	out := make(map[string]*platformhealth.Report)
	for _, snap := range s.GetAgentPlatformHealthSnapshots(maxAge, maxAge) {
		if !snap.Stale {
			out[snap.ClusterID] = snap.Report
		}
	}
	return out
}
