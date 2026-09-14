package alerts

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemoryStore is the default in-process alert store (also used when Postgres
// alert tables are unavailable).
type MemoryStore struct {
	mu       sync.RWMutex
	rules    map[string]Rule
	channels map[string]Channel
	active   map[string]ActiveAlert
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		rules:    map[string]Rule{},
		channels: map[string]Channel{},
		active:   map[string]ActiveAlert{},
	}
}

func (s *MemoryStore) ListRules() []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	return out
}

func (s *MemoryStore) UpsertRule(r Rule) (Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if r.ID == "" {
		r.ID = uuid.NewString()
		r.CreatedAt = now
	} else if existing, ok := s.rules[r.ID]; ok {
		r.CreatedAt = existing.CreatedAt
	} else {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	r.Operator = NormalizeOperator(r.Operator)
	if r.Severity == "" {
		r.Severity = SeverityWarning
	}
	s.rules[r.ID] = r
	return r, nil
}

func (s *MemoryStore) DeleteRule(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return fmt.Errorf("rule not found")
	}
	delete(s.rules, id)
	return nil
}

func (s *MemoryStore) ListChannels() []Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Channel, 0, len(s.channels))
	for _, c := range s.channels {
		out = append(out, c)
	}
	return out
}

func (s *MemoryStore) UpsertChannel(c Channel) (Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if c.ID == "" {
		c.ID = uuid.NewString()
		c.CreatedAt = now
	} else if existing, ok := s.channels[c.ID]; ok {
		c.CreatedAt = existing.CreatedAt
	} else {
		c.CreatedAt = now
	}
	c.UpdatedAt = now
	if c.Type == "" {
		c.Type = ChannelWebhook
	}
	s.channels[c.ID] = c
	return c, nil
}

func (s *MemoryStore) DeleteChannel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.channels[id]; !ok {
		return fmt.Errorf("channel not found")
	}
	delete(s.channels, id)
	return nil
}

func (s *MemoryStore) ReplaceActive(alerts []ActiveAlert) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = map[string]ActiveAlert{}
	for _, a := range alerts {
		s.active[a.ID] = a
	}
}

func (s *MemoryStore) ListActive() []ActiveAlert {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ActiveAlert, 0, len(s.active))
	for _, a := range s.active {
		out = append(out, a)
	}
	return out
}

func (s *MemoryStore) ChannelByID(id string) (Channel, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[id]
	return c, ok
}
