package alerts

import (
	"context"
	"log"
	"sync"
	"time"
)

// SampleFunc returns current service metrics for evaluation.
type SampleFunc func(ctx context.Context) ([]ServiceSample, error)

// Engine periodically evaluates rules and notifies on new firings.
type Engine struct {
	Store    *MemoryStore
	Notifier *Notifier
	Samples  SampleFunc
	Interval time.Duration

	mu       sync.Mutex
	lastSent map[string]time.Time
}

func NewEngine(store *MemoryStore, notifier *Notifier, samples SampleFunc) *Engine {
	if store == nil {
		store = NewMemoryStore()
	}
	if notifier == nil {
		notifier = NewNotifier(nil)
	}
	return &Engine{
		Store:    store,
		Notifier: notifier,
		Samples:  samples,
		Interval: 30 * time.Second,
		lastSent: map[string]time.Time{},
	}
}

func (e *Engine) Run(ctx context.Context) {
	if e.Interval <= 0 {
		e.Interval = 30 * time.Second
	}
	ticker := time.NewTicker(e.Interval)
	defer ticker.Stop()
	e.EvaluateOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.EvaluateOnce(ctx)
		}
	}
}

func (e *Engine) EvaluateOnce(ctx context.Context) []ActiveAlert {
	if e.Samples == nil {
		return nil
	}
	samples, err := e.Samples(ctx)
	if err != nil {
		log.Printf("[alerts] sample fetch failed: %v", err)
		return nil
	}
	firings := EvaluateRules(e.Store.ListRules(), samples, time.Now().UTC())
	e.Store.ReplaceActive(firings)
	e.notifyNew(ctx, firings)
	return firings
}

func (e *Engine) notifyNew(ctx context.Context, firings []ActiveAlert) {
	now := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, alert := range firings {
		if last, ok := e.lastSent[alert.ID]; ok && now.Sub(last) < 5*time.Minute {
			continue
		}
		rule := findRule(e.Store.ListRules(), alert.RuleID)
		if rule == nil {
			continue
		}
		for _, chID := range rule.Channels {
			ch, ok := e.Store.ChannelByID(chID)
			if !ok {
				continue
			}
			if err := e.Notifier.Notify(ctx, ch, alert); err != nil {
				log.Printf("[alerts] notify channel=%s: %v", ch.ID, err)
				continue
			}
		}
		e.lastSent[alert.ID] = now
	}
}

func findRule(rules []Rule, id string) *Rule {
	for i := range rules {
		if rules[i].ID == id {
			return &rules[i]
		}
	}
	return nil
}
