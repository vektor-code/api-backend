package traceinvest

import (
	"sync"
	"time"
)

type cacheEntry struct {
	report    *Report
	expiresAt time.Time
}

type ttlCache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
	ttl     time.Duration
}

func newTTLCache(ttl time.Duration) *ttlCache {
	if ttl <= 0 {
		ttl = DefaultTTL()
	}
	return &ttlCache{entries: map[string]cacheEntry{}, ttl: ttl}
}

func (c *ttlCache) get(key string) (*Report, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		if ok {
			delete(c.entries, key)
		}
		return nil, false
	}
	cp := *e.report
	cp.Cached = true
	return &cp, true
}

func (c *ttlCache) set(key string, report *Report) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *report
	c.entries[key] = cacheEntry{report: &cp, expiresAt: time.Now().Add(c.ttl)}
}

type limiter struct {
	mu          sync.Mutex
	limits      Limits
	global      int
	namespace   map[string]int
	workload    map[string]int
	destination map[string]int
}

func newLimiter(l Limits) *limiter {
	if l.Global <= 0 {
		l = DefaultLimits()
	}
	return &limiter{
		limits:      l,
		namespace:   map[string]int{},
		workload:    map[string]int{},
		destination: map[string]int{},
	}
}

func (l *limiter) acquire(ns, workload, dest string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.global >= l.limits.Global {
		return nil, false
	}
	if l.namespace[ns] >= l.limits.Namespace {
		return nil, false
	}
	if l.workload[ns+"/"+workload] >= l.limits.Workload {
		return nil, false
	}
	if l.destination[dest] >= l.limits.Destination {
		return nil, false
	}
	l.global++
	l.namespace[ns]++
	l.workload[ns+"/"+workload]++
	l.destination[dest]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.global--
		l.namespace[ns]--
		l.workload[ns+"/"+workload]--
		l.destination[dest]--
	}, true
}

type flight struct {
	mu sync.Mutex
	in map[string]*inflight
}

type inflight struct {
	wg     sync.WaitGroup
	report *Report
}

func newFlight() *flight {
	return &flight{in: map[string]*inflight{}}
}

func (f *flight) do(key string, fn func() *Report) *Report {
	f.mu.Lock()
	if call, ok := f.in[key]; ok {
		f.mu.Unlock()
		call.wg.Wait()
		if call.report == nil {
			return nil
		}
		cp := *call.report
		cp.Cached = true
		return &cp
	}
	call := &inflight{}
	call.wg.Add(1)
	f.in[key] = call
	f.mu.Unlock()

	call.report = fn()
	call.wg.Done()

	f.mu.Lock()
	delete(f.in, key)
	f.mu.Unlock()
	return call.report
}
