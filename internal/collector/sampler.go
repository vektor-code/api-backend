package collector

import (
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// traceDecisionTTL is how long a keep/drop decision sticks for a trace ID so
// later spans from other services or namespaces cannot be sampled differently
// and show up as skipped parents in the waterfall.
const traceDecisionTTL = 5 * time.Minute

type pinnedDecision struct {
	keep          bool
	expiry        time.Time
	probability   float64
	adjustedCount float64
}

// Attributes recording the sampling weight of a span. They are only written
// when a span was kept probabilistically, so unsampled telemetry carries no
// extra bytes.
const (
	AttrSampleProbability = "crnet.apm.sampling.probability"
	AttrAdjustedCount     = "crnet.apm.sampling.adjusted_count"
)

// AdaptiveSampler dynamically adjusts trace sampling based on incoming span rates
type AdaptiveSampler struct {
	targetRate   int64  // Target spans per second
	currentRatio uint32 // Current sampling ratio in basis points (0-10000, where 10000 is 100%)
	spansCount   int64  // Atomic counter of spans evaluated in the current window
	spansSampled int64  // Atomic counter of spans kept in the current window
	windowSize   time.Duration
	stopChan     chan struct{}

	// Sticky keep/drop so a trace that already crossed services (and
	// namespaces) cannot lose mid-tree spans when the adaptive ratio
	// changes between batches.
	decisions sync.Map // traceID -> pinnedDecision
}

// NewAdaptiveSampler initializes and starts the adaptive sampling loop
func NewAdaptiveSampler(targetSpansPerSec int64, windowSize time.Duration) *AdaptiveSampler {
	s := &AdaptiveSampler{
		targetRate:   targetSpansPerSec,
		currentRatio: 10000, // Start at 100%
		windowSize:   windowSize,
		stopChan:     make(chan struct{}),
	}
	go s.runAdjuster()
	return s
}

// Stop shuts down the adaptive adjuster loop
func (s *AdaptiveSampler) Stop() {
	close(s.stopChan)
}

// SampleDecision is the outcome of a sampling check.
type SampleDecision struct {
	// Keep is whether the span should be stored.
	Keep bool
	// Probability is the chance a span like this one had of being kept, in the
	// range (0, 1].
	Probability float64
	// AdjustedCount is 1/Probability: how many spans this one stands for. A
	// span kept at a 1% rate represents 100. Without it, every count derived
	// from stored spans silently under-reports by the sampling factor.
	AdjustedCount float64
}

// ShouldSample reports whether a span should be saved. It is retained for
// callers that do not need the weighting.
func (s *AdaptiveSampler) ShouldSample(traceID string, isError bool) bool {
	return s.Sample(traceID, isError).Keep
}

// Sample makes the decision and reports the weight the surviving span carries.
// forceKeep is true for errors and slow spans so a trace that already failed
// or blew its latency budget cannot be dropped mid-tree.
func (s *AdaptiveSampler) Sample(traceID string, forceKeep bool) SampleDecision {
	atomic.AddInt64(&s.spansCount, 1)

	// Forced keeps (errors, slow traces) always keep the whole trace. Giving
	// them the probabilistic weight would inflate those counts by the sampling
	// factor.
	if forceKeep {
		d := keptExactly()
		s.pin(traceID, d)
		atomic.AddInt64(&s.spansSampled, 1)
		return d
	}
	if d, ok := s.pinned(traceID); ok {
		if d.Keep {
			atomic.AddInt64(&s.spansSampled, 1)
		}
		return d
	}

	ratio := atomic.LoadUint32(&s.currentRatio)
	var d SampleDecision
	switch {
	case ratio >= 10000:
		d = keptExactly()
	case ratio == 0:
		d = SampleDecision{}
	default:
		h := fnv.New32a()
		_, _ = h.Write([]byte(traceID))
		if (h.Sum32() % 10000) < ratio {
			probability := float64(ratio) / 10000.0
			d = SampleDecision{
				Keep:          true,
				Probability:   probability,
				AdjustedCount: 1 / probability,
			}
		}
	}
	s.pin(traceID, d)
	if d.Keep {
		atomic.AddInt64(&s.spansSampled, 1)
	}
	return d
}

func (s *AdaptiveSampler) pin(traceID string, d SampleDecision) {
	s.decisions.Store(traceID, pinnedDecision{
		keep:          d.Keep,
		expiry:        time.Now().Add(traceDecisionTTL),
		probability:   d.Probability,
		adjustedCount: d.AdjustedCount,
	})
}

func (s *AdaptiveSampler) pinned(traceID string) (SampleDecision, bool) {
	v, ok := s.decisions.Load(traceID)
	if !ok {
		return SampleDecision{}, false
	}
	p, ok := v.(pinnedDecision)
	if !ok || time.Now().After(p.expiry) {
		s.decisions.Delete(traceID)
		return SampleDecision{}, false
	}
	if !p.keep {
		return SampleDecision{}, true
	}
	return SampleDecision{Keep: true, Probability: p.probability, AdjustedCount: p.adjustedCount}, true
}

func keptExactly() SampleDecision {
	return SampleDecision{Keep: true, Probability: 1, AdjustedCount: 1}
}

func slowTraceKeepMs() float64 {
	v := strings.TrimSpace(os.Getenv("APM_SLOW_TRACE_MS"))
	if v == "" {
		return 2000
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 2000
	}
	return f
}

// GetCurrentRatio returns the active sample rate as a percentage (0.0 to 100.0)
func (s *AdaptiveSampler) GetCurrentRatio() float64 {
	return float64(atomic.LoadUint32(&s.currentRatio)) / 100.0
}

func (s *AdaptiveSampler) runAdjuster() {
	ticker := time.NewTicker(s.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			now := time.Now()
			s.decisions.Range(func(k, v any) bool {
				if p, ok := v.(pinnedDecision); ok && now.After(p.expiry) {
					s.decisions.Delete(k)
				}
				return true
			})

			// Read and reset counters
			incoming := atomic.SwapInt64(&s.spansCount, 0)
			sampled := atomic.SwapInt64(&s.spansSampled, 0)

			// Convert throughput to spans/second rate
			secs := s.windowSize.Seconds()
			ratePerSec := int64(float64(incoming) / secs)

			oldRatio := atomic.LoadUint32(&s.currentRatio)
			var newRatio uint32

			if incoming == 0 {
				newRatio = 10000 // Reset to 100% if idle
			} else if ratePerSec <= s.targetRate {
				// Increase sample rate gradually (linear recovery towards 100%)
				newRatio = oldRatio + 1000 // Increase by 10%
				if newRatio > 10000 {
					newRatio = 10000
				}
			} else {
				// We exceeded target rate. Decrease sampling ratio to match budget:
				// targetRate / ratePerSec represents the percentage we should keep.
				idealRatio := uint32((float64(s.targetRate) / float64(ratePerSec)) * 10000)

				// Smooth change using a low-pass filter (Exponential Moving Average)
				// newRatio = 0.6 * oldRatio + 0.4 * idealRatio
				newRatio = uint32(0.6*float64(oldRatio) + 0.4*float64(idealRatio))
				if newRatio < 100 {
					newRatio = 100 // Cap minimum sample rate at 1% to avoid total blindness
				}
			}

			atomic.StoreUint32(&s.currentRatio, newRatio)

			if oldRatio != newRatio || ratePerSec > s.targetRate {
				log.Printf("[sampler] Ingestion throughput: %d spans/sec | Sample rate: %.2f%% (was %.2f%%) | Kept: %d spans",
					ratePerSec, float64(newRatio)/100.0, float64(oldRatio)/100.0, sampled)
			}
		}
	}
}
