package collector

import (
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// forceKeepTTL is how long a trace stays force-sampled after an error span,
// so the rest of its spans (across services and namespaces) are kept and the
// full flow stays intact instead of only the error span surviving sampling.
const forceKeepTTL = 5 * time.Minute

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

	forceKeep sync.Map // traceID -> expiry time.Time; traces pinned by an error span
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
func (s *AdaptiveSampler) Sample(traceID string, isError bool) SampleDecision {
	atomic.AddInt64(&s.spansCount, 1)

	// Always sample error spans, and pin the whole trace so its remaining
	// spans are kept too — otherwise multi-service/multi-namespace flows
	// would show only the failing span with no surrounding context.
	//
	// Deterministically kept spans represent only themselves; giving them the
	// probabilistic weight would inflate error counts by the sampling factor.
	if isError {
		s.forceKeep.Store(traceID, time.Now().Add(forceKeepTTL))
		atomic.AddInt64(&s.spansSampled, 1)
		return keptExactly()
	}
	if exp, ok := s.forceKeep.Load(traceID); ok {
		if t, ok := exp.(time.Time); ok && time.Now().Before(t) {
			atomic.AddInt64(&s.spansSampled, 1)
			return keptExactly()
		}
		s.forceKeep.Delete(traceID)
	}

	ratio := atomic.LoadUint32(&s.currentRatio)
	if ratio >= 10000 {
		atomic.AddInt64(&s.spansSampled, 1)
		return keptExactly()
	}
	if ratio == 0 {
		return SampleDecision{}
	}

	// Lock-free FNV-1a hashing of the trace ID
	h := fnv.New32a()
	_, _ = h.Write([]byte(traceID))
	hashVal := h.Sum32()

	// Compare against dynamic sampling ratio (in basis points)
	if (hashVal % 10000) < ratio {
		atomic.AddInt64(&s.spansSampled, 1)
		probability := float64(ratio) / 10000.0
		return SampleDecision{
			Keep:          true,
			Probability:   probability,
			AdjustedCount: 1 / probability,
		}
	}

	return SampleDecision{}
}

func keptExactly() SampleDecision {
	return SampleDecision{Keep: true, Probability: 1, AdjustedCount: 1}
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
			// Evict expired force-kept traces
			now := time.Now()
			s.forceKeep.Range(func(k, v any) bool {
				if t, ok := v.(time.Time); ok && now.After(t) {
					s.forceKeep.Delete(k)
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
