package collector

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveSampler_Basic(t *testing.T) {
	// Initialize sampler targeting 100 spans per second, window of 10ms for quick test
	sampler := NewAdaptiveSampler(100, 10*time.Millisecond)
	defer sampler.Stop()

	// Initial ratio should be 100% (100.0)
	if ratio := sampler.GetCurrentRatio(); ratio != 100.0 {
		t.Errorf("expected initial sample rate to be 100%%, got %.2f%%", ratio)
	}

	// 1. Errors should always be sampled
	for i := 0; i < 100; i++ {
		traceID := "trace-err-" + strconv.Itoa(i)
		if !sampler.ShouldSample(traceID, true) {
			t.Errorf("expected error span to be sampled")
		}
	}
}

func TestAdaptiveSampler_SamplingDecisions(t *testing.T) {
	sampler := NewAdaptiveSampler(100, 100*time.Millisecond)
	defer sampler.Stop()

	// Force currentRatio to 50% (5000 basis points)
	atomic.StoreUint32(&sampler.currentRatio, 5000)

	sampledCount := 0
	totalCount := 10000

	for i := 0; i < totalCount; i++ {
		traceID := "trace-id-hash-" + strconv.Itoa(i)
		if sampler.ShouldSample(traceID, false) {
			sampledCount++
		}
	}

	// At 50% target sampling, around 50% of the traces should be kept.
	// Allow for a normal statistical variance (+/- 3%)
	percentage := float64(sampledCount) / float64(totalCount) * 100.0
	t.Logf("Sampled %d out of %d (%.2f%%)", sampledCount, totalCount, percentage)

	if percentage < 47.0 || percentage > 53.0 {
		t.Errorf("expected sampled rate to be near 50%%, got %.2f%%", percentage)
	}
}

func TestAdaptiveSampler_TraceDecisionSticksAcrossRatioChange(t *testing.T) {
	sampler := NewAdaptiveSampler(100, time.Hour)
	defer sampler.Stop()

	traceID := "cross-ns-trace-keep-me"
	if !sampler.ShouldSample(traceID, false) {
		t.Fatal("expected first span kept at 100% ratio")
	}

	atomic.StoreUint32(&sampler.currentRatio, 100) // 1%
	for i := 0; i < 20; i++ {
		if !sampler.ShouldSample(traceID, false) {
			t.Fatalf("later span %d dropped after ratio drop — would show as a skipped parent", i)
		}
	}

	dropped := "cross-ns-trace-drop-me"
	atomic.StoreUint32(&sampler.currentRatio, 1)
	first := sampler.ShouldSample(dropped, false)
	atomic.StoreUint32(&sampler.currentRatio, 10000)
	for i := 0; i < 20; i++ {
		got := sampler.ShouldSample(dropped, false)
		if got != first {
			t.Fatalf("drop/keep flipped after ratio rose (first=%v later=%v)", first, got)
		}
	}
}

func TestSlowTraceKeepMsDefault(t *testing.T) {
	t.Setenv("APM_SLOW_TRACE_MS", "")
	if got := slowTraceKeepMs(); got != 2000 {
		t.Fatalf("default = %v, want 2000", got)
	}
	t.Setenv("APM_SLOW_TRACE_MS", "5000")
	if got := slowTraceKeepMs(); got != 5000 {
		t.Fatalf("override = %v, want 5000", got)
	}
}

func TestAdaptiveSampler_ErrorOverridesDropPin(t *testing.T) {
	sampler := NewAdaptiveSampler(100, time.Hour)
	defer sampler.Stop()
	atomic.StoreUint32(&sampler.currentRatio, 1)
	id := "error-promotes-trace"
	_ = sampler.ShouldSample(id, false)
	if !sampler.ShouldSample(id, true) {
		t.Fatal("error span must keep the trace")
	}
	atomic.StoreUint32(&sampler.currentRatio, 1)
	if !sampler.ShouldSample(id, false) {
		t.Fatal("spans after an error must stay kept")
	}
}

func TestAdaptiveSampler_Adjuster(t *testing.T) {
	// Initialize sampler targeting 50 spans per second, with 50ms window size
	sampler := NewAdaptiveSampler(50, 50*time.Millisecond)
	defer sampler.Stop()

	// Simulate high ingestion volume (e.g., 500 spans inside a 50ms window = 10,000 spans/sec)
	for i := 0; i < 500; i++ {
		traceID := "heavy-trace-" + strconv.Itoa(i)
		sampler.ShouldSample(traceID, false)
	}

	// Wait for the window adjuster loop to trigger and adjust sampling rate downwards
	// Sleep longer than the 50ms window but shorter than 100ms to avoid the idle tick resetting it.
	time.Sleep(75 * time.Millisecond)

	ratio := sampler.GetCurrentRatio()
	t.Logf("Adjusted sample rate after heavy load: %.2f%%", ratio)

	if ratio >= 100.0 {
		t.Errorf("expected sample rate to be throttled down below 100%%, got %.2f%%", ratio)
	}
}
