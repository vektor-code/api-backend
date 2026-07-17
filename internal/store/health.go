package store

import (
	"math"

	"github.com/kubetrace/api-backend/internal/models"
)

const (
	apdexSatisfiedMs = 300.0
	apdexToleratedMs = apdexSatisfiedMs * 4
)

func finalizeServiceStats(stat *models.ServiceStats) {
	if stat == nil {
		return
	}
	if stat.RequestCount <= 0 {
		stat.HealthScore = 100
		stat.Apdex = 1
		stat.Status = "unknown"
		return
	}
	stat.ErrorRate = float64(stat.ErrorCount) / float64(stat.RequestCount) * 100
	stat.Apdex = estimateApdex(stat)
	stat.HealthScore = estimateHealthScore(stat)
	stat.Status = serviceStatus(stat.HealthScore)
}

func updateLatencyEstimates(stat *models.ServiceStats, durationMs float64) {
	if stat == nil || stat.RequestCount <= 0 {
		return
	}
	if durationMs < 0 {
		durationMs = 0
	}

	n := float64(stat.RequestCount)
	stat.P50Ms = (stat.P50Ms*(n-1) + durationMs) / n
	stat.P95Ms = updateTailEstimate(stat.P95Ms, durationMs, 0.04, 0.005)
	stat.P99Ms = updateTailEstimate(stat.P99Ms, durationMs, 0.02, 0.001)

	if stat.P95Ms < stat.P50Ms {
		stat.P95Ms = stat.P50Ms
	}
	if stat.P99Ms < stat.P95Ms {
		stat.P99Ms = stat.P95Ms
	}
}

func updateTailEstimate(current, sample, riseWeight, decayWeight float64) float64 {
	if current <= 0 {
		return sample
	}
	if sample > current {
		return current*(1-riseWeight) + sample*riseWeight
	}
	return current*(1-decayWeight) + sample*decayWeight
}

func estimateApdex(stat *models.ServiceStats) float64 {
	latencyScore := 1.0

	if stat.P50Ms > apdexSatisfiedMs {
		latencyScore -= math.Min(0.30, ((stat.P50Ms-apdexSatisfiedMs)/apdexSatisfiedMs)*0.20)
	}
	if stat.P95Ms > apdexSatisfiedMs {
		latencyScore -= math.Min(0.25, ((stat.P95Ms-apdexSatisfiedMs)/(apdexToleratedMs-apdexSatisfiedMs))*0.25)
	}
	if stat.P95Ms > apdexToleratedMs {
		latencyScore -= math.Min(0.25, ((stat.P95Ms-apdexToleratedMs)/apdexToleratedMs)*0.25)
	}
	if stat.P99Ms > apdexToleratedMs*2 {
		latencyScore -= math.Min(0.10, ((stat.P99Ms-apdexToleratedMs*2)/(apdexToleratedMs*2))*0.10)
	}

	errorPenalty := math.Min(0.40, (stat.ErrorRate/100)*0.75)
	return clamp(latencyScore-errorPenalty, 0, 1)
}

func estimateHealthScore(stat *models.ServiceStats) float64 {
	errorPenalty := math.Min(70, stat.ErrorRate*4.5)
	latencyPenalty := 0.0
	if stat.P95Ms > apdexSatisfiedMs {
		latencyPenalty += math.Min(30, (stat.P95Ms-apdexSatisfiedMs)/30)
	}
	if stat.P99Ms > apdexToleratedMs {
		latencyPenalty += math.Min(15, (stat.P99Ms-apdexToleratedMs)/120)
	}
	return clamp(100-errorPenalty-latencyPenalty, 0, 100)
}

func serviceStatus(score float64) string {
	switch {
	case score >= 90:
		return "healthy"
	case score >= 70:
		return "degraded"
	default:
		return "critical"
	}
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
