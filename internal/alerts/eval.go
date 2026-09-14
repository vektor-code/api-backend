package alerts

import (
	"fmt"
	"time"
)

// EvaluateRules returns newly firing alerts for the given samples.
func EvaluateRules(rules []Rule, samples []ServiceSample, now time.Time) []ActiveAlert {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var out []ActiveAlert
	for _, rule := range rules {
		if !rule.Active {
			continue
		}
		for _, sample := range samples {
			if !RuleMatchesService(rule, sample) {
				continue
			}
			value, ok := SampleValue(sample, rule.Metric)
			if !ok {
				continue
			}
			if !Compare(rule.Operator, value, rule.Threshold) {
				continue
			}
			out = append(out, ActiveAlert{
				ID:        fmt.Sprintf("%s:%s:%s", rule.ID, sample.Namespace, sample.ServiceName),
				RuleID:    rule.ID,
				RuleName:  rule.Name,
				Service:   sample.ServiceName,
				Namespace: sample.Namespace,
				Metric:    rule.Metric,
				Condition: fmt.Sprintf("%s %s %g", rule.Metric, NormalizeOperator(rule.Operator), rule.Threshold),
				Value:     value,
				Severity:  rule.Severity,
				Status:    "Firing",
				FiredAt:   now,
			})
		}
	}
	return out
}
