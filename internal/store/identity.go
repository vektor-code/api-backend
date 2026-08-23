package store

import (
	"strings"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/kubetrace/shared/httproute"
)

func isRequestSpan(span *models.Span) bool {
	if span == nil {
		return false
	}
	return httproute.RequestIdentityEligible(string(span.Kind), span.Attributes)
}

func isTransactionSpan(span *models.Span) bool {
	if span == nil {
		return false
	}
	return httproute.TransactionIdentityEligible(string(span.Kind), span.Attributes)
}

func traceHasTransactionIdentity(trace *models.Trace) bool {
	return transactionIdentitySpan(trace) != nil
}

func transactionIdentitySpan(trace *models.Trace) *models.Span {
	if trace == nil {
		return nil
	}
	if isTransactionSpan(trace.RootSpan) {
		return trace.RootSpan
	}
	var best *models.Span
	for _, sp := range trace.Spans {
		if !isTransactionSpan(sp) {
			continue
		}
		if best == nil || sp.StartTime.Before(best.StartTime) {
			best = sp
		}
	}
	return best
}

func isPoolHousekeeper(tags map[string]string) bool {
	if tags == nil {
		return false
	}
	return strings.Contains(strings.ToLower(tags["thread.name"]), "housekeeper")
}
