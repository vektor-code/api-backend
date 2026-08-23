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
	var incoming *models.Span
	considerIncoming := func(sp *models.Span) {
		if sp == nil || !isRequestSpan(sp) {
			return
		}
		if incoming == nil || sp.StartTime.Before(incoming.StartTime) {
			incoming = sp
		}
	}
	considerIncoming(trace.RootSpan)
	for _, sp := range trace.Spans {
		considerIncoming(sp)
	}
	if incoming != nil {
		return incoming
	}
	// Cron/custom INTERNAL may name a transaction only when it is the root.
	// Child repository methods must not steal identity from a missing SERVER.
	if isTransactionSpan(trace.RootSpan) {
		return trace.RootSpan
	}
	return nil
}

func isPoolHousekeeper(tags map[string]string) bool {
	if tags == nil {
		return false
	}
	return strings.Contains(strings.ToLower(tags["thread.name"]), "housekeeper")
}
