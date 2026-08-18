package spantree

import (
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

const containSkew = 50 * time.Millisecond

// Forest is a view of parent/child links. Span records are not mutated.
type Forest struct {
	ByID           map[string]*models.Span
	Children       map[string][]*models.Span
	DisplayRoot    *models.Span
	Roots          []*models.Span
	PartialRoot    bool
	MidTreeMissing []*models.Span
}

// Build links spans by normalized parent IDs, then stitches remaining
// orphans into one tree so a missing remote root does not split the flow.
func Build(spans []*models.Span) Forest {
	f := Forest{
		ByID:     make(map[string]*models.Span, len(spans)),
		Children: make(map[string][]*models.Span, len(spans)),
	}
	var ordered []*models.Span
	for _, sp := range spans {
		if sp == nil || sp.SpanID == "" {
			continue
		}
		ordered = append(ordered, sp)
		f.ByID[Normalize(sp.SpanID)] = sp
	}

	var trueRoots, orphans []*models.Span
	parentOf := make(map[string]string, len(ordered))
	for _, sp := range ordered {
		if IsRoot(sp.ParentSpanID) {
			trueRoots = append(trueRoots, sp)
			continue
		}
		parentKey := Normalize(sp.ParentSpanID)
		if parent, ok := f.ByID[parentKey]; ok && parent.SpanID != sp.SpanID {
			f.Children[parentKey] = append(f.Children[parentKey], sp)
			parentOf[Normalize(sp.SpanID)] = parentKey
			continue
		}
		orphans = append(orphans, sp)
	}

	if len(trueRoots) > 0 {
		f.DisplayRoot = earliest(trueRoots)
		f.Roots = trueRoots
	} else {
		f.DisplayRoot = entrySpan(orphans)
		f.PartialRoot = f.DisplayRoot != nil
		if f.DisplayRoot != nil {
			f.Roots = []*models.Span{f.DisplayRoot}
		}
	}
	if f.DisplayRoot == nil && len(ordered) > 0 {
		f.DisplayRoot = earliest(ordered)
		f.Roots = []*models.Span{f.DisplayRoot}
	}

	for _, sp := range orphans {
		if f.DisplayRoot != nil && sp.SpanID == f.DisplayRoot.SpanID {
			continue
		}
		host := tightestContainer(ordered, sp)
		if host == nil || createsCycle(parentOf, Normalize(sp.SpanID), Normalize(host.SpanID)) {
			host = f.DisplayRoot
		}
		if host == nil || host.SpanID == sp.SpanID || createsCycle(parentOf, Normalize(sp.SpanID), Normalize(host.SpanID)) {
			f.MidTreeMissing = append(f.MidTreeMissing, sp)
			continue
		}
		f.Children[Normalize(host.SpanID)] = append(f.Children[Normalize(host.SpanID)], sp)
		parentOf[Normalize(sp.SpanID)] = Normalize(host.SpanID)
		f.MidTreeMissing = append(f.MidTreeMissing, sp)
	}

	// A missing remote/browser root is expected. Only mid-tree holes are breaks.
	if f.PartialRoot {
		filtered := f.MidTreeMissing[:0]
		for _, sp := range f.MidTreeMissing {
			if f.DisplayRoot != nil && sp.SpanID == f.DisplayRoot.SpanID {
				continue
			}
			filtered = append(filtered, sp)
		}
		f.MidTreeMissing = filtered
	}
	return f
}

func (f Forest) Complete() bool {
	return len(f.MidTreeMissing) == 0
}

func entrySpan(orphans []*models.Span) *models.Span {
	var bestServer, bestAny *models.Span
	for _, sp := range orphans {
		if bestAny == nil || sp.StartTime.Before(bestAny.StartTime) {
			bestAny = sp
		}
		if sp.Kind == models.SpanKindServer || sp.Kind == "SERVER" {
			if bestServer == nil || sp.StartTime.Before(bestServer.StartTime) {
				bestServer = sp
			}
		}
	}
	if bestServer != nil {
		return bestServer
	}
	return bestAny
}

func earliest(spans []*models.Span) *models.Span {
	var best *models.Span
	for _, sp := range spans {
		if best == nil || sp.StartTime.Before(best.StartTime) {
			best = sp
		}
	}
	return best
}

func tightestContainer(all []*models.Span, child *models.Span) *models.Span {
	var best *models.Span
	for _, cand := range all {
		if cand == nil || cand.SpanID == child.SpanID {
			continue
		}
		if !contains(cand, child) {
			continue
		}
		if best == nil || cand.DurationMs < best.DurationMs || (cand.DurationMs == best.DurationMs && cand.StartTime.Before(best.StartTime)) {
			best = cand
		}
	}
	return best
}

func createsCycle(parentOf map[string]string, child, host string) bool {
	for key := host; key != ""; key = parentOf[key] {
		if key == child {
			return true
		}
	}
	return false
}

func contains(parent, child *models.Span) bool {
	if parent.StartTime.After(child.StartTime.Add(containSkew)) {
		return false
	}
	if parent.EndTime.Add(containSkew).Before(child.EndTime) {
		return false
	}
	return true
}
