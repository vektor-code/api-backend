package spantree

import (
	"testing"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func TestNormalizePadsAndLowercases(t *testing.T) {
	if Normalize("AB") != "00000000000000ab" {
		t.Fatalf("got %q", Normalize("AB"))
	}
	if Normalize("0x00ab") != "00000000000000ab" {
		t.Fatalf("got %q", Normalize("0x00ab"))
	}
	if !IsRoot("") || !IsRoot("0000000000000000") || !IsRoot("0") {
		t.Fatal("zero ids must be roots")
	}
}

func TestPartialRootIsCompleteCapturedTree(t *testing.T) {
	base := time.Now()
	server := &models.Span{
		SpanID: "srv1", ParentSpanID: "browser-never-sent", Name: "GET /poll",
		Kind: models.SpanKindServer, StartTime: base, EndTime: base.Add(50 * time.Millisecond), DurationMs: 50,
	}
	client := &models.Span{
		SpanID: "c1", ParentSpanID: "srv1", Name: "GET /api",
		Kind: models.SpanKindClient, StartTime: base.Add(5 * time.Millisecond), EndTime: base.Add(20 * time.Millisecond), DurationMs: 15,
	}
	f := Build([]*models.Span{client, server})
	if !f.PartialRoot || f.DisplayRoot != server {
		t.Fatalf("display root=%v partial=%v", f.DisplayRoot, f.PartialRoot)
	}
	if !f.Complete() {
		t.Fatalf("expected complete captured tree, missing %+v", f.MidTreeMissing)
	}
	kids := f.Children[Normalize("srv1")]
	if len(kids) != 1 || kids[0].SpanID != "c1" {
		t.Fatalf("children=%v", kids)
	}
}

func TestMismatchedHexParentStillLinks(t *testing.T) {
	base := time.Now()
	parent := &models.Span{
		SpanID: "00000000000000ab", ParentSpanID: "", Name: "root",
		Kind: models.SpanKindServer, StartTime: base, EndTime: base.Add(10 * time.Millisecond), DurationMs: 10,
	}
	child := &models.Span{
		SpanID: "c1", ParentSpanID: "AB", Name: "child",
		Kind: models.SpanKindClient, StartTime: base.Add(time.Millisecond), EndTime: base.Add(2 * time.Millisecond), DurationMs: 1,
	}
	f := Build([]*models.Span{parent, child})
	if !f.Complete() {
		t.Fatalf("hex mismatch should not break the tree: %+v", f.MidTreeMissing)
	}
	if len(f.Children[Normalize(parent.SpanID)]) != 1 {
		t.Fatalf("child not linked")
	}
}

func TestMidTreeHoleIsBrokenButStitched(t *testing.T) {
	base := time.Now()
	root := &models.Span{
		SpanID: "root", ParentSpanID: "", Name: "GET /a",
		Kind: models.SpanKindServer, StartTime: base, EndTime: base.Add(40 * time.Millisecond), DurationMs: 40,
	}
	orphan := &models.Span{
		SpanID: "leaf", ParentSpanID: "missing-mid", Name: "SELECT",
		Kind: models.SpanKindClient, StartTime: base.Add(5 * time.Millisecond), EndTime: base.Add(15 * time.Millisecond), DurationMs: 10,
	}
	f := Build([]*models.Span{root, orphan})
	if f.Complete() {
		t.Fatal("mid-tree hole should be reported")
	}
	if len(f.Children[Normalize("root")]) != 1 {
		t.Fatal("orphan should still be stitched under the display root")
	}
}
