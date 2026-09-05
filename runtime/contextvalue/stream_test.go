package contextvalue

import (
	"context"
	"testing"
)

func TestStreamIDInspectionState(t *testing.T) {
	ctx := context.Background()
	if StreamIDInspected(ctx) {
		t.Fatal("untouched context is marked as inspected")
	}
	if id, ok := StreamIDFromContext(ctx); ok || id != nil {
		t.Fatalf("untouched context returned a stream id: id=%v ok=%v", id, ok)
	}

	ctx = WithStreamIDInspected(ctx)
	if !StreamIDInspected(ctx) {
		t.Fatal("context is not marked as inspected")
	}
	if id, ok := StreamIDFromContext(ctx); ok || id != nil {
		t.Fatalf("empty inspected context returned a stream id: id=%v ok=%v", id, ok)
	}

	ctx = WithStreamID(ctx, "stream-123")
	if !StreamIDInspected(ctx) {
		t.Fatal("context with a stream id is not marked as inspected")
	}
	id, ok := StreamIDFromContext(ctx)
	if !ok || id.GetID() != "stream-123" {
		t.Fatalf("unexpected stream id: id=%v ok=%v", id, ok)
	}
}
