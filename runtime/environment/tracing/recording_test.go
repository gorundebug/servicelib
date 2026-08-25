package tracing

import (
	"context"
	"testing"
)

func TestSamplingHonorsWorkflowReplayPolicy(t *testing.T) {
	replaying := true
	ctx := EnableSampling(context.Background())
	ctx = WithRecordingPolicy(ctx, func() bool { return !replaying })
	if SamplingEnabled(ctx) {
		t.Fatal("tracing must be suppressed during Workflow replay")
	}
	replaying = false
	if !SamplingEnabled(ctx) {
		t.Fatal("tracing must resume for new Workflow execution")
	}
}
