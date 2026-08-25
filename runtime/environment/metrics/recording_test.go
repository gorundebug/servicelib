package metrics

import (
	"context"
	"testing"
)

func TestRecordingPolicyIsEvaluatedAtObservationTime(t *testing.T) {
	replaying := true
	ctx := WithRecordingPolicy(context.Background(), func() bool { return !replaying })
	if RecordingEnabled(ctx) {
		t.Fatal("metrics must be suppressed during Workflow replay")
	}
	replaying = false
	if !RecordingEnabled(ctx) {
		t.Fatal("metrics must resume for new Workflow execution")
	}
}
