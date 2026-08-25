package temporal

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type temporalHeaderMap map[string]*commonpb.Payload

func (h temporalHeaderMap) Set(key string, value *commonpb.Payload) { h[key] = value }
func (h temporalHeaderMap) Get(key string) (*commonpb.Payload, bool) {
	value, ok := h[key]
	return value, ok
}
func (h temporalHeaderMap) ForEachKey(handler func(string, *commonpb.Payload) error) error {
	for key, value := range h {
		if err := handler(key, value); err != nil {
			return err
		}
	}
	return nil
}

type carrierTestTracing struct{ extractedKey struct{} }

func (carrierTestTracing) Tracer(string) tracing.Tracer { return nil }
func (carrierTestTracing) Inject(_ context.Context, carrier map[string]string) {
	carrier["traceparent"] = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	carrier["tracestate"] = "vendor=value"
	carrier["baggage"] = "tenant=example"
}
func (f carrierTestTracing) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return context.WithValue(ctx, f.extractedKey, carrier["baggage"])
}

func TestTemporalContextPropagatorUsesNativeHeaders(t *testing.T) {
	engine := carrierTestTracing{}
	propagator := temporalContextPropagator{tracing: engine}
	deadline := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx = runtime.WithStreamId(ctx, "stream-123")
	ctx = runtime.WithPriority(ctx, 7)
	ctx = tracing.EnableSampling(ctx)

	headers := temporalHeaderMap{}
	if err := propagator.Inject(ctx, headers); err != nil {
		t.Fatalf("inject Temporal headers: %v", err)
	}
	for _, key := range []string{
		"traceparent", "tracestate", "baggage", "x-trace", "x-stream-id",
		temporalHeaderPriority, temporalHeaderDeadlineUnixNano,
	} {
		if _, present := headers[key]; !present {
			t.Fatalf("Temporal header %q was not injected", key)
		}
	}

	extracted, err := propagator.Extract(context.Background(), headers)
	if err != nil {
		t.Fatalf("extract Temporal headers: %v", err)
	}
	if !tracing.SamplingEnabled(extracted) {
		t.Fatal("sampled Temporal trace carrier did not enable tracing")
	}
	if got, ok := runtime.StreamIdFromContext(extracted); !ok || got.GetID() != "stream-123" {
		t.Fatalf("stream ID = %v, %v", got, ok)
	}
	if got, ok := runtime.PriorityFromContext(extracted); !ok || got != 7 {
		t.Fatalf("priority = %d, %v", got, ok)
	}
	if got := extracted.Value(engine.extractedKey); got != "tenant=example" {
		t.Fatalf("tracing extractor baggage = %v", got)
	}
	if got, ok := extracted.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("deadline = %v, %v; expected %v", got, ok, deadline)
	}
}

func TestTemporalContextPropagatorForwardsWorkflowHeadersToActivity(t *testing.T) {
	engine := carrierTestTracing{}
	propagator := temporalContextPropagator{tracing: engine}
	headers := temporalHeaderMap{}
	ctx := tracing.EnableSampling(runtime.WithStreamId(context.Background(), "workflow-stream"))
	if err := propagator.Inject(ctx, headers); err != nil {
		t.Fatalf("inject Temporal headers: %v", err)
	}

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.SetHeader(&commonpb.Header{Fields: headers})
	environment.SetContextPropagators([]workflow.ContextPropagator{propagator})
	activityFunction := func(ctx context.Context) (string, error) {
		streamID, _ := runtime.StreamIdFromContext(ctx)
		if streamID == nil || !tracing.SamplingEnabled(ctx) {
			return "", context.Canceled
		}
		return streamID.GetID(), nil
	}
	workflowFunction := func(ctx workflow.Context) (string, error) {
		var streamID string
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}),
			activityFunction,
		).Get(ctx, &streamID)
		return streamID, err
	}
	environment.RegisterActivity(activityFunction)
	environment.RegisterWorkflow(workflowFunction)
	environment.ExecuteWorkflow(workflowFunction)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("execute propagated Workflow: %v", err)
	}
	var streamID string
	if err := environment.GetWorkflowResult(&streamID); err != nil {
		t.Fatalf("read Workflow result: %v", err)
	}
	if streamID != "workflow-stream" {
		t.Fatalf("Activity stream ID = %q", streamID)
	}
}
