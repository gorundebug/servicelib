package temporal

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestWorkflowTracingPreservesCallerContext(t *testing.T) {
	previousPropagator, previousProvider := otel.GetTextMapPropagator(), otel.GetTracerProvider()
	provider := trace.NewTracerProvider(trace.WithSampler(trace.AlwaysSample()))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTextMapPropagator(previousPropagator)
		otel.SetTracerProvider(previousProvider)
	})
	type key struct{}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.WithValue(t.Context(), key{}, "caller"), deadline)
	defer cancel()
	tracing := &workflowTracing{carrier: propagation.MapCarrier{
		"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
	}}
	child, span := tracing.Tracer("test").Start(ctx, "child")
	defer span.End()
	gotDeadline, ok := child.Deadline()
	if child.Value(key{}) != "caller" || !ok || !gotDeadline.Equal(deadline) || span.SpanContext().TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Fatal("workflow tracing replaced the caller context or lost the remote parent")
	}
	cancel()
	if child.Err() != context.Canceled {
		t.Fatal("workflow tracing lost caller cancellation")
	}
	// An existing graph span takes precedence over the workflow header.
	local, parent := provider.Tracer("test").Start(t.Context(), "local")
	defer parent.End()
	nested, nestedSpan := tracing.Tracer("test").Start(local, "nested")
	defer nestedSpan.End()
	if oteltrace.SpanContextFromContext(nested).TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("workflow header replaced the current graph trace")
	}
}
