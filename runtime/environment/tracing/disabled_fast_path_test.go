package tracing

import (
	"context"
	"testing"
)

type forbiddenFastPathTracer struct{}

func (forbiddenFastPathTracer) Start(context.Context, string, ...Attribute) (context.Context, Span) {
	panic("disabled tracing must not call the tracer")
}

func TestDisabledSpanFastPathDoesNotAllocate(t *testing.T) {
	sampled := EnableSampling(context.Background())
	for _, test := range []struct {
		name   string
		ctx    context.Context
		tracer Tracer
	}{
		{"no tracer", sampled, nil},
		{"unsampled", context.Background(), forbiddenFastPathTracer{}},
		{"replay", WithoutRecording(sampled), forbiddenFastPathTracer{}},
		{"policy disabled", WithRecordingPolicy(sampled, func() bool { return false }), forbiddenFastPathTracer{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, span := StartSpan(test.ctx, test.tracer, "stream.call")
			if ctx != test.ctx || span.SpanContext().IsValid {
				t.Fatal("disabled tracing must preserve context and return a no-op span")
			}
			allocations := testing.AllocsPerRun(1000, func() {
				_, span := StartSpan(test.ctx, test.tracer, "stream.call")
				span.End()
			})
			if allocations != 0 {
				t.Fatalf("disabled helper allocated %g objects per call", allocations)
			}
		})
	}
}
