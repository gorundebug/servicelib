package rabbitmqclient

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type traceStub struct {
	tracing.Tracing
	extracted map[string]string
}

func (*traceStub) Inject(_ context.Context, carrier map[string]string) {
	carrier["traceparent"] = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	carrier["tracestate"] = "vendor=value"
	carrier["baggage"] = "tenant=example"
}
func (t *traceStub) Extract(ctx context.Context, carrier map[string]string) context.Context {
	t.extracted = carrier
	return ctx
}

func TestNativeHeaderTracePropagationRetainsCallerHeaders(t *testing.T) {
	original := map[string]any{"X-Stream-ID": "stale", "TRACEPARENT": "stale", "Application": int32(42)}
	ctx := tracing.EnableSampling(runtime.WithStreamId(t.Context(), "request"))
	engine := &traceStub{}
	headers := Inject(ctx, engine, original)
	if original["X-Stream-ID"] != "stale" || original["TRACEPARENT"] != "stale" {
		t.Fatal("caller headers mutated")
	}
	if headers["x-stream-id"] != "request" || headers["X-Stream-ID"] != nil {
		t.Fatal("case variants not replaced")
	}
	if headers["Application"] != int32(42) {
		t.Fatal("caller header values lost")
	}
	mixed := make(map[string]any)
	for k, v := range headers {
		mixed[k] = v
	}
	mixed["X-TrAcE"] = mixed["x-trace"]
	delete(mixed, "x-trace")
	mixed["X-STREAM-ID"] = mixed["x-stream-id"]
	delete(mixed, "x-stream-id")
	out := Extract(t.Context(), engine, mixed)
	id, ok := runtime.StreamIdFromContext(out)
	if !ok || id.GetID() != "request" {
		t.Fatal("stream ID lost")
	}
	if !tracing.SamplingEnabled(out) {
		t.Fatal("sampling lost")
	}
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		if engine.extracted[key] == "" {
			t.Fatalf("lost %s", key)
		}
	}
	out = Extract(t.Context(), nil, mixed)
	if !tracing.SamplingEnabled(out) {
		t.Fatal("sampling marker ignored without tracing engine")
	}
}
