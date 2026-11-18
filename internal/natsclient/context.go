package natsclient

import (
	"context"
	"strings"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/nats-io/nats.go"
)

func traceKey(key string) bool {
	switch key {
	case "x-stream-id", "x-trace", "traceparent", "tracestate", "baggage":
		return true
	}
	return false
}

// Inject copies caller headers and changes only trace keys supplied by context.
// NATS Header is a map, so mixed-case spellings must be removed explicitly to
// prevent contradictory duplicate propagation headers.
func Inject(ctx context.Context, engine tracing.Tracing, headers nats.Header) nats.Header {
	out := make(nats.Header, len(headers)+5)
	for key, values := range headers {
		out[key] = append([]string(nil), values...)
	}
	carrier := make(map[string]string)
	if engine != nil {
		engine.Inject(ctx, carrier)
	}
	if id, ok := runtime.StreamIdFromContext(ctx); ok && id.GetID() != "" {
		carrier["x-stream-id"] = id.GetID()
	}
	if tracing.SamplingEnabled(ctx) {
		carrier["x-trace"] = "1"
	}
	for key, value := range carrier {
		key = strings.ToLower(key)
		if value == "" || !traceKey(key) {
			continue
		}
		for existing := range out {
			if strings.EqualFold(existing, key) {
				delete(out, existing)
			}
		}
		out[key] = []string{value}
	}
	return out
}

func Extract(ctx context.Context, engine tracing.Tracing, headers nats.Header) context.Context {
	carrier := make(map[string]string, 5)
	for key, values := range headers {
		key = strings.ToLower(key)
		if traceKey(key) && len(values) > 0 {
			carrier[key] = values[len(values)-1]
		}
	}
	if id := carrier["x-stream-id"]; id != "" {
		ctx = runtime.WithStreamId(ctx, id)
	}
	if engine != nil {
		ctx = engine.Extract(ctx, carrier)
	}
	if tracing.SamplingRequestedByCarrier(carrier) {
		ctx = tracing.EnableSampling(ctx)
	}
	return ctx
}
