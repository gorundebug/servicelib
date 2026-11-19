package dataquery

import (
	"context"
	"errors"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
)

func TestQueryEndpointMetrics(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rows       int
		completion bool
		failure    error
		replay     bool
	}{
		{name: "empty"}, {name: "results and application completion", rows: 3, completion: true},
		{name: "partial failure", rows: 2, failure: errors.New("native failure")},
		{name: "cancelled", failure: context.Canceled}, {name: "deadline", failure: context.DeadlineExceeded},
		{name: "replay", rows: 2, completion: true, replay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := testmetrics.New()
			telemetry, err := newEndpointMetrics(registry.Scope("dataquery_endpoint", nil))
			if err != nil {
				t.Fatal(err)
			}
			stream := &testStream{}
			handler := &testHandler{complete: tc.completion, key: &struct{}{}}
			consumer := &endpointConsumer[int, int, string, error]{stream: stream, handler: handler, telemetry: telemetry,
				execute: func(ctx context.Context, _ int, _ int, out runtime.Collect[string]) error {
					expected := int64(1)
					if tc.replay {
						expected = 0
					}
					if got := registry.Gauge("dataquery_endpoint_active_requests", nil).Value(); got != expected {
						t.Fatalf("active=%d", got)
					}
					for range tc.rows {
						out.Out(ctx, "row")
					}
					return tc.failure
				},
			}
			ctx := t.Context()
			if tc.replay {
				ctx = metrics.WithoutRecording(ctx)
			}
			consumer.Consume(ctx, 1)
			requests, results := int64(1), int64(tc.rows)
			if tc.completion {
				results++
			}
			if tc.replay {
				requests, results = 0, 0
			}
			if got := registry.Counter("dataquery_endpoint_requests_total", nil).Count(); got != requests {
				t.Fatalf("requests=%d", got)
			}
			if got := registry.Counter("dataquery_endpoint_results_total", nil).Count(); got != results {
				t.Fatalf("results=%d", got)
			}
			if got := registry.Gauge("dataquery_endpoint_active_requests", nil).Value(); got != 0 {
				t.Fatalf("leaked active=%d", got)
			}
			if got := registry.Histogram("dataquery_endpoint_request_duration_seconds", nil).Count(); got != requests {
				t.Fatalf("duration count=%d", got)
			}
			for _, outcome := range []string{"error", "cancelled"} {
				expected := int64(0)
				cancelled := errors.Is(tc.failure, context.Canceled) || errors.Is(tc.failure, context.DeadlineExceeded)
				if tc.failure != nil && !tc.replay && ((outcome == "cancelled") == cancelled) {
					expected = 1
				}
				if got := registry.Counter("dataquery_endpoint_failures_total", metrics.Labels{"outcome": outcome}).Count(); got != expected {
					t.Fatalf("%s failures=%d", outcome, got)
				}
			}
		})
	}
}
