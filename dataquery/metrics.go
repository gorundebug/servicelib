package dataquery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

// Handles are allocated once when binding an endpoint, never per result.
// Labels describe topology only; request values and SQL are never inspected.
type endpointMetrics struct {
	requests, results       metrics.Int64Counter
	failures, cancellations metrics.Int64Counter
	active                  metrics.Int64Gauge
	duration                metrics.Float64Histogram
}

func makeEndpointMetrics(env runtime.RuntimeEnvironment, endpointID func() int) (*endpointMetrics, error) {
	m := env.Metrics()
	if metrics.IsNoop(m) {
		return nil, nil
	}
	cfg, ok := env.RuntimeConfig().GetEndpointConfigByID(endpointID()).(*config.QueryEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("Query metrics require a Query endpoint configuration")
	}
	connector := env.RuntimeConfig().GetDataConnectorByID(cfg.IdDataConnector)
	if connector == nil {
		return nil, fmt.Errorf("Query endpoint %q has no connector", cfg.Name)
	}
	return newEndpointMetrics(m.Scope("dataquery_endpoint", metrics.Labels{
		"connector": connector.GetName(), "endpoint": cfg.Name, "operation": string(cfg.QueryOperation),
	}))
}

func newEndpointMetrics(scope metrics.MetricsScope) (*endpointMetrics, error) {
	m := &endpointMetrics{}
	var err error
	if m.requests, err = scope.Counter("requests_total", "Admitted Query endpoint requests", nil); err != nil {
		return nil, err
	}
	if m.results, err = scope.Counter("results_total", "Query result collector emissions, including application completion", nil); err != nil {
		return nil, err
	}
	if m.failures, err = scope.Counter("failures_total", "Failed Query endpoint requests", metrics.Labels{"outcome": "error"}); err != nil {
		return nil, err
	}
	if m.cancellations, err = scope.Counter("failures_total", "Failed Query endpoint requests", metrics.Labels{"outcome": "cancelled"}); err != nil {
		return nil, err
	}
	if m.active, err = scope.Gauge("active_requests", "Active Query endpoint requests through EndRequest", nil); err != nil {
		return nil, err
	}
	if m.duration, err = scope.Histogram("request_duration_seconds", "Query request duration through EndRequest in seconds", nil); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *endpointMetrics) begin(ctx context.Context) time.Time {
	if m == nil || !metrics.RecordingEnabled(ctx) {
		return time.Time{}
	}
	m.requests.Inc(ctx)
	m.active.Inc()
	return time.Now()
}

func (m *endpointMetrics) finish(ctx context.Context, start time.Time, err error) {
	if start.IsZero() {
		return
	}
	defer m.active.Dec()
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	m.duration.Observe(ctx, time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			m.cancellations.Inc(ctx)
		} else {
			m.failures.Inc(ctx)
		}
	}
}
