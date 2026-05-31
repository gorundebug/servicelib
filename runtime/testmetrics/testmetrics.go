/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// Package testmetrics provides an in-memory metrics engine for use in automated
// tests. It implements metrics.MetricsEngine and allows reading back recorded
// values via Counter, Gauge and Histogram accessor methods.
//
// Usage:
//
//	engine := testmetrics.New()
//	svc, _ := runtime.MakeService(ctx, "svc", dep, ...)
//	// dep.MetricsEngine() returns engine
//
//	sendRequest()
//
//	assert.Equal(t, int64(1),
//	    engine.Counter("datasource_endpoint_messages_total",
//	        metrics.Labels{"connector": "http", "endpoint": "data"}).Count())
package testmetrics

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"google.golang.org/grpc/stats"
)

// ── TestInt64Counter ──────────────────────────────────────────────────────────

type TestInt64Counter struct {
	count atomic.Int64
}

func (c *TestInt64Counter) Inc(_ context.Context)          { c.count.Add(1) }
func (c *TestInt64Counter) Add(_ context.Context, v int64) { c.count.Add(v) }

// Count returns the current counter value.
func (c *TestInt64Counter) Count() int64 { return c.count.Load() }

// ── TestInt64Gauge ────────────────────────────────────────────────────────────

type TestInt64Gauge struct {
	val atomic.Int64
}

func (g *TestInt64Gauge) Set(v int64)     { g.val.Store(v) }
func (g *TestInt64Gauge) Inc()            { g.val.Add(1) }
func (g *TestInt64Gauge) Dec()            { g.val.Add(-1) }
func (g *TestInt64Gauge) Add(delta int64) { g.val.Add(delta) }
func (g *TestInt64Gauge) Sub(delta int64) { g.val.Add(-delta) }

// Value returns the current gauge value.
func (g *TestInt64Gauge) Value() int64 { return g.val.Load() }

// ── TestFloat64Histogram ──────────────────────────────────────────────────────

type TestFloat64Histogram struct {
	mu     sync.Mutex
	count  int64
	sum    float64
	values []float64
}

func (h *TestFloat64Histogram) Observe(_ context.Context, v float64) {
	h.mu.Lock()
	h.count++
	h.sum += v
	h.values = append(h.values, v)
	h.mu.Unlock()
}

// Count returns the number of observations.
func (h *TestFloat64Histogram) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// Sum returns the sum of all observed values.
func (h *TestFloat64Histogram) Sum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// Values returns a copy of all individual observed values.
func (h *TestFloat64Histogram) Values() []float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]float64, len(h.values))
	copy(cp, h.values)
	return cp
}

// ── Vec adapters ──────────────────────────────────────────────────────────────

type testInt64CounterVec struct {
	m    *TestMetrics
	name string
	base metrics.Labels
}

func (v *testInt64CounterVec) With(labels metrics.Labels) metrics.Int64Counter {
	return v.m.getOrCreateCounter(v.name, mergeLabels(v.base, labels))
}

type testInt64GaugeVec struct {
	m    *TestMetrics
	name string
	base metrics.Labels
}

func (v *testInt64GaugeVec) With(labels metrics.Labels) metrics.Int64Gauge {
	return v.m.getOrCreateGauge(v.name, mergeLabels(v.base, labels))
}

func (v *testInt64GaugeVec) Delete(_ metrics.Labels) {}

type testFloat64HistogramVec struct {
	m    *TestMetrics
	name string
	base metrics.Labels
}

func (v *testFloat64HistogramVec) With(labels metrics.Labels) metrics.Float64Histogram {
	return v.m.getOrCreateHistogram(v.name, mergeLabels(v.base, labels))
}

// ── testScope ─────────────────────────────────────────────────────────────────

type testScope struct {
	m      *TestMetrics
	prefix string
	base   metrics.Labels
}

func (s *testScope) fullName(name string) string {
	if name == "" {
		return s.prefix
	}
	return s.prefix + "_" + name
}

func (s *testScope) Counter(name, _ string, labels metrics.Labels) (metrics.Int64Counter, error) {
	return s.m.getOrCreateCounter(s.fullName(name), mergeLabels(s.base, labels)), nil
}

func (s *testScope) CounterVec(name, _ string) (metrics.Int64CounterVec, error) {
	return &testInt64CounterVec{m: s.m, name: s.fullName(name), base: s.base}, nil
}

func (s *testScope) Gauge(name, _ string, labels metrics.Labels) (metrics.Int64Gauge, error) {
	return s.m.getOrCreateGauge(s.fullName(name), mergeLabels(s.base, labels)), nil
}

func (s *testScope) GaugeVec(name, _ string) (metrics.Int64GaugeVec, error) {
	return &testInt64GaugeVec{m: s.m, name: s.fullName(name), base: s.base}, nil
}

func (s *testScope) Histogram(name, _ string, labels metrics.Labels, _ ...float64) (metrics.Float64Histogram, error) {
	return s.m.getOrCreateHistogram(s.fullName(name), mergeLabels(s.base, labels)), nil
}

func (s *testScope) HistogramVec(name, _ string, _ ...float64) (metrics.Float64HistogramVec, error) {
	return &testFloat64HistogramVec{m: s.m, name: s.fullName(name), base: s.base}, nil
}

// ── TestMetrics ───────────────────────────────────────────────────────────────

// TestMetrics implements metrics.MetricsEngine with in-memory storage.
// All metric instances are accessible by name and labels for test assertions.
type TestMetrics struct {
	mu         sync.Mutex
	counters   map[string]*TestInt64Counter
	gauges     map[string]*TestInt64Gauge
	histograms map[string]*TestFloat64Histogram
	scopes     map[string]*testScope
}

// New creates a new TestMetrics instance.
func New() *TestMetrics {
	return &TestMetrics{
		counters:   make(map[string]*TestInt64Counter),
		gauges:     make(map[string]*TestInt64Gauge),
		histograms: make(map[string]*TestFloat64Histogram),
		scopes:     make(map[string]*testScope),
	}
}

// Reset clears all recorded metric values and scopes. Useful between test cases.
func (m *TestMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters = make(map[string]*TestInt64Counter)
	m.gauges = make(map[string]*TestInt64Gauge)
	m.histograms = make(map[string]*TestFloat64Histogram)
	m.scopes = make(map[string]*testScope)
}

// ── metrics.MetricsEngine ─────────────────────────────────────────────────────

func (m *TestMetrics) Metrics() metrics.Metrics                                     { return m }
func (m *TestMetrics) HTTPMetricsHandler() http.Handler                             { return http.NotFoundHandler() }
func (m *TestMetrics) GRPCStatsHandler() stats.Handler                              { return noopStats{} }
func (m *TestMetrics) GRPCClientHandler() stats.Handler                             { return noopStats{} }
func (m *TestMetrics) HTTPClientTransport(base http.RoundTripper) http.RoundTripper { return base }
func (m *TestMetrics) HTTPServerHandler(mux http.Handler, _ string) http.Handler    { return mux }
func (m *TestMetrics) Shutdown(_ context.Context) error                             { return nil }

// ── metrics.Metrics ───────────────────────────────────────────────────────────

func (m *TestMetrics) Scope(prefix string, base metrics.Labels) metrics.MetricsScope {
	key := prefix + "\x00" + labelsKey(base)
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.scopes[key]; ok {
		return s
	}
	s := &testScope{m: m, prefix: prefix, base: base}
	m.scopes[key] = s
	return s
}

// ── Test accessors ────────────────────────────────────────────────────────────

// Counter returns the TestInt64Counter for the given name and labels.
// If the metric has not been recorded yet, a zero-value instance is returned
// so the assertion still works (value will be 0).
func (m *TestMetrics) Counter(name string, labels metrics.Labels) *TestInt64Counter {
	return m.getOrCreateCounter(name, labels)
}

// Gauge returns the TestInt64Gauge for the given name and labels.
func (m *TestMetrics) Gauge(name string, labels metrics.Labels) *TestInt64Gauge {
	return m.getOrCreateGauge(name, labels)
}

// Histogram returns the TestFloat64Histogram for the given name and labels.
func (m *TestMetrics) Histogram(name string, labels metrics.Labels) *TestFloat64Histogram {
	return m.getOrCreateHistogram(name, labels)
}

// ── internal ──────────────────────────────────────────────────────────────────

func (m *TestMetrics) getOrCreateCounter(name string, labels metrics.Labels) *TestInt64Counter {
	key := metricKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[key]; ok {
		return c
	}
	c := &TestInt64Counter{}
	m.counters[key] = c
	return c
}

func (m *TestMetrics) getOrCreateGauge(name string, labels metrics.Labels) *TestInt64Gauge {
	key := metricKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.gauges[key]; ok {
		return g
	}
	g := &TestInt64Gauge{}
	m.gauges[key] = g
	return g
}

func (m *TestMetrics) getOrCreateHistogram(name string, labels metrics.Labels) *TestFloat64Histogram {
	key := metricKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.histograms[key]; ok {
		return h
	}
	h := &TestFloat64Histogram{}
	m.histograms[key] = h
	return h
}

// RegisteredNames returns the sorted unique metric names that have been
// registered (via scope.Counter/Gauge/Histogram calls) since the last Reset.
// Use this in tests to verify that a component registers all expected metrics.
func (m *TestMetrics) RegisteredNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]bool)
	for key := range m.counters {
		seen[nameFromKey(key)] = true
	}
	for key := range m.gauges {
		seen[nameFromKey(key)] = true
	}
	for key := range m.histograms {
		seen[nameFromKey(key)] = true
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func nameFromKey(key string) string {
	if i := strings.IndexByte(key, '\x00'); i >= 0 {
		return key[:i]
	}
	return key
}

func labelsKey(labels metrics.Labels) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte('\x00')
	}
	return b.String()
}

func metricKey(name string, labels metrics.Labels) string {
	return name + "\x00" + labelsKey(labels)
}

func mergeLabels(base, extra metrics.Labels) metrics.Labels {
	if len(extra) == 0 {
		return base
	}
	if len(base) == 0 {
		return extra
	}
	merged := make(metrics.Labels, len(base)+len(extra))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}
	return merged
}

// ── noop gRPC stats handler ───────────────────────────────────────────────────

type noopStats struct{}

func (noopStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (noopStats) HandleRPC(context.Context, stats.RPCStats)                         {}
func (noopStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (noopStats) HandleConn(context.Context, stats.ConnStats)                       {}
