/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package opentelemetry

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/stats"
)

func metricName(opts metrics.Opts) string {
	parts := make([]string, 0, 3)
	if opts.Namespace != "" {
		parts = append(parts, opts.Namespace)
	}
	if opts.Subsystem != "" {
		parts = append(parts, opts.Subsystem)
	}
	parts = append(parts, opts.Name)
	return strings.Join(parts, ".")
}

func attrsFromLabels(labels metrics.Labels) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(labels))
	for k, v := range labels {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}

// resolveMaxCardinality converts GaugeOpts.MaxCardinality to an internal limit:
// 0 → DefaultMaxCardinality, negative → 0 (unlimited).
func resolveMaxCardinality(v int) int {
	if v == 0 {
		return metrics.DefaultMaxCardinality
	}
	if v < 0 {
		return 0
	}
	return v
}

// labelsKey produces a stable string key for a Labels map by sorting keys first.
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

// ── Int64Counter ──────────────────────────────────────────────────────────────

type int64Counter struct {
	c     otelmetric.Int64Counter
	attrs []attribute.KeyValue
}

func (c *int64Counter) Inc(ctx context.Context) {
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	c.c.Add(ctx, 1, otelmetric.WithAttributes(c.attrs...))
}

func (c *int64Counter) Add(ctx context.Context, v int64) {
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	c.c.Add(ctx, v, otelmetric.WithAttributes(c.attrs...))
}

type int64CounterVec struct {
	c otelmetric.Int64Counter
}

func (cv *int64CounterVec) With(labels metrics.Labels) metrics.Int64Counter {
	return &int64Counter{c: cv.c, attrs: attrsFromLabels(labels)}
}

// ── Float64Counter ────────────────────────────────────────────────────────────

type float64Counter struct {
	c     otelmetric.Float64Counter
	attrs []attribute.KeyValue
}

func (c *float64Counter) Add(ctx context.Context, v float64) {
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	c.c.Add(ctx, v, otelmetric.WithAttributes(c.attrs...))
}

type float64CounterVec struct {
	c otelmetric.Float64Counter
}

func (cv *float64CounterVec) With(labels metrics.Labels) metrics.Float64Counter {
	return &float64Counter{c: cv.c, attrs: attrsFromLabels(labels)}
}

// ── Noop types (returned on error or when cardinality limit is exceeded) ──────

type noopFloat64Gauge struct{}

func (noopFloat64Gauge) Set(float64) {}
func (noopFloat64Gauge) Inc()        {}
func (noopFloat64Gauge) Dec()        {}
func (noopFloat64Gauge) Add(float64) {}
func (noopFloat64Gauge) Sub(float64) {}

type noopInt64Gauge struct{}

func (noopInt64Gauge) Set(int64) {}
func (noopInt64Gauge) Inc()      {}
func (noopInt64Gauge) Dec()      {}
func (noopInt64Gauge) Add(int64) {}
func (noopInt64Gauge) Sub(int64) {}

// ── MetricsScope ──────────────────────────────────────────────────────────────

type metricsScope struct {
	m      *Metrics
	prefix string
	base   metrics.Labels
}

func (s *metricsScope) mergeLabels(extra metrics.Labels) metrics.Labels {
	if len(extra) == 0 {
		return s.base
	}
	merged := make(metrics.Labels, len(s.base)+len(extra))
	for k, v := range s.base {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}
	return merged
}

func (s *metricsScope) fullName(name string) string {
	if name == "" {
		return s.prefix
	}
	return s.prefix + "_" + name
}

func (s *metricsScope) Counter(name, help string, labels metrics.Labels) (metrics.Int64Counter, error) {
	c, err := s.m.Int64Counter(metrics.CounterOpts{
		Opts: metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.mergeLabels(labels)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create counter %q: %w", s.fullName(name), err)
	}
	return c, nil
}

func (s *metricsScope) Gauge(name, help string, labels metrics.Labels) (metrics.Int64Gauge, error) {
	g, err := s.m.Int64Gauge(metrics.GaugeOpts{
		Opts: metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.mergeLabels(labels)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gauge %q: %w", s.fullName(name), err)
	}
	return g, nil
}

func (s *metricsScope) CounterVec(name, help string) (metrics.Int64CounterVec, error) {
	c, err := s.m.Int64CounterVec(metrics.CounterOpts{
		Opts: metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.base},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create counter vec %q: %w", s.fullName(name), err)
	}
	return c, nil
}

func (s *metricsScope) GaugeVec(name, help string) (metrics.Int64GaugeVec, error) {
	g, err := s.m.Int64GaugeVec(metrics.GaugeOpts{
		Opts: metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.base},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gauge vec %q: %w", s.fullName(name), err)
	}
	return g, nil
}

func (s *metricsScope) Histogram(name, help string, labels metrics.Labels, buckets ...float64) (metrics.Float64Histogram, error) {
	h, err := s.m.Float64Histogram(metrics.HistogramOpts{
		Opts:    metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.mergeLabels(labels)},
		Buckets: buckets,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create histogram %q: %w", s.fullName(name), err)
	}
	return h, nil
}

func (s *metricsScope) HistogramVec(name, help string, buckets ...float64) (metrics.Float64HistogramVec, error) {
	h, err := s.m.Float64HistogramVec(metrics.HistogramOpts{
		Opts:    metrics.Opts{Name: s.fullName(name), Help: help, ConstLabels: s.base},
		Buckets: buckets,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create histogram vec %q: %w", s.fullName(name), err)
	}
	return h, nil
}

func (s *metricsScope) ObservableFloat64Gauge(name, help string, fn func() float64) error {
	obs, err := s.m.meter.Float64ObservableGauge(s.fullName(name), otelmetric.WithDescription(help))
	if err != nil {
		return fmt.Errorf("failed to create observable gauge %q: %w", s.fullName(name), err)
	}
	base := attrsFromLabels(s.base)
	_, err = s.m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) error {
		o.ObserveFloat64(obs, fn(), otelmetric.WithAttributes(base...))
		return nil
	}, obs)
	if err != nil {
		return fmt.Errorf("failed to register callback for observable gauge %q: %w", s.fullName(name), err)
	}
	return nil
}

// ── Float64Gauge (observable) ─────────────────────────────────────────────────

type float64Gauge struct {
	attrs []attribute.KeyValue
	val   atomic.Uint64
}

func (g *float64Gauge) load() float64 { return math.Float64frombits(g.val.Load()) }

func (g *float64Gauge) add(delta float64) {
	for {
		old := g.val.Load()
		if g.val.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+delta)) {
			return
		}
	}
}

func (g *float64Gauge) Set(v float64)     { g.val.Store(math.Float64bits(v)) }
func (g *float64Gauge) Inc()              { g.add(1) }
func (g *float64Gauge) Dec()              { g.add(-1) }
func (g *float64Gauge) Add(delta float64) { g.add(delta) }
func (g *float64Gauge) Sub(delta float64) { g.add(-delta) }

// float64GaugeVec stores one gauge instance per unique label combination.
// Use only low-cardinality label values; every distinct combination allocates
// a permanent entry. Call Delete when the monitored entity is gone to prevent
// unbounded growth in long-running services.
type float64GaugeVec struct {
	obs            otelmetric.Float64ObservableGauge
	instances      sync.Map // key: string → *float64Gauge
	count          atomic.Int64
	dropped        atomic.Int64
	maxCardinality int
}

func (gv *float64GaugeVec) With(labels metrics.Labels) metrics.Float64Gauge {
	key := labelsKey(labels)
	if g, ok := gv.instances.Load(key); ok {
		return g.(*float64Gauge)
	}
	if gv.maxCardinality > 0 {
		for {
			curr := gv.count.Load()
			if curr >= int64(gv.maxCardinality) {
				gv.dropped.Add(1)
				return noopFloat64Gauge{}
			}
			if gv.count.CompareAndSwap(curr, curr+1) {
				break
			}
		}
		g := &float64Gauge{attrs: attrsFromLabels(labels)}
		if actual, loaded := gv.instances.LoadOrStore(key, g); loaded {
			gv.count.Add(-1) // key already existed, release reserved slot
			return actual.(*float64Gauge)
		}
		return g
	}
	g := &float64Gauge{attrs: attrsFromLabels(labels)}
	if actual, loaded := gv.instances.LoadOrStore(key, g); loaded {
		return actual.(*float64Gauge)
	}
	return g
}

func (gv *float64GaugeVec) Delete(labels metrics.Labels) {
	if _, loaded := gv.instances.LoadAndDelete(labelsKey(labels)); loaded {
		gv.count.Add(-1)
	}
}

// ── Int64Gauge (observable) ───────────────────────────────────────────────────

type int64Gauge struct {
	attrs []attribute.KeyValue
	val   atomic.Int64
}

func (g *int64Gauge) Set(v int64)     { g.val.Store(v) }
func (g *int64Gauge) Inc()            { g.val.Add(1) }
func (g *int64Gauge) Dec()            { g.val.Add(-1) }
func (g *int64Gauge) Add(delta int64) { g.val.Add(delta) }
func (g *int64Gauge) Sub(delta int64) { g.val.Add(-delta) }

// int64GaugeVec stores one gauge instance per unique label combination.
// Use only low-cardinality label values; every distinct combination allocates
// a permanent entry. Call Delete when the monitored entity is gone to prevent
// unbounded growth in long-running services.
type int64GaugeVec struct {
	obs            otelmetric.Int64ObservableGauge
	instances      sync.Map // key: string → *int64Gauge
	count          atomic.Int64
	dropped        atomic.Int64
	maxCardinality int
}

func (gv *int64GaugeVec) With(labels metrics.Labels) metrics.Int64Gauge {
	key := labelsKey(labels)
	if g, ok := gv.instances.Load(key); ok {
		return g.(*int64Gauge)
	}
	if gv.maxCardinality > 0 {
		for {
			curr := gv.count.Load()
			if curr >= int64(gv.maxCardinality) {
				gv.dropped.Add(1)
				return noopInt64Gauge{}
			}
			if gv.count.CompareAndSwap(curr, curr+1) {
				break
			}
		}
		g := &int64Gauge{attrs: attrsFromLabels(labels)}
		if actual, loaded := gv.instances.LoadOrStore(key, g); loaded {
			gv.count.Add(-1) // key already existed, release reserved slot
			return actual.(*int64Gauge)
		}
		return g
	}
	g := &int64Gauge{attrs: attrsFromLabels(labels)}
	if actual, loaded := gv.instances.LoadOrStore(key, g); loaded {
		return actual.(*int64Gauge)
	}
	return g
}

func (gv *int64GaugeVec) Delete(labels metrics.Labels) {
	if _, loaded := gv.instances.LoadAndDelete(labelsKey(labels)); loaded {
		gv.count.Add(-1)
	}
}

// ── Float64Histogram ──────────────────────────────────────────────────────────

type float64Histogram struct {
	h     otelmetric.Float64Histogram
	attrs []attribute.KeyValue
}

func (h *float64Histogram) Observe(ctx context.Context, v float64) {
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	h.h.Record(ctx, v, otelmetric.WithAttributes(h.attrs...))
}

type float64HistogramVec struct {
	h otelmetric.Float64Histogram
}

func (hv *float64HistogramVec) With(labels metrics.Labels) metrics.Float64Histogram {
	return &float64Histogram{h: hv.h, attrs: attrsFromLabels(labels)}
}

// ── Int64Histogram ────────────────────────────────────────────────────────────

type int64Histogram struct {
	h     otelmetric.Int64Histogram
	attrs []attribute.KeyValue
}

func (h *int64Histogram) Observe(ctx context.Context, v int64) {
	if !metrics.RecordingEnabled(ctx) {
		return
	}
	h.h.Record(ctx, v, otelmetric.WithAttributes(h.attrs...))
}

type int64HistogramVec struct {
	h otelmetric.Int64Histogram
}

func (hv *int64HistogramVec) With(labels metrics.Labels) metrics.Int64Histogram {
	return &int64Histogram{h: hv.h, attrs: attrsFromLabels(labels)}
}

// ── Metrics factory ───────────────────────────────────────────────────────────

type Metrics struct {
	meter  otelmetric.Meter
	scopes map[string]*metricsScope
	mu     sync.Mutex
}

func (m *Metrics) Int64Counter(opts metrics.CounterOpts) (metrics.Int64Counter, error) {
	c, err := m.meter.Int64Counter(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	return &int64Counter{c: c, attrs: attrsFromLabels(opts.ConstLabels)}, nil
}

func (m *Metrics) Int64CounterVec(opts metrics.CounterOpts) (metrics.Int64CounterVec, error) {
	c, err := m.meter.Int64Counter(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	return &int64CounterVec{c: c}, nil
}

func (m *Metrics) Float64Counter(opts metrics.CounterOpts) (metrics.Float64Counter, error) {
	c, err := m.meter.Float64Counter(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	return &float64Counter{c: c, attrs: attrsFromLabels(opts.ConstLabels)}, nil
}

func (m *Metrics) Float64CounterVec(opts metrics.CounterOpts) (metrics.Float64CounterVec, error) {
	c, err := m.meter.Float64Counter(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	return &float64CounterVec{c: c}, nil
}

func (m *Metrics) Float64Gauge(opts metrics.GaugeOpts) (metrics.Float64Gauge, error) {
	obs, err := m.meter.Float64ObservableGauge(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	g := &float64Gauge{attrs: attrsFromLabels(opts.ConstLabels)}
	_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) error {
		o.ObserveFloat64(obs, g.load(), otelmetric.WithAttributes(g.attrs...))
		return nil
	}, obs)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (m *Metrics) Float64GaugeVec(opts metrics.GaugeOpts) (metrics.Float64GaugeVec, error) {
	obs, err := m.meter.Float64ObservableGauge(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	maxCard := resolveMaxCardinality(opts.MaxCardinality)
	gv := &float64GaugeVec{obs: obs, maxCardinality: maxCard}
	_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				retErr = fmt.Errorf("float64GaugeVec callback panic: %v", r)
			}
		}()
		gv.instances.Range(func(_, v any) bool {
			g := v.(*float64Gauge)
			o.ObserveFloat64(obs, g.load(), otelmetric.WithAttributes(g.attrs...))
			return true
		})
		return nil
	}, obs)
	if err != nil {
		return nil, err
	}
	if maxCard > 0 {
		droppedObs, err := m.meter.Int64ObservableCounter(
			metricName(opts.Opts)+".dropped_series_total",
			otelmetric.WithDescription("Total series dropped due to cardinality limit on "+metricName(opts.Opts)),
		)
		if err != nil {
			return nil, err
		}
		_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) error {
			o.ObserveInt64(droppedObs, gv.dropped.Load())
			return nil
		}, droppedObs)
		if err != nil {
			return nil, err
		}
	}
	return gv, nil
}

func (m *Metrics) Int64Gauge(opts metrics.GaugeOpts) (metrics.Int64Gauge, error) {
	obs, err := m.meter.Int64ObservableGauge(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	g := &int64Gauge{attrs: attrsFromLabels(opts.ConstLabels)}
	_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) error {
		o.ObserveInt64(obs, g.val.Load(), otelmetric.WithAttributes(g.attrs...))
		return nil
	}, obs)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (m *Metrics) Int64GaugeVec(opts metrics.GaugeOpts) (metrics.Int64GaugeVec, error) {
	obs, err := m.meter.Int64ObservableGauge(metricName(opts.Opts), otelmetric.WithDescription(opts.Help))
	if err != nil {
		return nil, err
	}
	maxCard := resolveMaxCardinality(opts.MaxCardinality)
	gv := &int64GaugeVec{obs: obs, maxCardinality: maxCard}
	_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				retErr = fmt.Errorf("int64GaugeVec callback panic: %v", r)
			}
		}()
		gv.instances.Range(func(_, v any) bool {
			g := v.(*int64Gauge)
			o.ObserveInt64(obs, g.val.Load(), otelmetric.WithAttributes(g.attrs...))
			return true
		})
		return nil
	}, obs)
	if err != nil {
		return nil, err
	}
	if maxCard > 0 {
		droppedObs, err := m.meter.Int64ObservableCounter(
			metricName(opts.Opts)+".dropped_series_total",
			otelmetric.WithDescription("Total series dropped due to cardinality limit on "+metricName(opts.Opts)),
		)
		if err != nil {
			return nil, err
		}
		_, err = m.meter.RegisterCallback(func(_ context.Context, o otelmetric.Observer) error {
			o.ObserveInt64(droppedObs, gv.dropped.Load())
			return nil
		}, droppedObs)
		if err != nil {
			return nil, err
		}
	}
	return gv, nil
}

func (m *Metrics) Float64Histogram(opts metrics.HistogramOpts) (metrics.Float64Histogram, error) {
	h, err := m.meter.Float64Histogram(metricName(opts.Opts),
		otelmetric.WithDescription(opts.Help),
		otelmetric.WithExplicitBucketBoundaries(opts.Buckets...),
	)
	if err != nil {
		return nil, err
	}
	return &float64Histogram{h: h, attrs: attrsFromLabels(opts.ConstLabels)}, nil
}

func (m *Metrics) Float64HistogramVec(opts metrics.HistogramOpts) (metrics.Float64HistogramVec, error) {
	h, err := m.meter.Float64Histogram(metricName(opts.Opts),
		otelmetric.WithDescription(opts.Help),
		otelmetric.WithExplicitBucketBoundaries(opts.Buckets...),
	)
	if err != nil {
		return nil, err
	}
	return &float64HistogramVec{h: h}, nil
}

func (m *Metrics) Int64Histogram(opts metrics.HistogramOpts) (metrics.Int64Histogram, error) {
	h, err := m.meter.Int64Histogram(metricName(opts.Opts),
		otelmetric.WithDescription(opts.Help),
		otelmetric.WithExplicitBucketBoundaries(opts.Buckets...),
	)
	if err != nil {
		return nil, err
	}
	return &int64Histogram{h: h, attrs: attrsFromLabels(opts.ConstLabels)}, nil
}

func (m *Metrics) Int64HistogramVec(opts metrics.HistogramOpts) (metrics.Int64HistogramVec, error) {
	h, err := m.meter.Int64Histogram(metricName(opts.Opts),
		otelmetric.WithDescription(opts.Help),
		otelmetric.WithExplicitBucketBoundaries(opts.Buckets...),
	)
	if err != nil {
		return nil, err
	}
	return &int64HistogramVec{h: h}, nil
}

func (m *Metrics) Scope(prefix string, base metrics.Labels) metrics.MetricsScope {
	key := prefix + "\x00" + labelsKey(base)
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.scopes[key]; ok {
		return s
	}
	s := &metricsScope{m: m, prefix: prefix, base: base}
	if m.scopes == nil {
		m.scopes = make(map[string]*metricsScope)
	}
	m.scopes[key] = s
	return s
}

// ── MetricsEngine ─────────────────────────────────────────────────────────────

type MetricsEngine struct {
	environment environment.ServiceEnvironment
	metricsImpl *Metrics
	handler     http.Handler
	provider    *sdkmetric.MeterProvider
}

func (m *MetricsEngine) Metrics() metrics.Metrics {
	return m.metricsImpl
}

func (m *MetricsEngine) HTTPMetricsHandler() http.Handler {
	return m.handler
}

func (m *MetricsEngine) GRPCStatsHandler() stats.Handler {
	return otelgrpc.NewServerHandler(
		otelgrpc.WithMeterProvider(m.provider),
		otelgrpc.WithTracerProvider(nooptrace.NewTracerProvider()),
	)
}

func (m *MetricsEngine) GRPCClientHandler() stats.Handler {
	return otelgrpc.NewClientHandler(
		otelgrpc.WithMeterProvider(m.provider),
		otelgrpc.WithTracerProvider(nooptrace.NewTracerProvider()),
	)
}

func (m *MetricsEngine) HTTPClientTransport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base,
		otelhttp.WithMeterProvider(m.provider),
		otelhttp.WithTracerProvider(nooptrace.NewTracerProvider()),
	)
}

func (m *MetricsEngine) HTTPServerHandler(mux http.Handler, name string) http.Handler {
	return otelhttp.NewHandler(mux, name,
		otelhttp.WithMeterProvider(m.provider),
		otelhttp.WithTracerProvider(nooptrace.NewTracerProvider()),
	)
}

func (m *MetricsEngine) Shutdown(ctx context.Context) error {
	return m.provider.Shutdown(ctx)
}

// MetricsEngineOption configures a MetricsEngine at creation time.
type MetricsEngineOption func(*metricsEngineOptions)

type metricsEngineOptions struct {
	views []sdkmetric.View
}

// WithView adds an OTel SDK View to the MeterProvider.
func WithView(view sdkmetric.View) MetricsEngineOption {
	return func(o *metricsEngineOptions) {
		o.views = append(o.views, view)
	}
}

// WithHistogramBuckets sets explicit bucket boundaries for a named instrument.
func WithHistogramBuckets(instrumentName string, boundaries []float64) MetricsEngineOption {
	return WithView(sdkmetric.NewView(
		sdkmetric.Instrument{Name: instrumentName},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: boundaries,
		}},
	))
}

// GRPCDurationBuckets provides sub-millisecond resolution suitable for fast gRPC calls.
var GRPCDurationBuckets = []float64{
	0.0001, 0.0002, 0.0005,
	0.001, 0.002, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1,
}

// defaultGRPCViews returns the built-in views for gRPC duration histograms.
func defaultGRPCViews() []sdkmetric.View {
	names := []string{
		"rpc.server.call.duration",
		"rpc.client.call.duration",
		"rpc.server.duration",
		"rpc.client.duration",
	}
	views := make([]sdkmetric.View, len(names))
	for i, name := range names {
		views[i] = sdkmetric.NewView(
			sdkmetric.Instrument{Name: name},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: GRPCDurationBuckets,
			}},
		)
	}
	return views
}

func buildProviderOptions(readerOpt sdkmetric.Option, env environment.ServiceEnvironment, userOpts []MetricsEngineOption) []sdkmetric.Option {
	o := &metricsEngineOptions{}
	for _, opt := range userOpts {
		opt(o)
	}
	// User views first — they take precedence over defaults (first match wins in OTel SDK).
	providerOpts := []sdkmetric.Option{readerOpt, sdkmetric.WithResource(serviceResource(env))}
	for _, v := range o.views {
		providerOpts = append(providerOpts, sdkmetric.WithView(v))
	}
	for _, v := range defaultGRPCViews() {
		providerOpts = append(providerOpts, sdkmetric.WithView(v))
	}
	return providerOpts
}

func CreatePrometheusMetricsEngine(env environment.ServiceEnvironment, opts ...MetricsEngineOption) (metrics.MetricsEngine, error) {
	registry := promclient.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(buildProviderOptions(sdkmetric.WithReader(exporter), env, opts)...)
	meter := provider.Meter("github.com/gorundebug/servicelib")
	return &MetricsEngine{
		environment: env,
		metricsImpl: &Metrics{meter: meter},
		handler:     promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		provider:    provider,
	}, nil
}

func CreateOTLPMetricsEngine(ctx context.Context, env environment.ServiceEnvironment, opts ...MetricsEngineOption) (metrics.MetricsEngine, error) {
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(buildProviderOptions(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)), env, opts)...)
	meter := provider.Meter("github.com/gorundebug/servicelib")
	return &MetricsEngine{
		environment: env,
		metricsImpl: &Metrics{meter: meter},
		handler:     nil,
		provider:    provider,
	}, nil
}
