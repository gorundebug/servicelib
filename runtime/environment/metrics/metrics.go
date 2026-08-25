/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package metrics

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc/stats"
)

type MetricsEngine interface {
	HTTPMetricsHandler() http.Handler
	GRPCStatsHandler() stats.Handler
	GRPCClientHandler() stats.Handler
	HTTPClientTransport(base http.RoundTripper) http.RoundTripper
	HTTPServerHandler(mux http.Handler, name string) http.Handler
	Metrics() Metrics
	Shutdown(ctx context.Context) error
}

type Int64Counter interface {
	Inc(ctx context.Context)
	Add(ctx context.Context, v int64)
}

type Int64CounterVec interface {
	With(labels Labels) Int64Counter
}

type Float64Counter interface {
	Add(ctx context.Context, v float64)
}

type Float64CounterVec interface {
	With(labels Labels) Float64Counter
}

type Float64Gauge interface {
	Set(v float64)
	Inc()
	Dec()
	Add(delta float64)
	Sub(delta float64)
}

type Float64GaugeVec interface {
	With(labels Labels) Float64Gauge
	Delete(labels Labels)
}

type Int64Gauge interface {
	Set(v int64)
	Inc()
	Dec()
	Add(delta int64)
	Sub(delta int64)
}

type Int64GaugeVec interface {
	With(labels Labels) Int64Gauge
	Delete(labels Labels)
}

type Float64Histogram interface {
	Observe(ctx context.Context, v float64)
}

type Float64HistogramVec interface {
	With(labels Labels) Float64Histogram
}

type Int64Histogram interface {
	Observe(ctx context.Context, v int64)
}

type Int64HistogramVec interface {
	With(labels Labels) Int64Histogram
}

type Labels map[string]string

type Opts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Help        string
	ConstLabels Labels
}

type CounterOpts struct {
	Opts
}

// DefaultMaxCardinality is the default limit of unique label combinations for
// GaugeVec metrics. Set GaugeOpts.MaxCardinality = -1 to disable the limit.
const DefaultMaxCardinality = 1000

type GaugeOpts struct {
	Opts
	// MaxCardinality limits the number of unique label combinations for Vec
	// metrics. When the limit is reached, With() returns a no-op gauge and the
	// dropped series counter is incremented.
	// 0 uses DefaultMaxCardinality. -1 disables the limit.
	MaxCardinality int
}

type HistogramOpts struct {
	Opts
	Buckets                         []float64
	NativeHistogramBucketFactor     float64
	NativeHistogramZeroThreshold    float64
	NativeHistogramMaxBucketNumber  uint32
	NativeHistogramMinResetDuration time.Duration
	NativeHistogramMaxZeroThreshold float64
	NativeHistogramMaxExemplars     int
	NativeHistogramExemplarTTL      time.Duration
}

// MetricsScope is a factory bound to a fixed name prefix and a set of base
// labels. Calling Counter/Gauge/Histogram on it creates metrics whose full
// name is "<prefix>_<name>" and whose labels are the base labels merged with
// any extra labels supplied to the method.
// Vec variants use base labels as ConstLabels; variable labels are supplied
// via .With() at observation time.
// Histogram and HistogramVec accept optional bucket boundaries; if omitted,
// the default Prometheus buckets are used.
type MetricsScope interface {
	Counter(name, help string, labels Labels) (Int64Counter, error)
	CounterVec(name, help string) (Int64CounterVec, error)
	Gauge(name, help string, labels Labels) (Int64Gauge, error)
	GaugeVec(name, help string) (Int64GaugeVec, error)
	Histogram(name, help string, labels Labels, buckets ...float64) (Float64Histogram, error)
	HistogramVec(name, help string, buckets ...float64) (Float64HistogramVec, error)
	// ObservableFloat64Gauge registers a gauge whose value is computed by fn on
	// every metrics collection cycle (Prometheus scrape, OTLP push, etc.).
	// Use it for values that change independently of metric update calls —
	// for example, the age of the oldest entry in an in-memory map.
	ObservableFloat64Gauge(name, help string, fn func() float64) error
}

type Metrics interface {
	Scope(prefix string, labels Labels) MetricsScope
}

type recordingDisabledKey struct{}
type recordingPolicyKey struct{}

// WithoutRecording marks Workflow graph execution so application metrics are
// not duplicated while Temporal replays history.
func WithoutRecording(ctx context.Context) context.Context {
	return context.WithValue(ctx, recordingDisabledKey{}, true)
}

// WithRecordingPolicy installs a replay-aware decision evaluated at the
// observation point.
func WithRecordingPolicy(ctx context.Context, policy func() bool) context.Context {
	return context.WithValue(ctx, recordingPolicyKey{}, policy)
}

func RecordingEnabled(ctx context.Context) bool {
	if policy, ok := ctx.Value(recordingPolicyKey{}).(func() bool); ok && policy != nil {
		return policy()
	}
	disabled, _ := ctx.Value(recordingDisabledKey{}).(bool)
	return !disabled
}
