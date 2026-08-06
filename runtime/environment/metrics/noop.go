package metrics

import (
	"context"
	"net/http"

	"google.golang.org/grpc/stats"
)

// NoopMetricsEngine provides the complete metrics integration surface while
// discarding every observation. Instrument and scope instances are shared and
// allocate no per-series storage.
type NoopMetricsEngine struct{}

func NewNoopMetricsEngine() MetricsEngine { return NoopMetricsEngine{} }

func (NoopMetricsEngine) HTTPMetricsHandler() http.Handler                             { return http.NotFoundHandler() }
func (NoopMetricsEngine) GRPCStatsHandler() stats.Handler                              { return noopStatsHandler{} }
func (NoopMetricsEngine) GRPCClientHandler() stats.Handler                             { return noopStatsHandler{} }
func (NoopMetricsEngine) HTTPClientTransport(base http.RoundTripper) http.RoundTripper { return base }
func (NoopMetricsEngine) HTTPServerHandler(next http.Handler, _ string) http.Handler   { return next }
func (NoopMetricsEngine) Metrics() Metrics                                             { return noopMetrics{} }
func (NoopMetricsEngine) Shutdown(context.Context) error                               { return nil }

type noopMetrics struct{}
type noopScope struct{}
type noopCounter struct{}
type noopGauge struct{}
type noopHistogram struct{}
type noopCounterVec struct{}
type noopGaugeVec struct{}
type noopHistogramVec struct{}

func (noopMetrics) Scope(string, Labels) MetricsScope                  { return noopScope{} }
func (noopScope) Counter(string, string, Labels) (Int64Counter, error) { return noopCounter{}, nil }
func (noopScope) CounterVec(string, string) (Int64CounterVec, error)   { return noopCounterVec{}, nil }
func (noopScope) Gauge(string, string, Labels) (Int64Gauge, error)     { return noopGauge{}, nil }
func (noopScope) GaugeVec(string, string) (Int64GaugeVec, error)       { return noopGaugeVec{}, nil }
func (noopScope) Histogram(string, string, Labels, ...float64) (Float64Histogram, error) {
	return noopHistogram{}, nil
}
func (noopScope) HistogramVec(string, string, ...float64) (Float64HistogramVec, error) {
	return noopHistogramVec{}, nil
}
func (noopScope) ObservableFloat64Gauge(string, string, func() float64) error { return nil }

func (noopCounter) Inc(context.Context)                {}
func (noopCounter) Add(context.Context, int64)         {}
func (noopGauge) Set(int64)                            {}
func (noopGauge) Inc()                                 {}
func (noopGauge) Dec()                                 {}
func (noopGauge) Add(int64)                            {}
func (noopGauge) Sub(int64)                            {}
func (noopHistogram) Observe(context.Context, float64) {}
func (noopCounterVec) With(Labels) Int64Counter        { return noopCounter{} }
func (noopGaugeVec) With(Labels) Int64Gauge            { return noopGauge{} }
func (noopGaugeVec) Delete(Labels)                     {}
func (noopHistogramVec) With(Labels) Float64Histogram  { return noopHistogram{} }

type noopStatsHandler struct{}

func (noopStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (noopStatsHandler) HandleRPC(context.Context, stats.RPCStats)                       {}
func (noopStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (noopStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
