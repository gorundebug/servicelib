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
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorundebug/servicelib/runtime/contextvalue"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// ── attribute helpers ─────────────────────────────────────────────────────────

func attrFromTracing(a tracing.Attribute) attribute.KeyValue {
	switch v := a.Value.(type) {
	case string:
		return attribute.String(a.Key, v)
	case int64:
		return attribute.Int64(a.Key, v)
	case int:
		return attribute.Int(a.Key, v)
	case float64:
		return attribute.Float64(a.Key, v)
	case bool:
		return attribute.Bool(a.Key, v)
	default:
		return attribute.String(a.Key, fmt.Sprintf("%v", v))
	}
}

func attrsFromTracing(attrs []tracing.Attribute) []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		kvs[i] = attrFromTracing(a)
	}
	return kvs
}

// ── Span ──────────────────────────────────────────────────────────────────────

type span struct {
	s oteltrace.Span
}

func (s *span) End() {
	s.s.End()
}

func (s *span) SetAttributes(attrs ...tracing.Attribute) {
	s.s.SetAttributes(attrsFromTracing(attrs)...)
}

func (s *span) RecordError(err error) {
	s.s.RecordError(err)
}

func (s *span) SetStatus(code tracing.StatusCode, description string) {
	var c codes.Code
	switch code {
	case tracing.StatusOK:
		c = codes.Ok
	case tracing.StatusError:
		c = codes.Error
	default:
		c = codes.Unset
	}
	s.s.SetStatus(c, description)
}

func (s *span) AddEvent(name string, attrs ...tracing.Attribute) {
	s.s.AddEvent(name, oteltrace.WithAttributes(attrsFromTracing(attrs)...))
}

func (s *span) SpanContext() tracing.SpanContext {
	sc := s.s.SpanContext()
	return tracing.SpanContext{
		TraceID: sc.TraceID().String(),
		SpanID:  sc.SpanID().String(),
		IsValid: sc.IsValid(),
	}
}

// ── Tracer ────────────────────────────────────────────────────────────────────

type tracerImpl struct {
	t oteltrace.Tracer
}

func (t *tracerImpl) Start(ctx context.Context, spanName string, attrs ...tracing.Attribute) (context.Context, tracing.Span) {
	ctx, s := t.t.Start(ctx, spanName, oteltrace.WithAttributes(attrsFromTracing(attrs)...))
	return ctx, &span{s: s}
}

// ── Tracing factory ───────────────────────────────────────────────────────────

type tracingImpl struct {
	provider oteltrace.TracerProvider
}

func (tr *tracingImpl) Tracer(name string) tracing.Tracer {
	return &tracerImpl{t: tr.provider.Tracer(name)}
}

func (tr *tracingImpl) Inject(ctx context.Context, carrier map[string]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))
}

func (tr *tracingImpl) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// ── samplingGRPCServerHandler ─────────────────────────────────────────────────

// samplingGRPCServerHandler enables per-request tracing when the caller sends
// "x-trace" metadata, then delegates to the inner otelgrpc server handler that
// creates the actual gRPC server span with full semconv attributes.
type samplingGRPCServerHandler struct {
	inner stats.Handler
}

type grpcServerTracingActiveKey struct{}

func (h *samplingGRPCServerHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	requested := tracing.SamplingEnabled(ctx) || oteltrace.SpanContextFromContext(ctx).IsSampled()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if !contextvalue.StreamIDInspected(ctx) {
			if values := md["x-stream-id"]; len(values) > 0 && values[0] != "" {
				ctx = contextvalue.WithStreamID(ctx, values[0])
			} else {
				ctx = contextvalue.WithStreamIDInspected(ctx)
			}
		}
		if vals := md["x-trace"]; len(vals) > 0 && vals[0] != "" {
			ctx = tracing.EnableSampling(ctx)
			requested = true
		} else if len(md["traceparent"]) > 0 {
			// Let otelgrpc extract even an unsampled remote parent. Requests
			// without tracing metadata never enter otelgrpc at all.
			requested = true
		}
	} else if !contextvalue.StreamIDInspected(ctx) {
		ctx = contextvalue.WithStreamIDInspected(ctx)
	}
	if !requested {
		return ctx
	}

	ctx = h.inner.TagRPC(ctx, info)
	// A sampled remote traceparent is itself sufficient to continue
	// application spans. Requiring the ServiceLib-specific x-trace marker on
	// every hop would break standard W3C propagation.
	if oteltrace.SpanContextFromContext(ctx).IsSampled() {
		ctx = tracing.EnableSampling(ctx)
	}
	return context.WithValue(ctx, grpcServerTracingActiveKey{}, struct{}{})
}

func (h *samplingGRPCServerHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if _, ok := ctx.Value(grpcServerTracingActiveKey{}).(struct{}); ok {
		h.inner.HandleRPC(ctx, s)
	}
}

func (h *samplingGRPCServerHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *samplingGRPCServerHandler) HandleConn(context.Context, stats.ConnStats) {
}

// samplingGRPCClientHandler keeps otelgrpc entirely outside the normal client
// path. It is activated only for a locally sampled context or an explicit
// outgoing x-trace marker.
type samplingGRPCClientHandler struct {
	inner stats.Handler
}

type grpcClientTracingActiveKey struct{}

func (h *samplingGRPCClientHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	requested := tracing.SamplingEnabled(ctx) || oteltrace.SpanContextFromContext(ctx).IsSampled()
	if !requested {
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			requested = firstMetadataValuePresent(md, "x-trace") || len(md.Get("traceparent")) > 0
		}
	}
	if !requested {
		return ctx
	}
	ctx = h.inner.TagRPC(ctx, info)
	return context.WithValue(ctx, grpcClientTracingActiveKey{}, struct{}{})
}

func (h *samplingGRPCClientHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if _, ok := ctx.Value(grpcClientTracingActiveKey{}).(struct{}); ok {
		h.inner.HandleRPC(ctx, s)
	}
}

func (*samplingGRPCClientHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*samplingGRPCClientHandler) HandleConn(context.Context, stats.ConnStats) {
}

func firstMetadataValuePresent(md metadata.MD, key string) bool {
	values := md.Get(key)
	return len(values) > 0 && values[0] != ""
}

type samplingHTTPTransport struct {
	base   http.RoundTripper
	traced http.RoundTripper
}

func (t *samplingHTTPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if httpTracingRequested(r) {
		return t.traced.RoundTrip(r)
	}
	return t.base.RoundTrip(r)
}

type samplingHTTPServerHandler struct {
	base   http.Handler
	traced http.Handler
}

func (h *samplingHTTPServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !httpTracingRequested(r) {
		h.base.ServeHTTP(w, r)
		return
	}
	if (r.Header.Get("x-trace") != "" || sampledTraceparent(r.Header.Get("traceparent"))) &&
		!tracing.SamplingEnabled(r.Context()) {
		r = r.WithContext(tracing.EnableSampling(r.Context()))
	}
	h.traced.ServeHTTP(w, r)
}

func httpTracingRequested(r *http.Request) bool {
	ctx := r.Context()
	return tracing.SamplingEnabled(ctx) ||
		oteltrace.SpanContextFromContext(ctx).IsSampled() ||
		r.Header.Get("x-trace") != "" ||
		r.Header.Get("traceparent") != ""
}

func sampledTraceparent(value string) bool {
	separator := strings.LastIndexByte(value, '-')
	if separator < 0 || len(value) < separator+3 {
		return false
	}
	flags, err := strconv.ParseUint(value[separator+1:separator+3], 16, 8)
	return err == nil && flags&1 != 0
}

// ── TracingEngine ─────────────────────────────────────────────────────────────

type TracingEngine struct {
	environment environment.ServiceEnvironment
	impl        *tracingImpl
	provider    *sdktrace.TracerProvider
}

func (e *TracingEngine) Tracing() tracing.Tracing {
	return e.impl
}

// GRPCStatsHandler enables per-request sampling via "x-trace" metadata and
// creates gRPC server spans with standard semconv attributes (method, status, etc.).
func (e *TracingEngine) GRPCStatsHandler() stats.Handler {
	return &samplingGRPCServerHandler{
		inner: otelgrpc.NewServerHandler(
			otelgrpc.WithTracerProvider(e.provider),
			otelgrpc.WithMeterProvider(noopmetric.NewMeterProvider()),
			otelgrpc.WithPropagators(otel.GetTextMapPropagator()),
		),
	}
}

// GRPCClientHandler creates gRPC client spans and injects outgoing trace context.
func (e *TracingEngine) GRPCClientHandler() stats.Handler {
	return &samplingGRPCClientHandler{
		inner: otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(e.provider),
			otelgrpc.WithMeterProvider(noopmetric.NewMeterProvider()),
			otelgrpc.WithPropagators(otel.GetTextMapPropagator()),
		),
	}
}

// HTTPClientTransport creates HTTP client spans (including DNS, connect, TLS timing)
// and injects outgoing trace context into request headers.
func (e *TracingEngine) HTTPClientTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &samplingHTTPTransport{
		base: base,
		traced: otelhttp.NewTransport(base,
			otelhttp.WithTracerProvider(e.provider),
			otelhttp.WithMeterProvider(noopmetric.NewMeterProvider()),
			otelhttp.WithPropagators(otel.GetTextMapPropagator()),
		),
	}
}

// HTTPServerHandler creates a transport-level HTTP server span that covers
// routing, auth middleware, panic recovery, and request decoding — everything
// before the endpoint handler runs. Per-request sampling is enabled when the
// caller sends "X-Trace: 1"; the flag is set before otelhttp creates the span
// so contextSampler sees it.
func (e *TracingEngine) HTTPServerHandler(mux http.Handler, name string) http.Handler {
	inner := otelhttp.NewHandler(mux, name,
		otelhttp.WithTracerProvider(e.provider),
		otelhttp.WithMeterProvider(noopmetric.NewMeterProvider()),
		otelhttp.WithPropagators(otel.GetTextMapPropagator()),
	)
	return &samplingHTTPServerHandler{base: mux, traced: inner}
}

func (e *TracingEngine) Shutdown(ctx context.Context) error {
	return e.provider.Shutdown(ctx)
}

// ── contextSampler ────────────────────────────────────────────────────────────

// contextSampler records a span only when tracing.EnableSampling was called on
// the request context. Downstream services receiving a sampled traceparent are
// covered by ParentBased wrapping — no extra setup needed.
type contextSampler struct{}

func (contextSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if tracing.SamplingEnabled(p.ParentContext) {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}

func (contextSampler) Description() string { return "ContextSampler" }

// ── TracingOption ─────────────────────────────────────────────────────────────

// TracingOption configures how the TracerProvider is built.
type TracingOption func(*[]sdktrace.TracerProviderOption)

// WithContextSampler explicitly selects the default ServiceLib opt-in
// sampling contract. It remains useful in uniform option lists.
func WithContextSampler() TracingOption {
	return func(opts *[]sdktrace.TracerProviderOption) {
		*opts = append(*opts, sdktrace.WithSampler(
			sdktrace.ParentBased(contextSampler{}),
		))
	}
}

// ── factory ───────────────────────────────────────────────────────────────────

func newTracingEngine(env environment.ServiceEnvironment, provider *sdktrace.TracerProvider) *TracingEngine {
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return &TracingEngine{
		environment: env,
		impl:        &tracingImpl{provider: provider},
		provider:    provider,
	}
}

func serviceResource(env environment.ServiceEnvironment) *resource.Resource {
	return resource.NewSchemaless(attribute.String("service.name", env.ServiceConfig().Name))
}

func buildProvider(exp sdktrace.SpanExporter, env environment.ServiceEnvironment, opts []TracingOption) *sdktrace.TracerProvider {
	provOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(serviceResource(env)),
		sdktrace.WithSampler(sdktrace.ParentBased(contextSampler{})),
	}
	for _, o := range opts {
		o(&provOpts)
	}
	return sdktrace.NewTracerProvider(provOpts...)
}

func CreateStdoutTracingEngine(env environment.ServiceEnvironment, opts ...TracingOption) (tracing.TracingEngine, error) {
	exp, err := stdouttrace.New()
	if err != nil {
		return nil, err
	}
	return newTracingEngine(env, buildProvider(exp, env, opts)), nil
}

// CreatePrettyTracingEngine creates a TracingEngine that prints spans to stdout
// in a concise human-readable format — intended for local development and debugging.
func CreatePrettyTracingEngine(env environment.ServiceEnvironment, opts ...TracingOption) (tracing.TracingEngine, error) {
	exp := newPrettySpanExporter(os.Stdout)
	provOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithSyncer(exp),
		sdktrace.WithResource(serviceResource(env)),
		sdktrace.WithSampler(sdktrace.ParentBased(contextSampler{})),
	}
	for _, o := range opts {
		o(&provOpts)
	}
	return newTracingEngine(env, sdktrace.NewTracerProvider(provOpts...)), nil
}

func CreateOTLPTracingEngine(ctx context.Context, env environment.ServiceEnvironment, opts ...TracingOption) (tracing.TracingEngine, error) {
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	return newTracingEngine(env, buildProvider(exp, env, opts)), nil
}
