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

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/resource"
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

// ── lightweight propagation handlers ─────────────────────────────────────────

// grpcMetadataCarrier adapts gRPC metadata to the otel TextMapCarrier interface.
type grpcMetadataCarrier metadata.MD

func (c grpcMetadataCarrier) Get(key string) string {
	vals := metadata.MD(c).Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (c grpcMetadataCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

func (c grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// propagationServerStats extracts trace context from incoming gRPC metadata.
// It does not create spans — zero overhead for non-traced requests.
type propagationServerStats struct {
	prop propagation.TextMapPropagator
}

func (h *propagationServerStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = h.prop.Extract(ctx, grpcMetadataCarrier(md))
		if vals := md.Get("x-trace"); len(vals) > 0 && vals[0] != "" {
			ctx = tracing.EnableSampling(ctx)
		}
	}
	return ctx
}

func (h *propagationServerStats) HandleRPC(_ context.Context, _ stats.RPCStats) {}
func (h *propagationServerStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *propagationServerStats) HandleConn(_ context.Context, _ stats.ConnStats) {}

// propagationClientStats injects trace context into outgoing gRPC metadata.
type propagationClientStats struct {
	prop propagation.TextMapPropagator
}

func (h *propagationClientStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	h.prop.Inject(ctx, grpcMetadataCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

func (h *propagationClientStats) HandleRPC(_ context.Context, _ stats.RPCStats) {}
func (h *propagationClientStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *propagationClientStats) HandleConn(_ context.Context, _ stats.ConnStats) {}

// propagationTransport injects trace context into outgoing HTTP request headers.
type propagationTransport struct {
	base http.RoundTripper
	prop propagation.TextMapPropagator
}

func (t *propagationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	t.prop.Inject(req.Context(), propagation.HeaderCarrier(req.Header))
	return t.base.RoundTrip(req)
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

// GRPCStatsHandler extracts incoming trace context from gRPC metadata.
// No span is created — zero overhead when not sampling.
func (e *TracingEngine) GRPCStatsHandler() stats.Handler {
	return &propagationServerStats{prop: otel.GetTextMapPropagator()}
}

// GRPCClientHandler injects outgoing trace context into gRPC metadata.
func (e *TracingEngine) GRPCClientHandler() stats.Handler {
	return &propagationClientStats{prop: otel.GetTextMapPropagator()}
}

// HTTPClientTransport injects outgoing trace context into HTTP headers.
func (e *TracingEngine) HTTPClientTransport(base http.RoundTripper) http.RoundTripper {
	return &propagationTransport{base: base, prop: otel.GetTextMapPropagator()}
}

// HTTPServerHandler extracts incoming trace context from HTTP headers.
// No span is created — zero overhead when not sampling.
func (e *TracingEngine) HTTPServerHandler(mux http.Handler, _ string) http.Handler {
	prop := otel.GetTextMapPropagator()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
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

// WithContextSampler enables per-request tracing via tracing.EnableSampling(ctx).
// Without this option all spans are recorded (AlwaysSample).
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
