/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package tracing

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/stats"
)

type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

// Attribute is a key-value pair used to annotate spans.
type Attribute struct {
	Key   string
	Value interface{}
}

func StringAttr(key, value string) Attribute {
	return Attribute{Key: key, Value: value}
}

func Int64Attr(key string, value int64) Attribute {
	return Attribute{Key: key, Value: value}
}

func Float64Attr(key string, value float64) Attribute {
	return Attribute{Key: key, Value: value}
}

func BoolAttr(key string, value bool) Attribute {
	return Attribute{Key: key, Value: value}
}

type SpanContext struct {
	TraceID string
	SpanID  string
	IsValid bool
}

type Span interface {
	End()
	SetAttributes(attrs ...Attribute)
	RecordError(err error)
	SetStatus(code StatusCode, description string)
	AddEvent(name string, attrs ...Attribute)
	SpanContext() SpanContext
}

type Tracer interface {
	Start(ctx context.Context, spanName string, attrs ...Attribute) (context.Context, Span)
}

type Tracing interface {
	Tracer(name string) Tracer
	Inject(context.Context, map[string]string)
	Extract(context.Context, map[string]string) context.Context
}

type noopSpan struct{}

func (noopSpan) End()                              {}
func (noopSpan) SetAttributes(_ ...Attribute)      {}
func (noopSpan) RecordError(_ error)               {}
func (noopSpan) SetStatus(_ StatusCode, _ string)  {}
func (noopSpan) AddEvent(_ string, _ ...Attribute) {}
func (noopSpan) SpanContext() SpanContext          { return SpanContext{} }

// StartSpan starts a new span. Safe to call unconditionally; returns a no-op span
// when tracer is nil or sampling is not requested for this context.
func StartSpan(ctx context.Context, tracer Tracer, operation string, attrs ...Attribute) (context.Context, Span) {
	if tracer == nil || !SamplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return tracer.Start(ctx, operation, attrs...)
}

// SpanEvent adds an event to span if it is non-nil.
func SpanEvent(span Span, name string, attrs ...Attribute) {
	if span != nil {
		span.AddEvent(name, attrs...)
	}
}

// SpanError records err on span and sets its status to Error if span is non-nil.
func SpanError(span Span, err error) {
	if span != nil {
		span.RecordError(err)
		span.SetStatus(StatusError, err.Error())
	}
}

// SpanAttrs sets attributes on span if it is non-nil.
func SpanAttrs(span Span, attrs ...Attribute) {
	if span != nil {
		span.SetAttributes(attrs...)
	}
}

// samplingKey is the context key used to enable per-request tracing.
type samplingKey struct{}
type recordingDisabledKey struct{}
type recordingPolicyKey struct{}

// EnableSampling returns a new context that instructs the tracing engine to
// record the current request. Used in HTTP/gRPC middleware when a caller
// explicitly requests tracing with a non-empty X-Trace marker or continues a
// sampled remote parent.
func EnableSampling(ctx context.Context) context.Context {
	return context.WithValue(ctx, samplingKey{}, true)
}

// SamplingEnabled reports whether the context was marked for tracing.
func SamplingEnabled(ctx context.Context) bool {
	if policy, ok := ctx.Value(recordingPolicyKey{}).(func() bool); ok && policy != nil && !policy() {
		return false
	}
	if disabled, _ := ctx.Value(recordingDisabledKey{}).(bool); disabled {
		return false
	}
	v, _ := ctx.Value(samplingKey{}).(bool)
	return v
}

// WithRecordingPolicy installs a replay-aware tracing decision.
func WithRecordingPolicy(ctx context.Context, policy func() bool) context.Context {
	return context.WithValue(ctx, recordingPolicyKey{}, policy)
}

// WithoutRecording suppresses ordinary graph spans during Workflow replay.
// Temporal SDK interceptors own replay-aware Workflow tracing.
func WithoutRecording(ctx context.Context) context.Context {
	return context.WithValue(ctx, recordingDisabledKey{}, true)
}

// SamplingRequestedByCarrier reports whether a transport carrier explicitly
// requests ServiceLib tracing or contains a valid sampled W3C traceparent.
// Transport adapters use this before starting their input span; extracting an
// OpenTelemetry parent alone does not set ServiceLib's opt-in context marker.
func SamplingRequestedByCarrier(carrier map[string]string) bool {
	if carrier["x-trace"] != "" {
		return true
	}
	parts := strings.Split(carrier["traceparent"], "-")
	if len(parts) != 4 || len(parts[3]) != 2 {
		return false
	}
	flags, err := strconv.ParseUint(parts[3], 16, 8)
	return err == nil && flags&1 == 1
}

type TracingEngine interface {
	Tracing() Tracing
	GRPCStatsHandler() stats.Handler
	GRPCClientHandler() stats.Handler
	HTTPClientTransport(base http.RoundTripper) http.RoundTripper
	HTTPServerHandler(mux http.Handler, name string) http.Handler
	Shutdown(ctx context.Context) error
}
