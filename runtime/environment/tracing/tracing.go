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

// EnableSampling returns a new context that instructs the tracing engine to
// record the current request. Used in HTTP/gRPC middleware when a caller
// explicitly requests tracing (e.g. header "X-Trace: 1").
func EnableSampling(ctx context.Context) context.Context {
    return context.WithValue(ctx, samplingKey{}, true)
}

// SamplingEnabled reports whether the context was marked for tracing.
func SamplingEnabled(ctx context.Context) bool {
    v, _ := ctx.Value(samplingKey{}).(bool)
    return v
}

type TracingEngine interface {
    Tracing() Tracing
    GRPCStatsHandler() stats.Handler
    GRPCClientHandler() stats.Handler
    HTTPClientTransport(base http.RoundTripper) http.RoundTripper
    HTTPServerHandler(mux http.Handler, name string) http.Handler
    Shutdown(ctx context.Context) error
}
