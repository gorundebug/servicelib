/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package opentelemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

type countingStatsHandler struct {
	tagRPC    int
	handleRPC int
	tagRPCFn  func(context.Context) context.Context
}

func (h *countingStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	h.tagRPC++
	if h.tagRPCFn != nil {
		return h.tagRPCFn(ctx)
	}
	return ctx
}

func (h *countingStatsHandler) HandleRPC(context.Context, stats.RPCStats) {
	h.handleRPC++
}

func (*countingStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*countingStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func TestGRPCServerUnsampledBypassesTracing(t *testing.T) {
	inner := &countingStatsHandler{}
	handler := &samplingGRPCServerHandler{inner: inner}

	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/test.Service/Call"})
	handler.HandleRPC(ctx, nil)

	if inner.tagRPC != 0 || inner.handleRPC != 0 {
		t.Fatalf("unsampled RPC entered tracing handler: TagRPC=%d HandleRPC=%d", inner.tagRPC, inner.handleRPC)
	}
}

func TestGRPCServerXTraceActivatesTracingBeforeTagRPC(t *testing.T) {
	inner := &countingStatsHandler{
		tagRPCFn: func(ctx context.Context) context.Context {
			if !tracing.SamplingEnabled(ctx) {
				t.Fatal("sampling was not enabled before the tracing handler")
			}
			return ctx
		},
	}
	handler := &samplingGRPCServerHandler{inner: inner}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-trace", "1"))

	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/test.Service/Call"})
	handler.HandleRPC(ctx, nil)

	if inner.tagRPC != 1 || inner.handleRPC != 1 {
		t.Fatalf("traced RPC calls: TagRPC=%d HandleRPC=%d", inner.tagRPC, inner.handleRPC)
	}
}

func TestGRPCServerSampledTraceparentContinuesSampling(t *testing.T) {
	inner := &countingStatsHandler{
		tagRPCFn: func(ctx context.Context) context.Context {
			traceID, _ := oteltrace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
			spanID, _ := oteltrace.SpanIDFromHex("0102030405060708")
			spanContext := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: oteltrace.FlagsSampled,
				Remote:     true,
			})
			return oteltrace.ContextWithRemoteSpanContext(ctx, spanContext)
		},
	}
	handler := &samplingGRPCServerHandler{inner: inner}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
	))

	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/test.Service/Call"})

	if !tracing.SamplingEnabled(ctx) {
		t.Fatal("sampled remote traceparent did not enable application tracing")
	}
}

func TestGRPCClientUnsampledBypassesTracing(t *testing.T) {
	inner := &countingStatsHandler{}
	handler := &samplingGRPCClientHandler{inner: inner}

	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/test.Service/Call"})
	handler.HandleRPC(ctx, nil)

	if inner.tagRPC != 0 || inner.handleRPC != 0 {
		t.Fatalf("unsampled client RPC entered tracing handler: TagRPC=%d HandleRPC=%d", inner.tagRPC, inner.handleRPC)
	}
}

func TestGRPCClientSampledContextActivatesTracing(t *testing.T) {
	inner := &countingStatsHandler{}
	handler := &samplingGRPCClientHandler{inner: inner}
	ctx := tracing.EnableSampling(context.Background())

	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/test.Service/Call"})
	handler.HandleRPC(ctx, nil)

	if inner.tagRPC != 1 || inner.handleRPC != 1 {
		t.Fatalf("traced client RPC calls: TagRPC=%d HandleRPC=%d", inner.tagRPC, inner.handleRPC)
	}
}

func TestHTTPServerUnsampledBypassesTracing(t *testing.T) {
	baseCalls := 0
	tracedCalls := 0
	handler := &samplingHTTPServerHandler{
		base: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { baseCalls++ }),
		traced: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			tracedCalls++
		}),
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if baseCalls != 1 || tracedCalls != 0 {
		t.Fatalf("unexpected calls: base=%d traced=%d", baseCalls, tracedCalls)
	}
}

func TestHTTPServerXTraceActivatesTracing(t *testing.T) {
	tracedCalls := 0
	handler := &samplingHTTPServerHandler{
		base: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("X-Trace request bypassed tracing")
		}),
		traced: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			tracedCalls++
			if !tracing.SamplingEnabled(r.Context()) {
				t.Fatal("sampling was not enabled before the tracing handler")
			}
		}),
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Trace", "1")

	handler.ServeHTTP(httptest.NewRecorder(), req)

	if tracedCalls != 1 {
		t.Fatalf("traced calls=%d", tracedCalls)
	}
}

func TestHTTPServerSampledTraceparentActivatesApplicationTracing(t *testing.T) {
	handler := &samplingHTTPServerHandler{
		base: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("sampled traceparent bypassed tracing")
		}),
		traced: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			if !tracing.SamplingEnabled(r.Context()) {
				t.Fatal("sampled traceparent did not enable application tracing")
			}
		}),
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01")

	handler.ServeHTTP(httptest.NewRecorder(), req)
}

type countingRoundTripper struct {
	calls int
}

func (r *countingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls++
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
}

func TestHTTPClientUnsampledBypassesTracing(t *testing.T) {
	base := &countingRoundTripper{}
	traced := &countingRoundTripper{}
	transport := &samplingHTTPTransport{base: base, traced: traced}
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)

	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if base.calls != 1 || traced.calls != 0 {
		t.Fatalf("unexpected calls: base=%d traced=%d", base.calls, traced.calls)
	}
}

func TestHTTPClientSampledContextActivatesTracing(t *testing.T) {
	base := &countingRoundTripper{}
	traced := &countingRoundTripper{}
	transport := &samplingHTTPTransport{base: base, traced: traced}
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	req = req.WithContext(tracing.EnableSampling(req.Context()))

	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if base.calls != 0 || traced.calls != 1 {
		t.Fatalf("unexpected calls: base=%d traced=%d", base.calls, traced.calls)
	}
}
