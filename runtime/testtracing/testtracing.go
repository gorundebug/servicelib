/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// Package testtracing provides an in-memory tracing engine for use in automated
// tests. It implements tracing.TracingEngine with no-op spans by default.
// Recorded spans can be inspected via Spans() to assert tracing behaviour.
//
// Usage:
//
//	engine := testtracing.New()
//	// pass engine via ServiceDependencies.TracingEngine()
//
//	doWork(ctx)
//
//	spans := engine.Spans()
//	require.Equal(t, "process-message", spans[0].Name)
package testtracing

import (
	"context"
	"net/http"
	"sync"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"google.golang.org/grpc/stats"
)

// RecordedSpan holds the data captured when a span ends.
type RecordedSpan struct {
	Name       string
	Attributes []tracing.Attribute
	Events     []RecordedEvent
	StatusCode tracing.StatusCode
	StatusDesc string
	Err        error
}

// RecordedEvent holds the data captured by Span.AddEvent.
type RecordedEvent struct {
	Name       string
	Attributes []tracing.Attribute
}

// ── testSpan ──────────────────────────────────────────────────────────────────

type testSpan struct {
	engine *TestTracing
	name   string
	attrs  []tracing.Attribute
	events []RecordedEvent
	status tracing.StatusCode
	desc   string
	err    error
}

func (s *testSpan) End() {
	s.engine.record(RecordedSpan{
		Name:       s.name,
		Attributes: s.attrs,
		Events:     s.events,
		StatusCode: s.status,
		StatusDesc: s.desc,
		Err:        s.err,
	})
}

func (s *testSpan) SetAttributes(attrs ...tracing.Attribute) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *testSpan) RecordError(err error) {
	s.err = err
}

func (s *testSpan) SetStatus(code tracing.StatusCode, description string) {
	s.status = code
	s.desc = description
}

func (s *testSpan) AddEvent(name string, attrs ...tracing.Attribute) {
	s.events = append(s.events, RecordedEvent{Name: name, Attributes: attrs})
}

func (s *testSpan) SpanContext() tracing.SpanContext {
	return tracing.SpanContext{}
}

// ── testTracer ────────────────────────────────────────────────────────────────

type testTracer struct {
	engine *TestTracing
}

func (t *testTracer) Start(ctx context.Context, spanName string, attrs ...tracing.Attribute) (context.Context, tracing.Span) {
	return ctx, &testSpan{engine: t.engine, name: spanName, attrs: attrs}
}

// ── TestTracing ───────────────────────────────────────────────────────────────

// TestTracing implements tracing.TracingEngine and tracing.Tracing.
// All spans are recorded in memory and accessible via Spans().
type TestTracing struct {
	mu    sync.Mutex
	spans []RecordedSpan
}

func New() *TestTracing {
	return &TestTracing{}
}

func (e *TestTracing) record(s RecordedSpan) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, s)
}

// Spans returns a snapshot of all completed spans.
func (e *TestTracing) Spans() []RecordedSpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]RecordedSpan, len(e.spans))
	copy(out, e.spans)
	return out
}

// Reset clears all recorded spans.
func (e *TestTracing) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = e.spans[:0]
}

// ── tracing.TracingEngine ─────────────────────────────────────────────────────

func (e *TestTracing) Tracing() tracing.Tracing                                     { return e }
func (e *TestTracing) GRPCStatsHandler() stats.Handler                              { return testStats{} }
func (e *TestTracing) GRPCClientHandler() stats.Handler                             { return noopStats{} }
func (e *TestTracing) HTTPClientTransport(base http.RoundTripper) http.RoundTripper { return base }
func (e *TestTracing) HTTPServerHandler(next http.Handler, _ string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(tracing.EnableSampling(r.Context())))
	})
}
func (e *TestTracing) Shutdown(_ context.Context) error { return nil }

// ── tracing.Tracing ───────────────────────────────────────────────────────────

func (e *TestTracing) Tracer(_ string) tracing.Tracer {
	return &testTracer{engine: e}
}
func (e *TestTracing) Inject(_ context.Context, _ map[string]string)                    {}
func (e *TestTracing) Extract(ctx context.Context, _ map[string]string) context.Context { return ctx }

// ── noopStats ─────────────────────────────────────────────────────────────────

type noopStats struct{}

func (noopStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (noopStats) HandleRPC(_ context.Context, _ stats.RPCStats)                     {}
func (noopStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (noopStats) HandleConn(_ context.Context, _ stats.ConnStats)                   {}

// ── testStats ─────────────────────────────────────────────────────────────────

type testStats struct{}

func (testStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return tracing.EnableSampling(ctx)
}
func (testStats) HandleRPC(_ context.Context, _ stats.RPCStats)                     {}
func (testStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (testStats) HandleConn(_ context.Context, _ stats.ConnStats)                   {}
