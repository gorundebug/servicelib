package dataquery

import (
	"context"
	"errors"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"reflect"
	"testing"
	"time"
)

type testErrorStream struct {
	runtime.TypedConsumedStream[error]
	values []error
}

func (s *testErrorStream) Consume(_ context.Context, value error) { s.values = append(s.values, value) }

type testStream struct {
	runtime.TypedQueryStream[int, string, error]
	consumer runtime.Consumer[int]
	values   []string
	failures testErrorStream
}

func (s *testStream) SetQueryConsumer(c runtime.Consumer[int])           { s.consumer = c }
func (s *testStream) ConsumeResult(_ context.Context, v string)          { s.values = append(s.values, v) }
func (s *testStream) GetErrorStream() runtime.TypedConsumedStream[error] { return &s.failures }

type testHandler struct {
	beginError error
	ends       int
	endError   error
	complete   bool
	key        any
}

func (h *testHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, int, error) {
	return context.WithValue(ctx, h.key, "request"), 41, h.beginError
}
func (h *testHandler) EndRequest(ctx context.Context, _ runtime.Stream, state int, err error, out runtime.Collect[string], failures runtime.Collect[error]) {
	h.ends++
	h.endError = err
	if state != 41 || ctx.Value(h.key) != "request" {
		panic("request state/context lost")
	}
	if err != nil {
		failures.Out(ctx, err)
	}
	if h.complete {
		out.Out(ctx, "done")
	}
}

func TestQueryLifecycle(t *testing.T) {
	failure := errors.New("storage failed")
	for _, tc := range []struct {
		name                     string
		beginError, executeError error
		canceled, complete       bool
		results                  int
	}{
		{name: "empty"},
		{name: "many", results: 3},
		{name: "user completion", results: 2, complete: true},
		{name: "partial failure", results: 1, executeError: failure},
		{name: "begin failure", beginError: failure},
		{name: "canceled", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &testStream{}
			handler := &testHandler{beginError: tc.beginError, complete: tc.complete, key: &struct{ name string }{"request"}}
			calls := 0
			consumer, err := MakeEndpointConsumer[int, int, string, error](stream, handler, func(ctx context.Context, state, value int, out runtime.Collect[string]) error {
				calls++
				if state != 41 || value != 42 || ctx.Value(handler.key) != "request" {
					t.Fatal("request context/state/value lost")
				}
				for i := 0; i < tc.results; i++ {
					out.Out(ctx, "row")
				}
				return tc.executeError
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			consumer.Consume(ctx, 42)
			if handler.ends != 1 {
				t.Fatalf("EndRequest calls: %d", handler.ends)
			}
			wantCalls := 1
			if tc.canceled || tc.beginError != nil {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("execute calls: %d", calls)
			}
			expected := []string{}
			if wantCalls == 1 {
				for i := 0; i < tc.results; i++ {
					expected = append(expected, "row")
				}
			}
			if tc.complete {
				expected = append(expected, "done")
			}
			if len(stream.values) != len(expected) || (len(expected) > 0 && !reflect.DeepEqual(stream.values, expected)) {
				t.Fatalf("results: %v expected %v", stream.values, expected)
			}
			wantErr := tc.beginError
			if wantErr == nil {
				wantErr = tc.executeError
			}
			if tc.canceled {
				wantErr = context.Canceled
			}
			if !errors.Is(handler.endError, wantErr) {
				t.Fatalf("EndRequest error %v expected %v", handler.endError, wantErr)
			}
			if wantErr != nil && (len(stream.failures.values) != 1 || !errors.Is(stream.failures.values[0], wantErr)) {
				t.Fatalf("error output: %v", stream.failures.values)
			}
			if wantErr == nil && len(stream.failures.values) != 0 {
				t.Fatal("unexpected error output")
			}
		})
	}
}

func TestQueryOperationDeadline(t *testing.T) {
	for _, tc := range []struct {
		name              string
		parent, operation time.Duration
	}{
		{"operation", time.Second, 10 * time.Millisecond},
		{"parent", 10 * time.Millisecond, time.Second},
		{"inherited only", 10 * time.Millisecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), tc.parent)
			defer cancel()
			stream := &testStream{}
			handler := &testHandler{complete: true, key: &struct{ name string }{"timeout"}}
			cleaned := false
			consumer, err := MakeEndpointConsumer(stream, handler, func(ctx context.Context, state, value int, out runtime.Collect[string]) error {
				defer func() { cleaned = true }()
				if ctx.Value(handler.key) != "request" {
					t.Fatal("request values lost")
				}
				out.Out(ctx, "partial")
				<-ctx.Done()
				return nil // Deadline must not be reported as success even for this mapper.
			}, Options{OperationTimeout: tc.operation})
			if err != nil {
				t.Fatal(err)
			}
			consumer.Consume(ctx, 42)
			if !cleaned || handler.ends != 1 || !errors.Is(handler.endError, context.DeadlineExceeded) {
				t.Fatalf("invalid outcome: cleaned=%v ends=%d err=%v", cleaned, handler.ends, handler.endError)
			}
			if !reflect.DeepEqual(stream.values, []string{"partial", "done"}) {
				t.Fatal(stream.values)
			}
			if tc.name == "operation" && ctx.Err() != nil {
				t.Fatal("operation cancelled parent context")
			}
		})
	}
}

func TestQueryRejectsNegativeOperationTimeout(t *testing.T) {
	_, err := MakeEndpointConsumer(&testStream{}, &testHandler{}, func(context.Context, int, int, runtime.Collect[string]) error { return nil }, Options{OperationTimeout: -time.Second})
	if err == nil {
		t.Fatal("negative timeout accepted")
	}
}

type noopQueryEnvironment struct{ runtime.RuntimeEnvironment }

func (noopQueryEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*testStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return noopQueryEnvironment{} }
