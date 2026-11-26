package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
)

type correlationEndpoint struct {
	runtime.InputEndpoint
	mu      sync.Mutex
	pending map[string]bool
}

func (*correlationEndpoint) GetID() int                                           { return 1 }
func (*correlationEndpoint) GetName() string                                      { return "concurrent-activity" }
func (*correlationEndpoint) OnRequestStart(context.Context) time.Time             { return time.Time{} }
func (*correlationEndpoint) OnRequestEnd(context.Context, time.Time, error)       {}
func (*correlationEndpoint) OnLateResult(context.Context, string)                 {}
func (*correlationEndpoint) OnDuplicateMessageID(context.Context, string, string) {}
func (e *correlationEndpoint) OnPendingAdd(_ context.Context, id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pending[id] {
		panic("pending metric identity reused")
	}
	e.pending[id] = true
}
func (e *correlationEndpoint) OnPendingRemove(_ context.Context, id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pending, id)
}

type correlationEnvironment struct{ runtime.RuntimeEnvironment }

func (correlationEnvironment) RuntimeConfig() *config.RuntimeConfig { return nil }

type correlationStream struct {
	runtime.TypedInputStream[string, string, string]
	env runtime.RuntimeEnvironment
}

func (s correlationStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
	if s.env != nil {
		return s.env
	}
	return correlationEnvironment{}
}

type correlationSerde struct{ serde.StreamSerde[string] }

func (correlationSerde) Serialize(value string) ([]byte, error) { return []byte(value), nil }

type correlationResultStream struct {
	runtime.TypedSerializedStream[string]
}

func (correlationResultStream) GetSerde() serde.StreamSerde[string] { return correlationSerde{} }

type correlationCall struct {
	ctx   context.Context
	value string
}
type correlationHandler struct {
	calls  chan correlationCall
	invoke func(context.Context, string) error
}

func (*correlationHandler) BeginRequest(ctx context.Context, _ StreamContext[string, string, string]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}
func (h *correlationHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, string], _ struct{}, value string) error {
	if h.invoke != nil {
		return h.invoke(ctx, value)
	}
	h.calls <- correlationCall{ctx, value}
	return nil
}

func TestConcurrentWorkflowEndpointCallsIsolateResults(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx workflow.Context) ([]string, error) {
		g, err := newWorkflowSubStreamGraph(ctx, func(*WorkflowEnvironment, context.Context, int, runtime.Collect[int]) {})
		if err != nil {
			return nil, err
		}
		consumer, endpoint, handler := newCorrelationConsumer()
		consumer.DataSourceEndpointConsumer = runtime.MakeDataSourceEndpointConsumer[string, string, string](endpoint, correlationStream{env: g.env})
		consumer.workflowPending = make(map[*endpointInvocation]workflow.Channel)
		var calls []correlationCall
		handler.invoke = func(ctx context.Context, value string) error {
			calls = append(calls, correlationCall{ctx, value})
			return nil
		}
		outputs := make([]string, 2)
		var failures []error
		completed := 0
		for index := range 2 {
			workflow.Go(ctx, func(child workflow.Context) {
				parent := context.WithValue(g.ctx, workflowSubmissionContextKey{}, workflowSubmissionContext{workflowCtx: child})
				r, err := consumer.handle(parent, EndpointEnvelope{StreamID: "shared-parent", Payload: []byte(string(rune('a' + index)))})
				if err != nil {
					failures = append(failures, err)
				}
				outputs[index] = string(r.Payload)
				completed++
			})
		}
		if err := workflow.Await(ctx, func() bool { return len(calls) == 2 || completed > 0 }); err != nil {
			return nil, err
		}
		if len(failures) > 0 {
			return nil, failures[0]
		}
		for index := len(calls) - 1; index >= 0; index-- {
			call := calls[index]
			consumer.consumeResult(call.ctx, call.value+"-result")
			consumer.consumeResult(call.ctx, "duplicate")
		}
		if err := workflow.Await(ctx, func() bool { return completed == 2 }); err != nil {
			return nil, err
		}
		if len(failures) > 0 {
			return nil, failures[0]
		}
		if len(consumer.workflowPending) != 0 || len(endpoint.pending) != 0 {
			return nil, errors.New("pending invocation leaked")
		}
		return outputs, nil
	})
	require.NoError(t, env.GetWorkflowError())
	var outputs []string
	require.NoError(t, env.GetWorkflowResult(&outputs))
	require.Equal(t, []string{"a-result", "b-result"}, outputs)
}
func (*correlationHandler) EndRequest(context.Context, StreamContext[string, string, string], error, struct{}) {
}

func newCorrelationConsumer() (*endpointConsumer[struct{}, string, string, string, string], *correlationEndpoint, *correlationHandler) {
	endpoint := &correlationEndpoint{pending: make(map[string]bool)}
	handler := &correlationHandler{calls: make(chan correlationCall, 2)}
	consumer := &endpointConsumer[struct{}, string, string, string, string]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[string, string, string](endpoint, correlationStream{}),
		handler:                    handler,
		decode:                     func(e EndpointEnvelope) (string, error) { return string(e.Payload), nil },
		resultSer:                  correlationResultStream{},
		pending:                    make(map[*endpointInvocation]chan string),
	}
	return consumer, endpoint, handler
}

func TestConcurrentActivityCallsPreserveParentAndIsolateResults(t *testing.T) {
	consumer, endpoint, handler := newCorrelationConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type outcome struct {
		result EndpointResult
		err    error
	}
	first, second := make(chan outcome, 1), make(chan outcome, 1)
	start := func(value string, out chan outcome) {
		r, err := consumer.handle(ctx, EndpointEnvelope{StreamID: "shared-parent", MessageID: "shared-parent", Payload: []byte(value)})
		out <- outcome{r, err}
	}
	go start("first", first)
	one := <-handler.calls
	go start("second", second)
	var two correlationCall
	select {
	case two = <-handler.calls:
	case failed := <-second:
		t.Fatalf("second concurrent invocation rejected: %v", failed.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, call := range []correlationCall{one, two} {
		id, ok := runtime.StreamIdFromContext(call.ctx)
		require.True(t, ok)
		require.Equal(t, "shared-parent", id.GetID())
	}
	// Deliver in reverse order; duplicate results must not block or cross calls.
	consumer.consumeResult(two.ctx, "second-result")
	consumer.consumeResult(two.ctx, "duplicate")
	consumer.consumeResult(one.ctx, "first-result")
	a, b := <-first, <-second
	require.NoError(t, a.err)
	require.NoError(t, b.err)
	require.Equal(t, "first-result", string(a.result.Payload))
	require.Equal(t, "second-result", string(b.result.Payload))
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	require.Empty(t, endpoint.pending)
}

func TestCancelledActivityLateResultCannotCompleteNextInvocation(t *testing.T) {
	consumer, endpoint, handler := newCorrelationConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstCtx, firstCancel := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() {
		_, err := consumer.handle(firstCtx, EndpointEnvelope{StreamID: "shared-parent", Payload: []byte("cancelled")})
		first <- err
	}()
	old := <-handler.calls
	firstCancel()
	require.ErrorIs(t, <-first, context.Canceled)
	type outcome struct {
		result EndpointResult
		err    error
	}
	second := make(chan outcome, 1)
	go func() {
		r, err := consumer.handle(ctx, EndpointEnvelope{StreamID: "shared-parent", Payload: []byte("retry")})
		second <- outcome{r, err}
	}()
	current := <-handler.calls
	consumer.consumeResult(old.ctx, "stale")
	consumer.consumeResult(current.ctx, "current")
	got := <-second
	require.False(t, errors.Is(got.err, context.Canceled))
	require.NoError(t, got.err)
	require.Equal(t, "current", string(got.result.Payload))
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	require.Empty(t, endpoint.pending)
}
