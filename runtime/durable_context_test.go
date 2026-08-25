package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/testtracing"
)

func TestDurableCallContextExplicitSuccess(t *testing.T) {
	durable := NewDurableCallContext("parent", nil, nil)
	result := make(chan error, 1)
	activityContext := make(chan context.Context, 1)
	go func() {
		result <- RunDurableCallActivity(context.Background(), durable, func(ctx context.Context) error {
			activityContext <- ctx
			return nil
		})
	}()

	ctx := <-activityContext
	select {
	case err := <-result:
		t.Fatalf("Activity completed without an explicit outcome: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	require.NoError(t, DurableCallSuccess(ctx))
	require.NoError(t, <-result)
}

func TestDurableCallContextExplicitErrorAndFirstTerminalWins(t *testing.T) {
	want := errors.New("business failure")
	durable := NewDurableCallContext("parent", nil, nil)
	ctx := WithDurableCallContext(context.Background(), durable)

	require.NoError(t, DurableCallError(ctx, want))
	require.ErrorIs(t, DurableCallSuccess(ctx), ErrDurableCallAlreadyCompleted)
	require.ErrorIs(t, durable.outcome, want)
}

func TestDurableCallHeartbeatOnlyWhileOpen(t *testing.T) {
	var mu sync.Mutex
	var messages []any
	durable := NewDurableCallContext("parent", func(_ context.Context, message any) error {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
		return nil
	}, nil)
	ctx := WithDurableCallContext(context.Background(), durable)

	require.NoError(t, DurableCallHeartbeat(ctx, "half-way"))
	require.NoError(t, DurableCallSuccess(ctx))
	require.ErrorIs(t, DurableCallHeartbeat(ctx, "too-late"), ErrDurableCallHeartbeatAfterCompletion)
	mu.Lock()
	require.Equal(t, []any{"half-way"}, messages)
	mu.Unlock()
}

func TestDurableCallCancellationSuppliesMissingOutcome(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	durable := NewDurableCallContext("parent", nil, nil)
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		result <- RunDurableCallActivity(ctx, durable, func(context.Context) error {
			close(started)
			return nil
		})
	}()
	<-started
	cause := errors.New("request deadline")
	cancel(cause)

	err := <-result
	require.ErrorIs(t, err, ErrDurableCallOutcomeMissing)
	require.ErrorIs(t, err, cause)
}

func TestDurableCallWithoutDeadlineRemainsPending(t *testing.T) {
	durable := NewDurableCallContext("parent", nil, nil)
	result := make(chan error, 1)
	activityContext := make(chan context.Context, 1)
	go func() {
		result <- RunDurableCallActivity(context.Background(), durable, func(ctx context.Context) error {
			activityContext <- ctx
			return nil
		})
	}()
	ctx := <-activityContext

	select {
	case err := <-result:
		t.Fatalf("Activity without a deadline unexpectedly completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, DurableCallSuccess(ctx))
	require.NoError(t, <-result)
}

func TestDurableCallOperationsOutsideActivityAreObservable(t *testing.T) {
	ctx := context.Background()
	require.ErrorIs(t, DurableCallHeartbeat(ctx, "progress"), ErrNoDurableCallContext)
	require.ErrorIs(t, DurableCallSuccess(ctx), ErrNoDurableCallContext)
	require.ErrorIs(t, DurableCallError(ctx, errors.New("failure")), ErrNoDurableCallContext)
}

func TestDurableCallLifecycleIsRecordedOnActivitySpan(t *testing.T) {
	engine := testtracing.New()
	durable := NewDurableCallContext("parent", nil, nil)
	result := make(chan error, 1)
	go func() {
		result <- RunDurableCallActivity(tracing.EnableSampling(context.Background()), durable, func(ctx context.Context) error {
			_, span := engine.Tracer("service").Start(ctx, "temporal.activity")
			require.True(t, BindDurableCallSpan(ctx, span))
			require.NoError(t, DurableCallHeartbeat(ctx, "half-way"))
			return DurableCallSuccess(ctx)
		})
	}()
	require.NoError(t, <-result)

	spans := engine.Spans()
	require.Len(t, spans, 1)
	require.Equal(t, "temporal.activity", spans[0].Name)
	require.Equal(t, tracing.StatusOK, spans[0].StatusCode)
	require.Equal(t, []string{"durable_call.heartbeat", "durable_call.success"}, []string{
		spans[0].Events[0].Name,
		spans[0].Events[1].Name,
	})
}
