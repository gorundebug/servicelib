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

func TestDurableActivityAttachesContextAndClosesAtEndpointBoundary(t *testing.T) {
	var mu sync.Mutex
	var messages []any
	durable := NewDurableCallContext("execution", func(_ context.Context, message any) error {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
		return nil
	}, nil)
	var activityContext context.Context

	require.NoError(t, RunDurableActivity(context.Background(), durable, func(ctx context.Context) error {
		activityContext = ctx
		_, ok := DurableCallContextFromContext(ctx)
		require.True(t, ok)
		return DurableCallHeartbeat(ctx, "half-way")
	}))

	require.ErrorIs(t, DurableCallHeartbeat(activityContext, "too-late"), ErrDurableCallHeartbeatAfterCompletion)
	mu.Lock()
	require.Equal(t, []any{"half-way"}, messages)
	mu.Unlock()
}

func TestDurableActivityReturnsEndpointErrorAndRecordsClosure(t *testing.T) {
	want := errors.New("business failure")
	var events []DurableCallEvent
	durable := NewDurableCallContext("execution", nil, func(_ context.Context, event DurableCallEvent, _ error) {
		events = append(events, event)
	})

	err := RunDurableActivity(context.Background(), durable, func(context.Context) error { return want })
	require.ErrorIs(t, err, want)
	require.Equal(t, []DurableCallEvent{DurableCallEventError}, events)
}

func TestDurableCallHeartbeatOutsideActivityIsNoop(t *testing.T) {
	require.NoError(t, DurableCallHeartbeat(context.Background(), "progress"))
}

func TestDurableWorkflowUsesDurableDelayAndIgnoresHeartbeat(t *testing.T) {
	var waited time.Duration
	durable := NewDurableWorkflowContext("workflow", func(duration time.Duration) error {
		waited = duration
		return nil
	}, nil)
	called := false
	require.NoError(t, RunDurableWorkflow(context.Background(), durable, func(ctx context.Context) error {
		require.NoError(t, DurableCallHeartbeat(ctx, "ignored"))
		handled, err := RunDurableCallDelay(ctx, 3*time.Second, func() { called = true })
		require.True(t, handled)
		return err
	}))
	require.Equal(t, 3*time.Second, waited)
	require.True(t, called)
}

func TestDurableActivityLifecycleIsRecordedOnActivitySpan(t *testing.T) {
	engine := testtracing.New()
	durable := NewDurableCallContext("execution", nil, nil)
	require.NoError(t, RunDurableActivity(
		tracing.EnableSampling(context.Background()), durable,
		func(ctx context.Context) error {
			_, span := engine.Tracer("service").Start(ctx, "temporal.activity")
			require.True(t, BindDurableCallSpan(ctx, span))
			return DurableCallHeartbeat(ctx, "half-way")
		},
	))

	spans := engine.Spans()
	require.Len(t, spans, 1)
	require.Equal(t, "temporal.activity", spans[0].Name)
	require.Equal(t, tracing.StatusOK, spans[0].StatusCode)
	require.Equal(t, []string{"temporal.activity.heartbeat", "temporal.activity.success"}, []string{
		spans[0].Events[0].Name,
		spans[0].Events[1].Name,
	})
}
