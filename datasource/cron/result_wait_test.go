package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
)

type cancellationScheduleFunction func(context.Context, runtime.ScheduleTrigger, runtime.Collect[string])

func (f cancellationScheduleFunction) OnTrigger(ctx context.Context, trigger runtime.ScheduleTrigger, out runtime.Collect[string]) {
	f(ctx, trigger, out)
}

func TestCronResultWaitCancellation(t *testing.T) {
	for _, mode := range []string{"parent", "child-deadline", "cancelled-child", "parent-without-output"} {
		t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithCancel(runtime.WithStreamId(context.Background(), "cancellation-"+mode))
			defer cancel()
			started := make(chan struct{})
			consumer := endpointConsumer[string, string, error]{hasResult: true, pending: make(map[string]chan struct{})}
			consumer.out = runtime.CollectFunc[string](func(context.Context, string) {})
			consumer.function = cancellationScheduleFunction(func(ctx context.Context, _ runtime.ScheduleTrigger, out runtime.Collect[string]) {
				switch mode {
				case "parent":
					out.Out(ctx, "work")
				case "child-deadline":
					child, stop := context.WithTimeout(ctx, 10*time.Millisecond)
					t.Cleanup(stop)
					out.Out(child, "work")
				case "cancelled-child":
					child, stop := context.WithCancel(ctx)
					stop()
					out.Out(child, "work")
				}
				close(started)
			})
			completed := make(chan error, 1)
			go func() { completed <- consumer.onTrigger(parent, runtime.ScheduleTrigger{}) }()
			<-started
			if mode == "parent" || mode == "parent-without-output" {
				cancel()
			}
			want := error(context.Canceled)
			if mode == "child-deadline" {
				want = context.DeadlineExceeded
			}
			select {
			case err := <-completed:
				if !errors.Is(err, want) {
					t.Fatalf("wrong cancellation outcome: %v want %v", err, want)
				}
			case <-time.After(300 * time.Millisecond):
				// Drain the old implementation too, so the red regression leaves no
				// blocked goroutine in the package test process.
				consumer.consumeResult(parent, "test cleanup")
				<-completed
				t.Error("cancelled activation did not release its result wait")
			}
			consumer.mu.Lock()
			remaining := len(consumer.pending)
			consumer.mu.Unlock()
			if remaining != 0 {
				t.Fatal("cancelled activation retained pending state")
			}
		})
	}
}
func TestCronResultWaitPrefersDeliveredResult(t *testing.T) {
	ctx, cancel := context.WithCancel(runtime.WithStreamId(context.Background(), "completed"))
	defer cancel()
	consumer := endpointConsumer[string, string, error]{hasResult: true, pending: make(map[string]chan struct{})}
	consumer.out = runtime.CollectFunc[string](func(ctx context.Context, _ string) { consumer.consumeResult(ctx, "done") })
	consumer.function = cancellationScheduleFunction(func(ctx context.Context, _ runtime.ScheduleTrigger, out runtime.Collect[string]) {
		out.Out(ctx, "work")
		cancel()
	})
	if err := consumer.onTrigger(ctx, runtime.ScheduleTrigger{}); err != nil {
		t.Fatalf("delivered result lost to cancellation: %v", err)
	}
}
func TestCronResultWaitDropsLateEmission(t *testing.T) {
	ctx := runtime.WithStreamId(context.Background(), "late")
	var saved runtime.Collect[string]
	var calls atomic.Int32
	consumer := endpointConsumer[string, string, error]{hasResult: true, pending: make(map[string]chan struct{})}
	consumer.out = runtime.CollectFunc[string](func(ctx context.Context, _ string) { calls.Add(1); consumer.consumeResult(ctx, "done") })
	consumer.function = cancellationScheduleFunction(func(ctx context.Context, _ runtime.ScheduleTrigger, out runtime.Collect[string]) {
		saved = out
		out.Out(ctx, "work")
	})
	if err := consumer.onTrigger(ctx, runtime.ScheduleTrigger{}); err != nil {
		t.Fatal(err)
	}
	saved.Out(ctx, "late work")
	if calls.Load() != 1 {
		t.Fatal("completed activation admitted a late emission")
	}
}
func TestCronResultWaitReleasesWatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := newResultWait()
	if !wait.observe(ctx) || !wait.observe(ctx) {
		t.Fatal("live context rejected")
	}
	if len(wait.watches) != 1 {
		t.Fatal("duplicate cancellation watches")
	}
	wait.close()
	if len(wait.watches) != 0 || wait.observe(ctx) {
		t.Fatal("closed wait retained or accepted a watch")
	}
}
