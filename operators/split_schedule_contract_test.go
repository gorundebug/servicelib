package operators

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
)

// These callers isolate Split's sorting/dispatch contract from config resolution.
// Parallel execution itself uses the ordinary production ServiceApp scheduler.
type scheduleContractCaller struct {
	app      *runtime.ServiceApp
	parallel bool
	call     func(context.Context, int)
}

func (c *scheduleContractCaller) IsAsync() bool { return c.parallel }
func (c *scheduleContractCaller) Consume(ctx context.Context, value int) {
	if c.parallel {
		c.app.RunParallel(ctx, func() { c.call(ctx, value) })
		return
	}
	c.call(ctx, value)
}

// Build only checks that the consumer is present; all callback observation goes
// through the injected callers, not through a mock consumer implementation.
type scheduleContractConsumer struct{ runtime.TypedStreamConsumer[int] }

func TestSplitSchedulesParallelBeforeWaitingForDirectBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var app runtime.ServiceApp
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	returned := make(chan struct{})
	var releaseOnce sync.Once
	releaseParallel := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseParallel()
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	direct := &scheduleContractCaller{app: &app, call: func(ctx context.Context, _ int) {
		record("direct entered")
		select {
		case <-entered:
			record("direct finished")
		case <-ctx.Done():
		}
	}}
	parallel := &scheduleContractCaller{app: &app, parallel: true, call: func(ctx context.Context, _ int) {
		defer close(finished)
		record("parallel entered")
		close(entered)
		select {
		case <-release:
			record("parallel finished")
		case <-ctx.Done():
		}
	}}
	split := &SplitStream[int]{links: []*SplitLink[int]{
		{index: 0, caller: direct, consumer: &scheduleContractConsumer{}},
		{index: 1, caller: parallel, consumer: &scheduleContractConsumer{}},
	}}
	if err := split.Build(); err != nil {
		t.Fatal(err)
	}
	go func() {
		split.Consume(ctx, 42)
		close(returned)
	}()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("direct branch prevented parallel dispatch")
	}
	if ctx.Err() != nil {
		t.Fatal("Split only returned after the test deadline")
	}
	select {
	case <-finished:
		t.Fatal("parallel callback finished before release")
	default:
	}
	mu.Lock()
	snapshot := append([]string(nil), events...)
	mu.Unlock()
	if len(snapshot) != 3 || snapshot[2] != "direct finished" {
		t.Fatalf("Consume did not await the direct callback: %v", snapshot)
	}
	releaseParallel()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("released parallel callback did not finish")
	}
}
