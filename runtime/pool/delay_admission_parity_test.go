package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDelayAdmissionCancelStopRacePreservesExactlyAcceptedCallbacks(t *testing.T) {
	for round := 0; round < 200; round++ {
		p := newTestDelayPool(t)
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(3)
		var executions atomic.Int32
		var accepted bool
		go func() {
			defer workers.Done()
			<-start
			accepted = p.Delay(ctx, time.Hour, func() {
				executions.Add(1)
			}) == nil
		}()
		go func() {
			defer workers.Done()
			<-start
			cancel()
		}()
		go func() {
			defer workers.Done()
			<-start
			p.Stop(context.Background())
		}()
		close(start)
		done := make(chan struct{})
		go func() {
			workers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: admission/cancellation/stop did not finish", round)
		}
		want := int32(0)
		if accepted {
			want = 1
		}
		if got := executions.Load(); got != want {
			t.Fatalf("round %d: callback count = %d, want %d", round, got, want)
		}
		if err := p.Delay(context.Background(), 0, func() {}); err != ErrPoolStopped {
			t.Fatalf("round %d: closed pool admission = %v, want %v", round, err, ErrPoolStopped)
		}
	}
}
