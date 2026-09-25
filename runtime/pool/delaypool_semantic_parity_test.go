package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDelayPoolAlreadyCompletedContextRejectsCallback(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "cancelled"
		if expired {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			p := newTestDelayPool(t)
			if err := p.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer p.Stop(context.Background())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if expired {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
			}
			var calls atomic.Int32
			err := p.Delay(ctx, 100*time.Millisecond, func() {
				calls.Add(1)
			})
			if !errors.Is(err, ctx.Err()) {
				t.Errorf("Delay error = %v, want %v", err, ctx.Err())
			}
			p.Stop(context.Background())
			if got := calls.Load(); got != 0 {
				t.Fatalf("callback count = %d, want 0", got)
			}
		})
	}
}
