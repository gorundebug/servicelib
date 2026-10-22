package cron

import (
	"context"
	"errors"
	"sync"
)

// A ScheduleEndpointFunction may attach a shorter deadline to the context it
// emits. That context, not just the scheduler's context, bounds result waiting.
// Watches are deduplicated by Done channel and released with the activation.
type resultWait struct {
	mu        sync.Mutex
	stopped   bool
	watches   map[<-chan struct{}]func() bool
	cancelled chan error
}

func newResultWait() *resultWait { return &resultWait{cancelled: make(chan error, 1)} }
func (wait *resultWait) signal(err error) {
	select {
	case wait.cancelled <- err:
	default:
	}
}
func (wait *resultWait) observe(ctx context.Context) bool {
	wait.mu.Lock()
	defer wait.mu.Unlock()
	if wait.stopped {
		return false
	}
	if ctx == nil {
		wait.signal(errors.New("cron endpoint emitted a nil context"))
		return false
	}
	if err := ctx.Err(); err != nil {
		wait.signal(err)
		return false
	}
	done := ctx.Done()
	if done == nil {
		return true
	}
	if wait.watches == nil {
		wait.watches = make(map[<-chan struct{}]func() bool)
	}
	if _, exists := wait.watches[done]; !exists {
		wait.watches[done] = context.AfterFunc(ctx, func() { wait.signal(ctx.Err()) })
	}
	return true
}
func (wait *resultWait) close() {
	wait.mu.Lock()
	wait.stopped = true
	watches := wait.watches
	wait.watches = nil
	wait.mu.Unlock()
	for _, stop := range watches {
		stop()
	}
}
func (wait *resultWait) await(ctx context.Context, result <-chan struct{}) error {
	// Prefer a terminal result that has already reached the input. Its handler
	// may cancel the emitted context immediately after reporting completion.
	select {
	case <-result:
		return nil
	default:
	}
	var err error
	select {
	case <-result:
		return nil
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-wait.cancelled:
	}
	select {
	case <-result:
		return nil
	default:
		return err
	}
}
