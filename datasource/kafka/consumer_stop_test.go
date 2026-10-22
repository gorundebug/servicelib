package kafka

import (
	"context"
	"testing"
	"time"
)

type stopFixture struct {
	lifecycleFixture
	terminal context.Context
}

func (f *stopFixture) ConsumerStopContext() context.Context { return f.terminal }

func TestConsumerStopCancelsIdleClaimsAndPersists(t *testing.T) {
	terminal, fail := context.WithCancel(context.Background())
	defer fail()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooks := &stopFixture{terminal: terminal}
	stop := bindConsumerStop(hooks, cancel)
	defer stop()
	fail()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal failure did not cancel the group context")
	}
	// A second bind must not briefly admit another session after failure.
	next, cancelNext := context.WithCancel(context.Background())
	defer cancelNext()
	detach := bindConsumerStop(hooks, cancelNext)
	defer detach()
	if next.Err() == nil {
		t.Fatal("terminal failure was cleared on restart")
	}
}

func TestConsumerStopDefaultAndDetach(t *testing.T) {
	for _, hooks := range []ConsumerLifecycle{nil, &lifecycleFixture{}, &stopFixture{}} {
		ctx, cancel := context.WithCancel(context.Background())
		stop := bindConsumerStop(hooks, cancel)
		stop()
		if ctx.Err() != nil {
			t.Fatal("default retry policy changed")
		}
		cancel()
	}
	terminal, fail := context.WithCancel(context.Background())
	defer fail()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := bindConsumerStop(&stopFixture{terminal: terminal}, cancel)
	if !stop() {
		t.Fatal("callback could not be detached")
	}
	fail()
	if ctx.Err() != nil {
		t.Fatal("detached callback cancelled a later run")
	}
}
