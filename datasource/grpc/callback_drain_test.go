package grpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
)

func TestGRPCFinalizationDrainsAcceptedCallback(t *testing.T) {
	for _, mode := range []string{"unary", "server", "client", "bidi"} {
		t.Run(mode, func(t *testing.T) {
			callbackEntered, releaseCallback := make(chan struct{}), make(chan struct{})
			h := &activeRequestHandler{
				scenario: "cancel", entered: make(chan *activeRequestState, 1), releaseMessage: make(chan struct{}),
				endEntered: make(chan struct{}), releaseEnd: make(chan struct{}), reentrantError: make(chan error, 1),
				callbackGate: func() { close(callbackEntered); <-releaseCallback },
			}
			endpoint := &activeRequestEndpoint{}
			call, deliver, reserved := makeActiveRequestTestConsumer(t, mode, true, h, endpoint)
			h.deliver, h.reopen = deliver, call
			var messageOnce, callbackOnce, endOnce sync.Once
			unblockMessage := func() { messageOnce.Do(func() { close(h.releaseMessage) }) }
			unblockCallback := func() { callbackOnce.Do(func() { close(releaseCallback) }) }
			unblockEnd := func() { endOnce.Do(func() { close(h.releaseEnd) }) }
			t.Cleanup(func() { unblockMessage(); unblockCallback(); unblockEnd() })
			ctx, cancel := context.WithCancel(runtime.WithStreamId(context.Background(), "callback-drain"))
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- call(ctx) }()
			state := awaitActiveRequest(t, h.entered)
			delivered := make(chan struct{})
			go func() { deliver(state.ctx, "reply"); close(delivered) }()
			awaitActiveRequest(t, callbackEntered)
			cancel()
			unblockMessage()
			select {
			case <-h.endEntered:
				t.Fatal("EndRequest raced an accepted callback")
			case <-time.After(20 * time.Millisecond):
			}
			if !reserved("callback-drain") { t.Fatal("active callback lost its request ID") }
			unblockCallback()
			awaitActiveRequest(t, delivered)
			awaitActiveRequest(t, h.endEntered)
			deliver(state.ctx, "late")
			if h.callbacks.Load() != 1 { t.Fatal("closing request invoked another callback") }
			if !reserved("callback-drain") { t.Fatal("EndRequest lost its ID") }
			unblockEnd()
			if err := awaitActiveRequest(t, finished); !errors.Is(err, context.Canceled) { t.Fatalf("got %v", err) }
			if reserved("callback-drain") { t.Fatal("finalized request retained its ID") }
			if endpoint.pending.Load() != 0 { t.Fatal("pending metric was not drained") }
		})
	}
}
