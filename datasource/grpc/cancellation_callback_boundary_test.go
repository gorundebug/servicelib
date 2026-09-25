package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
)

func TestCancelledGRPCContextClosesCallbacksAtFinalization(t *testing.T) {
	for _, mode := range []string{"unary", "server", "client", "bidi"} {
		for _, hasResult := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/result=%v", mode, hasResult), func(t *testing.T) {
				h := &activeRequestHandler{
					scenario: "cancel",
					entered: make(chan *activeRequestState, 1), releaseMessage: make(chan struct{}),
					endEntered: make(chan struct{}), releaseEnd: make(chan struct{}),
					reentrantError: make(chan error, 1),
				}
				endpoint := &activeRequestEndpoint{}
				call, deliver, reserved := makeActiveRequestTestConsumer(t, mode, hasResult, h, endpoint)
				h.deliver, h.reopen = deliver, call
				var messageOnce, endOnce sync.Once
				releaseMessage := func() { messageOnce.Do(func() { close(h.releaseMessage) }) }
				releaseEnd := func() { endOnce.Do(func() { close(h.releaseEnd) }) }
				t.Cleanup(func() { releaseMessage(); releaseEnd() })
				ctx, cancel := context.WithCancel(runtime.WithStreamId(context.Background(), "cancel-boundary"))
				defer cancel()
				finished := make(chan error, 1)
				go func() { finished <- call(ctx) }()
				state := awaitActiveRequest(t, h.entered)
				cancel()
				deliver(state.ctx, "reply")
				want := int32(0)
				if hasResult { want = 1 }
				if got := h.callbacks.Load(); got != want {
					t.Fatalf("cancellation alone closed callbacks: got %d, want %d", got, want)
				}
				if !reserved("cancel-boundary") { t.Fatal("cancelled active handler lost its ID") }
				releaseMessage()
				awaitActiveRequest(t, h.endEntered)
				deliver(state.ctx, "after-finalization-started")
				if h.callbacks.Load() != want { t.Fatal("closing request still accepted callbacks") }
				if !reserved("cancel-boundary") { t.Fatal("EndRequest lost its ID before returning") }
				releaseEnd()
				if err := awaitActiveRequest(t, finished); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
				if reserved("cancel-boundary") { t.Fatal("finished request retained its ID") }
			})
		}
	}
}
