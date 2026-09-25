package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
)

func TestGRPCEndRequestControlsFinalError(t *testing.T) {
	replacement := errors.New("replacement error from EndRequest")
	for _, mode := range []string{"unary", "server", "client", "bidi"} {
		for _, hasResult := range []bool{false, true} {
			for _, recoverError := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/result=%v/recover=%v", mode, hasResult, recoverError), func(t *testing.T) {
					var observed error
					h := &activeRequestHandler{
						scenario: "consume_error", entered: make(chan *activeRequestState, 1), releaseMessage: make(chan struct{}),
						endEntered: make(chan struct{}), releaseEnd: make(chan struct{}), reentrantError: make(chan error, 1),
						endResult: func(err error) error {
							observed = err
							if recoverError { return nil }
							return replacement
						},
					}
					endpoint := &activeRequestEndpoint{}
					call, deliver, reserved := makeActiveRequestTestConsumer(t, mode, hasResult, h, endpoint)
					h.deliver = deliver
					// This test isolates the final error contract, not reentrant admission.
					h.reopen = func(context.Context) error { return nil }
					close(h.releaseMessage)
					close(h.releaseEnd)
					err := call(runtime.WithStreamId(context.Background(), "end-result"))
					if !errors.Is(observed, errActiveRequestConsume) { t.Fatalf("EndRequest received %v", observed) }
					want := replacement
					if recoverError { want = nil }
					if !errors.Is(err, want) { t.Fatalf("returned %v, want %v", err, want) }
					if reserved("end-result") || endpoint.pending.Load() != 0 { t.Fatal("final result retained request state") }
				})
			}
		}
	}
}
