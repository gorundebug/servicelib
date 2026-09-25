package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
)

// The peer may finish an RPC before the caller invokes Done. A completed RPC
// must not leave its CloseSend waiter attached to an unrelated long-lived root.
func TestBidiReceiveTerminationRetiresCloseWaiter(t *testing.T) {
	for _, termination := range []struct {
		name string
		err  error
	}{
		{name: "eof", err: io.EOF},
		{name: "transport_error", err: errors.New("peer disconnected")},
	} {
		t.Run(termination.name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			endpoint := &reservationEndpoint{failures: make(chan error, 8), ended: make(chan error, 8)}
			handler := &rpcScopeHandler{messages: make(chan rpcScopeObservation, 8), responses: make(chan rpcScopeObservation, 8)}
			var call *reservationRPC
			consumer := &grpcBidiStreamingSinkConsumer[int, string, string, string, string, error]{
				grpcTypedSinkEndpointConsumer: grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: endpoint},
				handler: handler,
				clientFn: func(ctx context.Context) (BidiStreamingGRPCStream[string, string], error) {
					call = &reservationRPC{ctx: ctx, replies: make(chan reservationReply, 2), sent: make(chan string, 8), sendClosed: make(chan struct{})}
					return call, nil
				},
			}
			require.NoError(t, consumer.Start(root))
			defer consumer.Stop(context.Background())
			ctx := context.WithValue(runtime.WithStreamId(root, "early-peer-finish"), rpcScopeKey{}, 1)
			consumer.Consume(ctx, "request")
			require.Equal(t, "request", reservationReceive(t, call.sent))
			call.replies <- reservationReply{err: termination.err}
			ended := reservationReceive(t, endpoint.ended)
			if errors.Is(termination.err, io.EOF) {
				require.NoError(t, ended)
			} else {
				require.ErrorIs(t, ended, termination.err)
			}

			closed := false
			select {
			case <-call.sendClosed:
				closed = true
			case <-time.After(time.Second):
			}
			// Always release the old implementation's waiter before asserting.
			// Closure only after cancellation proves that EndRequest did not
			// finish this task; the timeout is not used to terminate the test.
			cancel()
			reservationReceive(t, call.sendClosed)
			require.True(t, closed, "CloseSend waiter survived EndRequest until the parent context was cancelled")
		})
	}
}
