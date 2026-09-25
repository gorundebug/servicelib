package grpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
)

type reservationEndpoint struct {
	runtime.SinkEndpoint
	failures chan error
	ended    chan error
}

func (e *reservationEndpoint) OnBeginRequestFailed(_ context.Context, err error)      { e.failures <- err }
func (e *reservationEndpoint) OnLateResult(context.Context, string)                   {}
func (e *reservationEndpoint) OnRequestStart(context.Context) time.Time               { return time.Time{} }
func (e *reservationEndpoint) OnRequestEnd(_ context.Context, _ time.Time, err error) { e.ended <- err }

type reservationReply struct {
	value string
	err   error
}
type reservationRPC struct {
	ctx        context.Context
	replies    chan reservationReply
	sent       chan string
	sendClosed chan struct{}
	closeOnce  sync.Once
}

func (r *reservationRPC) Send(value string) error { r.sent <- value; return nil }
func (r *reservationRPC) Recv() (string, error) {
	select {
	case reply := <-r.replies:
		return reply.value, reply.err
	case <-r.ctx.Done():
		return "", r.ctx.Err()
	}
}
func (r *reservationRPC) CloseAndRecv() (string, error) { return r.Recv() }
func (r *reservationRPC) CloseSend() error {
	r.closeOnce.Do(func() { close(r.sendClosed) })
	return nil
}

type reservationGate struct {
	entered, release chan struct{}
	once             sync.Once
}

func newReservationGate() *reservationGate {
	return &reservationGate{entered: make(chan struct{}), release: make(chan struct{})}
}
func (g *reservationGate) open() { g.once.Do(func() { close(g.release) }) }
func (g *reservationGate) hold() { close(g.entered); <-g.release }

type reservationHandler struct {
	begins            atomic.Int32
	consumes          atomic.Int32
	response, end     *reservationGate
	onResponse, onEnd func(context.Context)
	responseError     error
}

func (h *reservationHandler) BeginRequest(ctx context.Context, _ StreamContext[string, string, error]) (context.Context, int, error) {
	return ctx, int(h.begins.Add(1)), nil
}
func (h *reservationHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, error], _ int, value string, sender Sender[string], result ResultContext) error {
	h.consumes.Add(1)
	if err := sender.Send(ctx, value); err != nil {
		return err
	}
	if value == "done" {
		result.Done()
	}
	return nil
}
func (h *reservationHandler) HandleResponse(ctx context.Context, _ StreamContext[string, string, error], state int, _ string) error {
	if state == 1 {
		if h.onResponse != nil {
			h.onResponse(ctx)
		}
		h.response.hold()
		return h.responseError
	}
	return nil
}
func (h *reservationHandler) EndRequest(ctx context.Context, _ StreamContext[string, string, error], _ error, state int) {
	if state == 1 {
		h.onEnd(ctx)
		h.end.hold()
	}
}

func reservationReceive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("streaming reservation test timed out")
	}
	var zero T
	return zero
}

func TestStreamingSinkReservesIDUntilCompletion(t *testing.T) {
	for _, bidi := range []bool{false, true} {
		mode := "client"
		if bidi {
			mode = "bidi"
		}
		for _, scenario := range []string{"success", "response_error", "transport_error", "cancel", "start_error"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(runtime.WithStreamId(context.Background(), "reserved-id"))
				defer cancel()
				endpoint := &reservationEndpoint{failures: make(chan error, 16), ended: make(chan error, 4)}
				handler := &reservationHandler{response: newReservationGate(), end: newReservationGate()}
				defer handler.response.open()
				defer handler.end.open()
				failure := errors.New("test RPC failure")
				if scenario == "response_error" {
					handler.responseError = failure
				}
				opened := make(chan *reservationRPC, 4)
				var opens atomic.Int32
				client := func(callCtx context.Context) (*reservationRPC, error) {
					rpc := &reservationRPC{ctx: callCtx, replies: make(chan reservationReply, 2), sent: make(chan string, 16), sendClosed: make(chan struct{})}
					index := opens.Add(1)
					opened <- rpc
					if index == 1 && scenario == "start_error" {
						return nil, failure
					}
					return rpc, nil
				}
				base := grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: endpoint}
				var consume func(context.Context, string)
				var reserved func() bool
				if bidi {
					ec := &grpcBidiStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context) (BidiStreamingGRPCStream[string, string], error) { return client(ctx) },
					}
					require.NoError(t, ec.Start(ctx))
					defer ec.Stop(context.Background())
					consume = ec.Consume
					reserved = func() bool { _, ok := ec.pending.Get("reserved-id"); return ok }
				} else {
					ec := &grpcClientStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context) (ClientStreamingGRPCStream[string, string], error) { return client(ctx) },
					}
					require.NoError(t, ec.Start(ctx))
					defer ec.Stop(context.Background())
					consume = ec.Consume
					reserved = func() bool { _, ok := ec.pending.Get("reserved-id"); return ok }
				}
				handler.onEnd = func(callbackCtx context.Context) { consume(callbackCtx, "forbidden") }
				if !bidi {
					handler.onResponse = handler.onEnd
				}
				call := func(callCtx context.Context, value string) <-chan struct{} {
					done := make(chan struct{})
					go func() { defer close(done); consume(callCtx, value) }()
					return done
				}
				first := call(ctx, "first")
				rpc := reservationReceive(t, opened)
				if scenario != "start_error" {
					reservationReceive(t, first)
					reservationReceive(t, call(ctx, "second"))
					require.EqualValues(t, 2, handler.consumes.Load())
					if scenario == "success" || scenario == "response_error" {
						if !bidi {
							reservationReceive(t, call(ctx, "done"))
						}
						rpc.replies <- reservationReply{value: "response"}
						reservationReceive(t, handler.response.entered)
						require.True(t, reserved())
						if bidi {
							// An open bidi session still permits messages during a response.
							reservationReceive(t, call(ctx, "during-response"))
							require.EqualValues(t, 3, handler.consumes.Load())
							reservationReceive(t, call(ctx, "done"))
						} else {
							reservationReceive(t, call(ctx, "forbidden"))
							require.Len(t, endpoint.failures, 2)
						}
						handler.response.open()
						if bidi && scenario == "success" {
							rpc.replies <- reservationReply{err: io.EOF}
						}
					} else {
						reservationReceive(t, call(ctx, "done"))
						if scenario == "cancel" {
							cancel()
						} else {
							rpc.replies <- reservationReply{err: failure}
						}
					}
				}
				reservationReceive(t, handler.end.entered)
				require.True(t, reserved(), "ID released before EndRequest returned")
				reservationReceive(t, call(ctx, "forbidden"))
				require.EqualValues(t, 1, opens.Load())
				require.EqualValues(t, 1, handler.begins.Load())
				wantRejected := 2
				if !bidi && (scenario == "success" || scenario == "response_error") {
					wantRejected = 4
				}
				require.Len(t, endpoint.failures, wantRejected)
				for range wantRejected {
					require.Contains(t, (<-endpoint.failures).Error(), "still completing")
				}
				handler.end.open()
				reservationReceive(t, first)
				endError := reservationReceive(t, endpoint.ended)
				if scenario == "success" {
					require.NoError(t, endError)
				} else if scenario == "cancel" {
					require.ErrorIs(t, endError, context.Canceled)
				} else {
					require.ErrorIs(t, endError, failure)
				}
				require.Eventually(t, func() bool { return !reserved() }, 3*time.Second, time.Millisecond)
				if bidi && scenario != "start_error" {
					reservationReceive(t, rpc.sendClosed)
				}

				// Reuse is valid only after the entire previous invocation finished.
				fresh := runtime.WithStreamId(context.Background(), "reserved-id")
				reservationReceive(t, call(fresh, "done"))
				second := reservationReceive(t, opened)
				if bidi {
					second.replies <- reservationReply{err: io.EOF}
				} else {
					second.replies <- reservationReply{value: "next"}
				}
				require.NoError(t, reservationReceive(t, endpoint.ended))
				require.Eventually(t, func() bool { return !reserved() }, 3*time.Second, time.Millisecond)
				if bidi {
					reservationReceive(t, second.sendClosed)
				}
				require.EqualValues(t, 2, handler.begins.Load())
				require.EqualValues(t, 2, opens.Load())
				require.Empty(t, endpoint.failures)
			})
		}
	}
}
