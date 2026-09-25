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

type bidiBoundaryGate struct {
	ch   chan struct{}
	once sync.Once
}

func newBidiBoundaryGate() *bidiBoundaryGate { return &bidiBoundaryGate{ch: make(chan struct{})} }
func (g *bidiBoundaryGate) open()            { g.once.Do(func() { close(g.ch) }) }

type bidiBoundaryHandler struct {
	messageEntered, messageRelease *bidiBoundaryGate
	endEntered, endRelease         *bidiBoundaryGate
	callDone, responseFails         bool
}

func (*bidiBoundaryHandler) BeginRequest(ctx context.Context, _ StreamContext[string, string, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}
func (h *bidiBoundaryHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, error], _ struct{}, value string, sender Sender[string], result ResultContext) error {
	if err := sender.Send(ctx, value); err != nil {
		return err
	}
	if h.callDone {
		result.Done()
	}
	h.messageEntered.open()
	<-h.messageRelease.ch
	return nil
}
func (h *bidiBoundaryHandler) HandleResponse(context.Context, StreamContext[string, string, error], struct{}, string) error {
	if h.responseFails {
		return errors.New("response rejected")
	}
	return nil
}
func (h *bidiBoundaryHandler) EndRequest(context.Context, StreamContext[string, string, error], error, struct{}) {
	h.endEntered.open()
	<-h.endRelease.ch
}

type bidiBoundaryCall struct {
	*reservationRPC
	closes atomic.Int32
}

func (c *bidiBoundaryCall) CloseSend() error {
	c.closes.Add(1)
	return c.reservationRPC.CloseSend()
}

func TestBidiCompletionPreservesUserDoneAndHandlerLifetime(t *testing.T) {
	for _, tc := range []struct {
		name, endError string
		holdMessage    bool
		callDone       bool
	}{
		{name: "eof"},
		{name: "receive_error", endError: "peer disconnected"},
		{name: "handler_error", endError: "response rejected"},
		{name: "active_message", holdMessage: true},
		{name: "user_done_during_message", holdMessage: true, callDone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &bidiBoundaryHandler{
				messageEntered: newBidiBoundaryGate(), messageRelease: newBidiBoundaryGate(),
				endEntered: newBidiBoundaryGate(), endRelease: newBidiBoundaryGate(),
				callDone: tc.callDone, responseFails: tc.endError == "response rejected",
			}
			defer h.messageRelease.open()
			defer h.endRelease.open()
			if !tc.holdMessage {
				h.messageRelease.open()
			}
			e := &reservationEndpoint{failures: make(chan error, 8), ended: make(chan error, 8)}
			call := &bidiBoundaryCall{reservationRPC: &reservationRPC{
				ctx: root, replies: make(chan reservationReply, 2), sent: make(chan string, 8), sendClosed: make(chan struct{}),
			}}
			consumer := &grpcBidiStreamingSinkConsumer[struct{}, string, string, string, string, error]{
				grpcTypedSinkEndpointConsumer: grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: e},
				handler: h,
				clientFn: func(context.Context) (BidiStreamingGRPCStream[string, string], error) { return call, nil },
			}
			require.NoError(t, consumer.Start(root))
			defer consumer.Stop(context.Background())
			returned := make(chan struct{})
			go func() {
				consumer.Consume(runtime.WithStreamId(root, "completion-boundary"), "request")
				close(returned)
			}()
			reservationReceive(t, h.messageEntered.ch)
			if tc.callDone {
				reservationReceive(t, call.sendClosed)
			}
			if h.responseFails {
				call.replies <- reservationReply{value: "response"}
			} else if tc.endError != "" {
				call.replies <- reservationReply{err: errors.New(tc.endError)}
			} else {
				call.replies <- reservationReply{err: io.EOF}
			}
			if tc.holdMessage {
				select {
				case <-h.endEntered.ch:
					t.Fatal("EndRequest overtook active ConsumeMessage")
				case <-time.After(25 * time.Millisecond):
				}
			}
			h.messageRelease.open()
			reservationReceive(t, returned)
			reservationReceive(t, h.endEntered.ch)
			if !tc.callDone {
				require.Zero(t, call.closes.Load(), "cleanup overtook EndRequest")
			}
			h.endRelease.open()
			err := reservationReceive(t, e.ended)
			if tc.endError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.endError)
			}
			reservationReceive(t, call.sendClosed)
			require.Equal(t, int32(1), call.closes.Load())
			require.Empty(t, e.failures)
			require.NoError(t, root.Err(), "internal completion cancelled caller context")
		})
	}
}
