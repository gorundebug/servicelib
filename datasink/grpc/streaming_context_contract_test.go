package grpc

import (
	"context"
	"io"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
)

type rpcScopeKey struct{}

type rpcScopeObservation struct{ state, context int }

type rpcScopeHandler struct {
	messages  chan rpcScopeObservation
	responses chan rpcScopeObservation
	begins    int
}

func (h *rpcScopeHandler) BeginRequest(ctx context.Context, _ StreamContext[string, string, error]) (context.Context, int, error) {
	h.begins++
	return ctx, ctx.Value(rpcScopeKey{}).(int), nil
}
func (h *rpcScopeHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, error], state int, value string, sender Sender[string], result ResultContext) error {
	h.messages <- rpcScopeObservation{state, ctx.Value(rpcScopeKey{}).(int)}
	if value == "done" {
		result.Done()
		return nil
	}
	return sender.Send(ctx, value)
}
func (h *rpcScopeHandler) HandleResponse(ctx context.Context, _ StreamContext[string, string, error], state int, _ string) error {
	h.responses <- rpcScopeObservation{state, ctx.Value(rpcScopeKey{}).(int)}
	return nil
}
func (*rpcScopeHandler) EndRequest(context.Context, StreamContext[string, string, error], error, int) {
}

// These are messages of an RPC, not independent SubStream invocations. A local
// context value on a later message does not implicitly open another RPC.
func TestStreamingMessagesPreserveRPCContext(t *testing.T) {
	for _, bidi := range []bool{false, true} {
		name := "client"
		if bidi {
			name = "bidi"
		}
		t.Run(name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			makeContext := func(id string, marker int) context.Context {
				return context.WithValue(runtime.WithStreamId(root, id), rpcScopeKey{}, marker)
			}
			e := &reservationEndpoint{failures: make(chan error, 8), ended: make(chan error, 8)}
			h := &rpcScopeHandler{messages: make(chan rpcScopeObservation, 8), responses: make(chan rpcScopeObservation, 8)}
			var calls []*reservationRPC
			var wireIDs []string
			client := func(ctx context.Context) (*reservationRPC, error) {
				sid, ok := runtime.StreamIdFromContext(ctx)
				require.True(t, ok)
				wireIDs = append(wireIDs, sid.GetID())
				rpc := &reservationRPC{ctx: ctx, replies: make(chan reservationReply, 2), sent: make(chan string, 8), sendClosed: make(chan struct{})}
				calls = append(calls, rpc)
				return rpc, nil
			}
			base := grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: e}
			var consume func(context.Context, string)
			if bidi {
				ec := &grpcBidiStreamingSinkConsumer[int, string, string, string, string, error]{
					grpcTypedSinkEndpointConsumer: base, handler: h,
					clientFn: func(ctx context.Context) (BidiStreamingGRPCStream[string, string], error) { return client(ctx) },
				}
				require.NoError(t, ec.Start(root))
				defer ec.Stop(context.Background())
				consume = ec.Consume
			} else {
				ec := &grpcClientStreamingSinkConsumer[int, string, string, string, string, error]{
					grpcTypedSinkEndpointConsumer: base, handler: h,
					clientFn: func(ctx context.Context) (ClientStreamingGRPCStream[string, string], error) { return client(ctx) },
				}
				require.NoError(t, ec.Start(root))
				defer ec.Stop(context.Background())
				consume = ec.Consume
			}
			first := makeContext("shared", 1)
			consume(first, "first")
			consume(makeContext("shared", 2), "second")
			require.Len(t, calls, 1)
			require.Equal(t, 1, h.begins)
			for range 2 {
				require.Equal(t, rpcScopeObservation{1, 1}, reservationReceive(t, h.messages))
			}
			require.Equal(t, "first", reservationReceive(t, calls[0].sent))
			require.Equal(t, "second", reservationReceive(t, calls[0].sent))
			third, fourth := makeContext("third", 3), makeContext("fourth", 4)
			consume(third, "third")
			consume(fourth, "fourth")
			require.Equal(t, rpcScopeObservation{3, 3}, reservationReceive(t, h.messages))
			require.Equal(t, rpcScopeObservation{4, 4}, reservationReceive(t, h.messages))
			require.Len(t, calls, 3)
			require.Equal(t, 3, h.begins)
			require.NotEqual(t, wireIDs[0], wireIDs[1])
			require.NotEqual(t, wireIDs[0], wireIDs[2])
			require.NotEqual(t, wireIDs[1], wireIDs[2])
			for _, ctx := range []context.Context{first, third, fourth} {
				consume(ctx, "done")
			}
			for _, rpc := range calls {
				rpc.replies <- reservationReply{value: "reply"}
				if bidi {
					rpc.replies <- reservationReply{err: io.EOF}
				}
			}
			var observations []rpcScopeObservation
			for range 3 {
				observations = append(observations, reservationReceive(t, h.responses))
				require.NoError(t, reservationReceive(t, e.ended))
			}
			require.ElementsMatch(t, []rpcScopeObservation{{1, 1}, {3, 3}, {4, 4}}, observations)
			require.Empty(t, e.failures)
			if bidi {
				for _, rpc := range calls {
					reservationReceive(t, rpc.sendClosed)
				}
			}
		})
	}
}
