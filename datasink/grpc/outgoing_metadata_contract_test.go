package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestOutgoingRPCMetadataContainsOnlyFreshRequestID(t *testing.T) {
	for _, mode := range []string{"unary", "server", "client", "bidi"} {
		for _, inherited := range []struct {
			name string
			ids  []string
		}{
			{name: "none"},
			{name: "one", ids: []string{"old-rpc"}},
			{name: "multiple", ids: []string{"old-rpc", "older-rpc"}},
		} {
			t.Run(mode+"/"+inherited.name, func(t *testing.T) {
				root, cancel := context.WithCancel(context.Background())
				defer cancel()
				md := metadata.Pairs("x-custom", "preserve")
				if len(inherited.ids) != 0 {
					md.Set("x-stream-id", inherited.ids...)
				}
				parent := runtime.WithStreamId(metadata.NewOutgoingContext(root, md), "caller-parent")
				parent = context.WithValue(parent, rpcScopeKey{}, 1)
				endpoint := &reservationEndpoint{failures: make(chan error, 8), ended: make(chan error, 8)}
				handler := &rpcScopeHandler{messages: make(chan rpcScopeObservation, 8), responses: make(chan rpcScopeObservation, 8)}
				base := grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: endpoint}
				type observation struct {
					id string
					md metadata.MD
				}
				var observed []observation
				transportFailure := errors.New("stop at transport boundary")
				capture := func(ctx context.Context) {
					outgoing, _ := metadata.FromOutgoingContext(ctx)
					id := ""
					if stream, ok := runtime.StreamIdFromContext(ctx); ok {
						id = stream.GetID()
					}
					observed = append(observed, observation{id: id, md: outgoing})
				}
				var consume func(context.Context, string)
				switch mode {
				case "unary":
					consumer := &grpcNoStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context, _ string) (string, error) {
							capture(ctx)
							return "", transportFailure
						},
					}
					consume = consumer.Consume
				case "server":
					consumer := &grpcServerStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context, _ string) (ServerStreamingGRPCStream[string], error) {
							capture(ctx)
							return nil, transportFailure
						},
					}
					consume = consumer.Consume
				case "client":
					consumer := &grpcClientStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context) (ClientStreamingGRPCStream[string, string], error) {
							capture(ctx)
							return nil, transportFailure
						},
					}
					require.NoError(t, consumer.Start(root))
					defer consumer.Stop(context.Background())
					consume = consumer.Consume
				case "bidi":
					consumer := &grpcBidiStreamingSinkConsumer[int, string, string, string, string, error]{
						grpcTypedSinkEndpointConsumer: base, handler: handler,
						clientFn: func(ctx context.Context) (BidiStreamingGRPCStream[string, string], error) {
							capture(ctx)
							return nil, transportFailure
						},
					}
					require.NoError(t, consumer.Start(root))
					defer consumer.Stop(context.Background())
					consume = consumer.Consume
				}
				// A failed transport open retires the first attempt. The second
				// Consume is a fresh attempt, not another message in an open RPC.
				consume(parent, "first")
				consume(parent, "second")
				require.Len(t, observed, 2)
				require.NotEqual(t, observed[0].id, observed[1].id)
				for _, request := range observed {
					require.NotEmpty(t, request.id)
					require.NotEqual(t, "caller-parent", request.id)
					require.Equal(t, []string{"preserve"}, request.md.Get("x-custom"))
					require.Equal(t, []string{request.id}, request.md.Get("x-stream-id"),
						"an inherited outgoing ID must not shadow a new RPC's ID")
				}
				unchanged, ok := metadata.FromOutgoingContext(parent)
				require.True(t, ok)
				require.Equal(t, inherited.ids, unchanged.Get("x-stream-id"))
				require.Equal(t, md, unchanged)
			})
		}
	}
}
