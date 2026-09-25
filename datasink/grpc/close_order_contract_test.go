package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
)

type closeOrderRPC struct{ entered chan struct{} }

func (r *closeOrderRPC) Send(string) error { return nil }
func (r *closeOrderRPC) CloseAndRecv() (string, error) {
	close(r.entered)
	return "response", nil
}

type closeOrderHandler struct {
	reservationHandler
	afterDone *reservationGate
}

func (h *closeOrderHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, error], _ int,
	value string, sender Sender[string], result ResultContext) error {
	if err := sender.Send(ctx, value); err != nil {
		return err
	}
	result.Done()
	h.afterDone.hold()
	return nil
}

func TestClientStreamingCloseStartsBeforeActiveHandlerReturnsButResponseWaits(t *testing.T) {
	ctx := runtime.WithStreamId(context.Background(), "close-order")
	endpoint := &reservationEndpoint{failures: make(chan error, 2), ended: make(chan error, 2)}
	handler := &closeOrderHandler{afterDone: newReservationGate()}
	handler.response, handler.end = newReservationGate(), newReservationGate()
	handler.response.open()
	handler.end.open()
	responses := make(chan struct{}, 1)
	handler.onResponse = func(context.Context) { responses <- struct{}{} }
	handler.onEnd = func(context.Context) {}
	defer handler.afterDone.open()
	rpc := &closeOrderRPC{entered: make(chan struct{})}
	consumer := &grpcClientStreamingSinkConsumer[int, string, string, string, string, error]{
		grpcTypedSinkEndpointConsumer: grpcTypedSinkEndpointConsumer[string, string, error]{endpoint: endpoint},
		handler: handler,
		clientFn: func(context.Context) (ClientStreamingGRPCStream[string, string], error) { return rpc, nil },
	}
	require.NoError(t, consumer.Start(ctx))
	defer consumer.Stop(context.Background())
	returned := make(chan struct{})
	go func() { defer close(returned); consumer.Consume(ctx, "request") }()
	reservationReceive(t, handler.afterDone.entered)
	select {
	case <-rpc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseAndRecv is delayed by active ConsumeMessage")
	}
	require.Empty(t, responses, "HandleResponse must wait for active ConsumeMessage")
	require.Empty(t, endpoint.ended)
	handler.afterDone.open()
	reservationReceive(t, returned)
	reservationReceive(t, responses)
	require.NoError(t, reservationReceive(t, endpoint.ended))
	require.Empty(t, endpoint.failures)
}
