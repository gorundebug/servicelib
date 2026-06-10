/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

const pendingRotationInterval = 30 * time.Second

// ClientStreamingServer is satisfied structurally by grpc.ClientStreamingServer[Req, Res]
// when T = *Req and R = *Res.
type ClientStreamingServer[T, R any] interface {
	Recv() (T, error)
	SendAndClose(R) error
}

// ServerStreamingServer is satisfied structurally by grpc.ServerStreamingServer[Res]
// when R = *Res.
type ServerStreamingServer[R any] interface {
	Send(R) error
}

// BidiStreamingServer is satisfied structurally by grpc.BidiStreamingServer[Req, Res]
// when T = *Req and R = *Res.
type BidiStreamingServer[T, R any] interface {
	Recv() (T, error)
	Send(R) error
}

type BidiStreamingHandler[T, R any] func(ctx context.Context, server BidiStreamingServer[T, R]) error
type ClientStreamingHandler[T, R any] func(ctx context.Context, server ClientStreamingServer[T, R]) error
type ServerStreamingHandler[T, R any] func(ctx context.Context, req T, server ServerStreamingServer[R]) error
type UnaryHandler[T, R any] func(ctx context.Context, req T) (R, error)

type grpcInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type grpcEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type grpcDataSource struct {
	*runtime.InputDataSource
}

type grpcEndpoint struct {
	consumer grpcEndpointConsumer
	*runtime.DataSourceEndpoint
}

type grpcTypedEndpointConsumer[T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	sc        StreamContext[T, R, E]
	hasResult bool
	tracer    tracing.Tracer
}

// Out implements runtime.Collect[T] so custom consumers satisfy the interface.
func (ec *grpcTypedEndpointConsumer[T, R, E]) Out(ctx context.Context, value T) {
	ec.Consume(ctx, value)
}

// Sender sends result values back to the gRPC client.
type Sender[R, ResR any] interface {
	Send(ctx context.Context, value ResR) error
}

// ResultContext is passed to ConsumeMessage and allows the handler to register interest
// in a pipeline result value that will arrive asynchronously.
//
// SetResultCallback registers a callback keyed by messageID. When the pipeline produces
// a result value R whose GetMessageID matches that key, the callback is invoked with the
// value and the sender. The callback is responsible for deciding whether and when to write
// to sender. If the callback returns true the registration is removed; returning false
// keeps it active so the same messageID can match again.
//
// Done signals that the handler no longer expects a result via this context:
//   - For unary calls Done has no effect; the framework always waits for a reply to be
//     written to sender via the internal channel regardless.
//   - For client-streaming calls Done closes the send side of the stream (no further
//     responses will be sent). If sender.Send has not been called yet, the framework
//     will send a zero response via SendAndClose.
//   - For server-streaming and bidi-streaming calls Done stops the pipeline, telling the
//     framework that no further results will be produced for this request.
//
// ResultCallback is the callback type registered via ResultContext.SetResultCallback.
// It is called when a pipeline result R with a matching messageID arrives.
// Return true to deregister the callback after this invocation; false to keep it active.
type ResultCallback[HandlerState, T, ResR, R, E any] func(
	ctx context.Context,
	sc StreamContext[T, R, E],
	handlerState HandlerState,
	value R,
	sender Sender[R, ResR],
) bool

type ResultContext[HandlerState, T, ResR, R, E any] interface {
	SetResultCallback(messageID string, callback ResultCallback[HandlerState, T, ResR, R, E])
	Done()
}

// StreamContext bundles the typed stream, result stream, output collector, and error collector
// that are passed to every EndpointHandler lifecycle method.
type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// EndpointHandler handles gRPC calls for a single endpoint.
//
// Pipeline lifecycle — unary and client-streaming (pipeline result expected):
//
//	BeginRequest → ConsumeMessage → Eof → [await sender] → EndRequest
//	                    │                        ↑
//	                    └─ SetResultCallback ─→ cb(R) → sender.Send
//
// The framework blocks after Eof until a result is written to sender (either directly
// in ConsumeMessage/EndRequest, or via a registered callback when a matching pipeline
// result R arrives). EndRequest is called only after the result is received or the
// context is cancelled.
//
// Pipeline lifecycle — unary and client-streaming (no pipeline result):
//
//	BeginRequest → ConsumeMessage (one or more) → Eof → EndRequest
//
// Pipeline lifecycle — server-streaming and bidi-streaming:
//
//	BeginRequest → ConsumeMessage → Eof → EndRequest
//	                    │            ↑
//	                    └─ SetResultCallback ─→ cb(R) → sender.Send (stream to client)
//
// The handler may also call sender directly at any point. Done() on the ResultContext
// signals that no further results are expected and stops the pipeline.
//
// Result delivery:
// For unary calls the sender is backed by a buffered channel of size 1. At most one
// result must be written; writing more than once returns an error on the second call.
// For client-streaming calls the sender wraps server.SendAndClose via sync.Once:
// the first Send call invokes SendAndClose and signals Done; subsequent calls are no-ops.
// For server-streaming and bidi-streaming calls the sender writes directly to the gRPC
// stream without buffering. Multiple results may be sent and concurrent Send calls are
// safe (serialised internally by the sender).
//
// BeginRequest initialises per-request handler state.
// If it returns a non-nil error the pipeline will not be started: the framework will NOT
// call ConsumeMessage, Eof, or EndRequest. BeginRequest is therefore responsible for
// releasing any resources it acquired before returning the error. If the handler wants to
// send a response even on a begin failure it must write to sender before returning.
//
// ConsumeMessage is called once per inbound message. It may write a result to sender
// directly, or register a callback via resultCtx.SetResultCallback to be notified when
// a matching pipeline result arrives asynchronously. If it returns a non-nil error the
// framework stops processing: Eof will not be called, EndRequest will be called with
// that error. The handler may still write to sender before returning the error.
//
// Eof signals end of the inbound message stream:
//   - For unary and server-streaming calls it is called once, immediately after the single
//     successful ConsumeMessage.
//   - For client-streaming and bidi-streaming calls it is called when the client closes its
//     send side (i.e. after the last Recv returns io.EOF).
//   - Eof is NOT called if ConsumeMessage returned an error.
//
// EndRequest is called after Eof (or after a ConsumeMessage error) to finalise the request.
// It receives the error that caused the pipeline to stop (nil on the happy path).
// EndRequest may write a result to sender; its own return value is propagated to the caller
// as the final RPC error. For unary and client-streaming calls, writing to sender inside
// EndRequest is only meaningful when no result has been sent yet (e.g. on the error path);
// if a reply was already consumed from the channel before EndRequest is called, any
// subsequent write to sender will be silently discarded.
//
// GetMessageID correlates an inbound pipeline result value R with the originating request,
// enabling the framework to route the result back via the correct ResultContext callback.
//
// Thread safety:
// GetMessageID and the ResultContext callbacks registered via SetResultCallback may be
// called concurrently from multiple goroutines (one per pipeline result that arrives for
// the same request), and may also run concurrently with an in-progress ConsumeMessage call.
// Implementations must synchronise all access to HandlerState inside these methods.
// BeginRequest, ConsumeMessage, Eof, and EndRequest are called sequentially from a single
// goroutine per request, so no synchronisation is needed among them — but access to
// HandlerState from these methods still requires synchronisation if it is shared with
// GetMessageID or a registered callback.
type EndpointHandler[HandlerState, ReqT, ResR, T, R, E any] interface {
	BeginRequest(
		ctx context.Context,
		sc StreamContext[T, R, E],
	) (context.Context, HandlerState, error)
	ConsumeMessage(
		ctx context.Context,
		sc StreamContext[T, R, E],
		handlerState HandlerState,
		req ReqT,
		resultCtx ResultContext[HandlerState, T, ResR, R, E],
		sender Sender[R, ResR],
	) (context.Context, error)
	GetMessageID(
		ctx context.Context,
		sc StreamContext[T, R, E],
		handlerState HandlerState,
		value R,
	) string
	Eof(
		ctx context.Context,
		sc StreamContext[T, R, E],
		handlerState HandlerState,
	)
	EndRequest(
		ctx context.Context,
		sc StreamContext[T, R, E],
		err error,
		handlerState HandlerState,
	) error
}

// streamSender serializes concurrent Send calls and deactivates after close.
type streamSender[R, ResR any] struct {
	mu     sync.Mutex
	sendFn func(ResR) error
	active bool
	span   tracing.Span
}

func (s *streamSender[R, ResR]) Send(_ context.Context, value ResR) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return fmt.Errorf("stream is closed")
	}
	err := s.sendFn(value)
	if err != nil {
		tracing.SpanError(s.span, err)
		tracing.SpanEvent(s.span, "send.error", tracing.StringAttr("error", err.Error()))
	} else {
		tracing.SpanEvent(s.span, "send")
	}
	return err
}

func (s *streamSender[R, ResR]) close() {
	s.mu.Lock()
	s.active = false
	s.mu.Unlock()
}

type resultConsumer[T any] interface {
	consumeResult(ctx context.Context, value T)
}

type resultConsumerProxy[T any] struct {
	consumer resultConsumer[T]
}

func (c *resultConsumerProxy[T]) Consume(ctx context.Context, value T) {
	c.consumer.consumeResult(ctx, value)
}

func (ds *grpcDataSource) Start(ctx context.Context) error {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(grpcInputEndpoint).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *grpcDataSource) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		endpoints.At(i).(grpcInputEndpoint).Stop(ctx)
	}
}

func (ep *grpcEndpoint) Start(ctx context.Context) error {
	return ep.consumer.Start(ctx)
}

func (ep *grpcEndpoint) Stop(ctx context.Context) {
	ep.consumer.Stop(ctx)
}

func getOrCreateGRPCDataSource(id int, env runtime.RuntimeEnvironment) (runtime.DataSource, error) {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for datasource with id=%d not found", id)
	}
	dsCfg, ok := cfg.(*config.GrpcDataConnectorConfig)
	if !ok || dsCfg.Implementation != api.DataConnectorImplementationGoogleGRPC {
		return nil, fmt.Errorf("invalid config type for datasource %q", cfg.GetName())
	}
	inputDS, err := runtime.MakeInputDataSource(cfg, env)
	if err != nil {
		return nil, err
	}
	ds := &grpcDataSource{
		InputDataSource: inputDS,
	}
	env.AddDataSource(ds)
	return ds, nil
}

func createGRPCDataSourceEndpoint(id int, env runtime.RuntimeEnvironment) (*grpcEndpoint, error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for endpoint with id=%d not found", id)
	}
	epCfg, ok := cfg.(*config.GrpcEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for endpoint %q", cfg.GetName())
	}
	dataSource, err := getOrCreateGRPCDataSource(epCfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", cfg.GetName())
	}
	sourceEndpoint, err := runtime.MakeDataSourceEndpoint(dataSource, id, env)
	if err != nil {
		return nil, err
	}
	ep := &grpcEndpoint{
		DataSourceEndpoint: sourceEndpoint,
	}
	dataSource.AddEndpoint(ep)
	return ep, nil
}
