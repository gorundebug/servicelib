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

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
	"google.golang.org/grpc/metadata"
)

// ClientStreamingGRPCStream is the minimal interface satisfied by a gRPC
// client-streaming client (grpc.ClientStreamingClient[Req, Res]).
type ClientStreamingGRPCStream[ReqT, ResR any] interface {
	Send(ReqT) error
	CloseAndRecv() (ResR, error)
}

// clientStreamingResult tracks an open client-streaming gRPC connection.
// A single result is created the first time Consume is called for a given streamID
// and reused for all subsequent Consume calls that share the same streamID.
// Call Done() when no more requests will be sent; the framework will then call
// CloseAndRecv() and process the single response via HandleResponse.
type clientStreamingResult[HandlerState, ReqT, ResR, T, R any] struct {
	once         sync.Once
	handlerCtx   context.Context
	handlerState HandlerState
	sender       *grpcSender[ReqT]
	span         tracing.Span
	doneCh       chan struct{}
	mu           sync.RWMutex
}

func makeClientStreamingResult[HandlerState, ReqT, ResR, T, R any](
	handlerCtx context.Context,
	handlerState HandlerState,
	sender *grpcSender[ReqT],
	span tracing.Span,
	doneCh chan struct{},
) *clientStreamingResult[HandlerState, ReqT, ResR, T, R] {
	return &clientStreamingResult[HandlerState, ReqT, ResR, T, R]{
		handlerCtx:   handlerCtx,
		handlerState: handlerState,
		sender:       sender,
		span:         span,
		doneCh:       doneCh,
	}
}

func (r *clientStreamingResult[HandlerState, ReqT, ResR, T, R]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done")
		close(r.doneCh)
	})
}

type grpcClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedSinkEndpointConsumer[T, R, E]
	handler  EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	clientFn ClientStreamingClientFunction[ReqT, ResR]
	pending  *store.RotatingMap[string, *clientStreamingResult[HandlerState, ReqT, ResR, T, R]]
	mu       sync.Mutex
}

func (ec *grpcClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	ec.pending = store.MakeRotatingMap[string, *clientStreamingResult[HandlerState, ReqT, ResR, T, R]](pendingRotationInterval)
	return ec.pending.Start(ctx)
}

func (ec *grpcClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *grpcClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Consume(ctx context.Context, value T) {
	var streamID string
	if sid, ok := runtime.StreamIdFromContext(ctx); ok {
		streamID = sid.GetID()
	} else {
		streamID = runtime.NewStreamID()
		ctx = runtime.WithStreamId(ctx, streamID)
	}

	ec.mu.Lock()
	result, loaded := ec.pending.Get(streamID)
	if !loaded {
		handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
		if err != nil {
			ec.mu.Unlock()
			ec.endpoint.OnBeginRequestFailed(ctx, err)
			return
		}

		var outputSpan tracing.Span
		if ec.tracer != nil {
			handlerCtx, outputSpan = ec.tracer.Start(handlerCtx, "grpc.output",
				tracing.StringAttr("endpoint", ec.endpoint.GetName()),
			)
		}
		tracing.SpanEvent(outputSpan, "begin_request")
		startTime := ec.endpoint.OnRequestStart(handlerCtx)

		if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
			handlerCtx = metadata.AppendToOutgoingContext(handlerCtx, "x-stream-id", sid.GetID())
		}

		grpcStream, err := ec.clientFn(handlerCtx)
		if err != nil {
			ec.mu.Unlock()
			tracing.SpanError(outputSpan, err)
			if outputSpan != nil {
				tracing.SpanEvent(outputSpan, "grpc_call.error", tracing.StringAttr("error", err.Error()))
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
			if outputSpan != nil {
				outputSpan.End()
			}
			return
		}
		tracing.SpanEvent(outputSpan, "grpc_call")

		doneCh := make(chan struct{})
		result = makeClientStreamingResult[HandlerState, ReqT, ResR, T, R](
			handlerCtx, handlerState, &grpcSender[ReqT]{sendFn: grpcStream.Send, span: outputSpan}, outputSpan, doneCh,
		)
		ec.pending.Set(streamID, result)
		ec.mu.Unlock()

		// Wait for Done() or context cancellation, then close and receive the response.
		go func() {
			select {
			case <-doneCh:
			case <-handlerCtx.Done():
				result.mu.Lock()
				defer result.mu.Unlock()
				ec.pending.Pop(streamID)
				tracing.SpanError(outputSpan, handlerCtx.Err())
				if outputSpan != nil {
					tracing.SpanEvent(outputSpan, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
				}
				ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
				ec.endpoint.OnRequestEnd(handlerCtx, startTime, handlerCtx.Err())
				if outputSpan != nil {
					outputSpan.End()
				}
				return
			}

			res, err := grpcStream.CloseAndRecv()

			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)

			if err != nil {
				tracing.SpanError(outputSpan, err)
				if outputSpan != nil {
					tracing.SpanEvent(outputSpan, "close_and_recv.error", tracing.StringAttr("error", err.Error()))
				}
				ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
				ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
				if outputSpan != nil {
					outputSpan.End()
				}
				return
			}
			tracing.SpanEvent(outputSpan, "close_and_recv")
			if err := ec.handler.HandleResponse(handlerCtx, ec.sc, handlerState, res); err != nil {
				tracing.SpanError(outputSpan, err)
				if outputSpan != nil {
					tracing.SpanEvent(outputSpan, "handle_response.error", tracing.StringAttr("error", err.Error()))
				}
				ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
				ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
				if outputSpan != nil {
					outputSpan.End()
				}
				return
			}
			tracing.SpanEvent(outputSpan, "handle_response")
			ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, nil)
			if outputSpan != nil {
				outputSpan.End()
			}
		}()
	} else {
		ec.mu.Unlock()
	}

	result.mu.RLock()
	defer result.mu.RUnlock()

	if res, ld := ec.pending.Get(streamID); !ld || res != result {
		ec.endpoint.OnLateResult(ctx, streamID)
		return
	}

	if err := ec.handler.ConsumeMessage(result.handlerCtx, ec.sc, result.handlerState, value, result.sender, result); err != nil {
		if result.span != nil {
			tracing.SpanEvent(result.span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		result.Done()
	} else {
		tracing.SpanEvent(result.span, "consume_message")
	}
}

func MakeGRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	clientFn ClientStreamingClientFunction[ReqT, ResR],
) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for GRPCClientStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCSinkEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, fmt.Errorf("invalid endpoint config type for GRPCClientStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &grpcClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]{
		grpcTypedSinkEndpointConsumer: grpcTypedSinkEndpointConsumer[T, R, E]{
			endpoint: endpoint,
			stream:   stream,
			tracer:   tr,
		},
		handler:  handler,
		clientFn: clientFn,
	}
	ec.sc = runtime.MakeSinkStreamContext[T, R, E](
		stream,
		runtime.CollectFunc[R](stream.ConsumeResult),
		runtime.CollectFunc[E](stream.GetErrorStream().Consume),
	)
	stream.SetSinkConsumer(ec)
	endpoint.consumer = ec
	return ec, nil
}
