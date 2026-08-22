/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
	"google.golang.org/grpc/metadata"
)

// BidiStreamingGRPCStream is the minimal interface satisfied by a gRPC
// bidi-streaming client (grpc.BidiStreamingClient[Req, Res]).
type BidiStreamingGRPCStream[ReqT, ResR any] interface {
	Send(ReqT) error
	Recv() (ResR, error)
	CloseSend() error
}

// bidiStreamingResult tracks an open bidi-streaming gRPC connection.
// A single result is created the first time Consume is called for a given streamID
// and reused for all subsequent Consume calls that share the same streamID.
// Call Done() when no more requests will be sent; the framework will then close
// the send side and wait for the server to finish sending responses.
type bidiStreamingResult[HandlerState, ReqT, ResR, T, R any] struct {
	once         sync.Once
	handlerCtx   context.Context
	handlerState HandlerState
	sender       *grpcSender[ReqT]
	span         tracing.Span
	doneCh       chan struct{}
	mu           sync.RWMutex

	// ready is closed once creation of this entry finishes (successfully or
	// not). A caller that finds an existing-but-not-yet-ready entry (a
	// concurrent Consume for the same streamID that is still being created)
	// waits on ready instead of on a service-wide lock.
	ready chan struct{}
	err   error
}

func makeBidiStreamingResult[HandlerState, ReqT, ResR, T, R any]() *bidiStreamingResult[HandlerState, ReqT, ResR, T, R] {
	return &bidiStreamingResult[HandlerState, ReqT, ResR, T, R]{
		ready: make(chan struct{}),
	}
}

func (r *bidiStreamingResult[HandlerState, ReqT, ResR, T, R]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done_called")
		close(r.doneCh)
	})
}

type grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedSinkEndpointConsumer[T, R, E]
	handler  EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	clientFn BidiStreamingClientFunction[ReqT, ResR]
	pending  *store.RotatingMap[string, *bidiStreamingResult[HandlerState, ReqT, ResR, T, R]]
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	ec.pending = store.MakeRotatingMap[string, *bidiStreamingResult[HandlerState, ReqT, ResR, T, R]](pendingRotationInterval)
	return ec.pending.Start(ctx)
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Consume(ctx context.Context, value T) {
	var streamID string
	if sid, ok := runtime.StreamIdFromContext(ctx); ok {
		streamID = sid.GetID()
	} else {
		streamID = runtime.NewStreamID()
		ctx = runtime.WithStreamId(ctx, streamID)
	}

	result, loaded := ec.pending.GetOrCreate(streamID, func() *bidiStreamingResult[HandlerState, ReqT, ResR, T, R] {
		return makeBidiStreamingResult[HandlerState, ReqT, ResR, T, R]()
	})
	if !loaded {
		handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
		if err != nil {
			ec.pending.Pop(streamID)
			result.err = err
			close(result.ready)
			ec.endpoint.OnBeginRequestFailed(ctx, err)
			return
		}
		var outputSpan tracing.Span
		if ec.tracer != nil && tracing.SamplingEnabled(handlerCtx) {
			handlerCtx, outputSpan = ec.tracer.Start(handlerCtx, "grpc.output",
				tracing.StringAttr("stream", ec.stream.GetName()),
				tracing.StringAttr("endpoint", ec.endpoint.GetName()),
			)
		}
		requestCtx := runtime.WithStreamId(handlerCtx, runtime.NewStreamID())
		tracing.SpanEvent(outputSpan, "begin_request")
		startTime := ec.endpoint.OnRequestStart(requestCtx)

		sid, _ := runtime.StreamIdFromContext(requestCtx)
		requestCtx = metadata.AppendToOutgoingContext(requestCtx, "x-stream-id", sid.GetID())

		grpcStream, err := ec.clientFn(requestCtx)
		if err != nil {
			ec.pending.Pop(streamID)
			result.err = err
			close(result.ready)
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
		result.handlerCtx = handlerCtx
		result.handlerState = handlerState
		result.sender = &grpcSender[ReqT]{sendFn: grpcStream.Send, span: outputSpan}
		result.span = outputSpan
		result.doneCh = doneCh
		close(result.ready)

		// Close the send side when Done() is called or the context is cancelled.
		go func() {
			select {
			case <-doneCh:
			case <-requestCtx.Done():
			}
			_ = grpcStream.CloseSend()
		}()

		// Receive responses until the server closes the stream.
		go func() {
			var recvErr error
			msgCount := 0
			for {
				res, err := grpcStream.Recv()
				if errors.Is(err, io.EOF) {
					if outputSpan != nil {
						tracing.SpanEvent(outputSpan, "eof", tracing.Int64Attr("messages_received", int64(msgCount)))
					}
					break
				}
				if err != nil {
					tracing.SpanError(outputSpan, err)
					if outputSpan != nil {
						tracing.SpanEvent(outputSpan, "recv.error", tracing.StringAttr("error", err.Error()))
					}
					recvErr = err
					break
				}
				if err := ec.handler.HandleResponse(handlerCtx, ec.sc, handlerState, res); err != nil {
					tracing.SpanError(outputSpan, err)
					if outputSpan != nil {
						tracing.SpanEvent(outputSpan, "handle_response.error", tracing.StringAttr("error", err.Error()))
					}
					recvErr = err
					break
				}
				msgCount++
			}
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
			if recvErr == nil {
				tracing.SpanEvent(outputSpan, "done_received")
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, recvErr, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, recvErr)
			if outputSpan != nil {
				outputSpan.End()
			}
		}()
	} else {
		<-result.ready
		if result.err != nil {
			// Creation failed on another goroutine; it has already reported
			// and cleaned up, so this message is simply dropped.
			return
		}
	}

	result.mu.RLock()
	defer result.mu.RUnlock()

	if res, ld := ec.pending.Get(streamID); !ld || res != result {
		ec.endpoint.OnLateResult(ctx, streamID)
		return
	}

	if err := ec.handler.ConsumeMessage(result.handlerCtx, ec.sc, result.handlerState, value, result.sender, result); err != nil {
		tracing.SpanError(result.span, err)
		if result.span != nil {
			tracing.SpanEvent(result.span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		result.Done()
	} else {
		tracing.SpanEvent(result.span, "consume_message")
	}
}

func MakeGRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	clientFn BidiStreamingClientFunction[ReqT, ResR],
) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for GRPCBidiStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCSinkEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, fmt.Errorf("invalid endpoint config type for GRPCBidiStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]{
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
	env.RegisterEndpointConsumer(ec)
	return ec, nil
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
