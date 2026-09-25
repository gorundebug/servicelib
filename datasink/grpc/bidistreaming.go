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
	"sync/atomic"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
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
	closing      atomic.Bool

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
		if r.span != nil {
			r.span.AddEvent("done_called")
		}
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
			defer ec.pending.Pop(streamID)
			result.closing.Store(true)
			result.err = err
			close(result.ready)
			ec.endpoint.OnBeginRequestFailed(ctx, err)
			return
		}
		var outputSpan tracing.Span
		if ec.tracer != nil && tracing.SamplingEnabled(handlerCtx) {
			handlerCtx, outputSpan = ec.tracer.Start(handlerCtx, "grpc.output", ec.spanAttributes[:]...)
		}
		requestCtx := runtime.WithStreamId(handlerCtx, runtime.NewStreamID())
		if outputSpan != nil {
			outputSpan.AddEvent("begin_request")
		}
		startTime := ec.endpoint.OnRequestStart(requestCtx)

		sid, _ := runtime.StreamIdFromContext(requestCtx)
		requestCtx = withOutgoingStreamID(requestCtx, sid.GetID())

		grpcStream, err := ec.clientFn(requestCtx)
		if err != nil {
			defer ec.pending.Pop(streamID)
			result.closing.Store(true)
			result.err = err
			close(result.ready)
			if outputSpan != nil {
				tracing.SpanError(outputSpan, err)
			}
			if outputSpan != nil {
				outputSpan.AddEvent("grpc_call.error", tracing.StringAttr("error", err.Error()))
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
			if outputSpan != nil {
				outputSpan.End()
			}
			return
		}
		if outputSpan != nil {
			outputSpan.AddEvent("grpc_call")
		}

		doneCh := make(chan struct{})
		result.handlerCtx = handlerCtx
		result.handlerState = handlerState
		result.sender = &grpcSender[ReqT]{sendFn: grpcStream.Send, span: outputSpan}
		result.span = outputSpan
		result.doneCh = doneCh
		close(result.ready)

		// Also release the close waiter when the peer finishes first. This is
		// lifecycle cleanup, not a synthetic call to the user's Done method.
		receiveFinished := make(chan struct{})
		// Close the send side when Done() is called or the context is cancelled.
		go func() {
			select {
			case <-doneCh:
			case <-requestCtx.Done():
			case <-receiveFinished:
			}
			_ = grpcStream.CloseSend()
		}()

		// Receive responses until the server closes the stream.
		go func() {
			defer close(receiveFinished)
			defer ec.pending.Pop(streamID)
			var recvErr error
			msgCount := 0
			for {
				res, err := grpcStream.Recv()
				if errors.Is(err, io.EOF) {
					if outputSpan != nil {
						outputSpan.AddEvent("eof", tracing.Int64Attr("messages_received", int64(msgCount)))
					}
					break
				}
				if err != nil {
					if outputSpan != nil {
						tracing.SpanError(outputSpan, err)
					}
					if outputSpan != nil {
						outputSpan.AddEvent("recv.error", tracing.StringAttr("error", err.Error()))
					}
					recvErr = err
					break
				}
				if err := ec.handler.HandleResponse(handlerCtx, ec.sc, handlerState, res); err != nil {
					if outputSpan != nil {
						tracing.SpanError(outputSpan, err)
					}
					if outputSpan != nil {
						outputSpan.AddEvent("handle_response.error", tracing.StringAttr("error", err.Error()))
					}
					recvErr = err
					break
				}
				msgCount++
			}
			result.closing.Store(true)
			result.mu.Lock()
			defer result.mu.Unlock()
			if recvErr == nil {
				if outputSpan != nil {
					outputSpan.AddEvent("done_received")
				}
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, recvErr, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, recvErr)
			if outputSpan != nil {
				outputSpan.End()
			}
		}()
	} else {
		<-result.ready
		if result.closing.Load() {
			ec.endpoint.OnBeginRequestFailed(ctx, fmt.Errorf("gRPC bidi-streaming session %q is still completing", streamID))
			return
		}
		if result.err != nil {
			// Creation failed on another goroutine; it has already reported
			// and cleaned up, so this message is simply dropped.
			return
		}
	}

	// Bidi responses may overlap messages, but terminal handlers may not reopen
	// the same ID. Check before locking to make EndRequest reentrancy safe.
	if result.closing.Load() {
		ec.endpoint.OnBeginRequestFailed(ctx, fmt.Errorf("gRPC bidi-streaming session %q is still completing", streamID))
		return
	}
	result.mu.RLock()
	defer result.mu.RUnlock()
	if result.closing.Load() {
		ec.endpoint.OnBeginRequestFailed(ctx, fmt.Errorf("gRPC bidi-streaming session %q is still completing", streamID))
		return
	}

	if res, ld := ec.pending.Get(streamID); !ld || res != result {
		ec.endpoint.OnLateResult(ctx, streamID)
		return
	}

	if err := ec.handler.ConsumeMessage(result.handlerCtx, ec.sc, result.handlerState, value, result.sender, result); err != nil {
		if result.span != nil {
			tracing.SpanError(result.span, err)
		}
		if result.span != nil {
			result.span.AddEvent("consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		result.Done()
	} else {
		if result.span != nil {
			result.span.AddEvent("consume_message")
		}
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
	if ec.tracer != nil {
		ec.spanAttributes = runtime.MakeEndpointSpanAttributes(ec.stream, ec.endpoint)
	}
	ec.sc = runtime.MakeSinkStreamContext[T, R, E](
		stream,
		runtime.CollectFunc[R](stream.ConsumeResult),
		runtime.CollectFunc[E](stream.GetErrorStream().Consume),
	)
	stream.SetSinkConsumer(ec)
	endpoint.AddEndpointConsumer(ec)
	env.RegisterEndpointConsumer(ec)
	return ec, nil
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *grpcBidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
