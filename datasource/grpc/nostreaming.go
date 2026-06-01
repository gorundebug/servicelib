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

// unarySender sends a single result through a buffered channel.
type unarySender[R, ResR any] struct {
	ch   chan<- ResR
	span tracing.Span
}

func (s *unarySender[R, ResR]) Send(value ResR) error {
	select {
	case s.ch <- value:
		tracing.SpanEvent(s.span, "send")
		return nil
	default:
		err := fmt.Errorf("result already sent")
		tracing.SpanError(s.span, err)
		if s.span != nil {
			tracing.SpanEvent(s.span, "send.error", tracing.StringAttr("error", err.Error()))
		}
		return err
	}
}

// noStreamingResult holds callback state for a unary gRPC call.
// Single response via replyCh; Done is a no-op (the framework waits on replyCh directly).
type noStreamingResult[HandlerState, T, ResR, R, E any] struct {
	handlerState       HandlerState
	sender             *unarySender[R, ResR]
	span               tracing.Span
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap map[string]ResultCallback[HandlerState, T, ResR, R, E]
}

func makeNoStreamingResult[HandlerState, T, ResR, R, E any](
	handlerState HandlerState,
	sender *unarySender[R, ResR],
	span tracing.Span,
) *noStreamingResult[HandlerState, T, ResR, R, E] {
	return &noStreamingResult[HandlerState, T, ResR, R, E]{
		handlerState:       handlerState,
		sender:             sender,
		span:               span,
		messageCallbackMap: make(map[string]ResultCallback[HandlerState, T, ResR, R, E]),
	}
}

func (r *noStreamingResult[HandlerState, T, ResR, R, E]) SetResultCallback(
	messageID string,
	cb ResultCallback[HandlerState, T, ResR, R, E],
) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap[messageID] = cb
}

func (r *noStreamingResult[HandlerState, T, ResR, R, E]) Done() {
}

// noStreamingEndpointConsumer handles unary gRPC calls via a user-supplied handler.
// The client sends one request; the handler processes it via a single ConsumeMessage call
// followed by an immediate Eof, then waits for a single response via replyCh.
type noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedEndpointConsumer[T, R, E]
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	pending *store.RotatingMap[string, *noStreamingResult[HandlerState, T, ResR, R, E]]
}

func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *noStreamingResult[HandlerState, T, ResR, R, E]](pendingRotationInterval)
		if err := ec.pending.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) consumeResult(ctx context.Context, value R) {
	sid, ok := runtime.StreamIdFromContext(ctx)
	if !ok {
		ec.Endpoint().OnMissingStreamID(ctx)
		return
	}
	result, loaded := ec.pending.Get(sid.GetID())
	if !loaded {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		return
	}

	result.mu.RLock()
	defer result.mu.RUnlock()

	if res, ld := ec.pending.Get(sid.GetID()); !ld || res != result {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		tracing.SpanEvent(result.span, "late_result")
		return
	}

	messageID := ec.handler.GetMessageID(ctx, ec.sc, result.handlerState, value)

	result.cbMu.Lock()
	resultCallback, ok := result.messageCallbackMap[messageID]
	result.cbMu.Unlock()
	if !ok || resultCallback == nil {
		ec.Endpoint().OnUnknownMessageID(ctx, sid.GetID(), messageID)
		if result.span != nil {
			tracing.SpanEvent(result.span, "unknown_message_id", tracing.StringAttr("message_id", messageID))
		}
		return
	}
	if resultCallback(ctx, ec.sc, result.handlerState, value, result.sender) {
		result.cbMu.Lock()
		if _, exists := result.messageCallbackMap[messageID]; exists {
			delete(result.messageCallbackMap, messageID)
		} else {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), messageID)
			if result.span != nil {
				tracing.SpanEvent(result.span, "duplicate_message_id", tracing.StringAttr("message_id", messageID))
			}
		}
		result.cbMu.Unlock()
	}
	if result.span != nil {
		tracing.SpanEvent(result.span, "result_consumed", tracing.StringAttr("message_id", messageID))
	}
}

// handle processes a single unary gRPC request.
// It creates a reply channel backed by a unarySender and calls BeginRequest to initialise
// handler state. The request is forwarded to ConsumeMessage, which pushes values into the
// pipeline; Eof is called immediately after to signal end of input.
// If the stream expects a result (hasResult), the call blocks until either a value arrives
// on replyCh via consumeResult, or the context is cancelled. The pending entry is removed
// from the rotating map before returning in all cases.
// If the stream does not expect a result, any value already buffered in replyCh is drained
// and the call returns immediately after Eof.
func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) handle(ctx context.Context, req ReqT) (ResR, error) {
	if _, ok := runtime.StreamIdFromContext(ctx); !ok {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("x-stream-id"); len(vals) > 0 && vals[0] != "" {
				ctx = runtime.WithStreamId(ctx, vals[0])
			}
		}
	}
	var span tracing.Span
	if ec.tracer != nil {
		ctx, span = ec.tracer.Start(ctx, "grpc.input",
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
		)
		defer span.End()
	}
	replyCh := make(chan ResR, 1)
	sender := &unarySender[R, ResR]{ch: replyCh, span: span}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		var zeroRes ResR
		return zeroRes, err
	}
	tracing.SpanEvent(span, "begin_request")
	startTime := ec.Endpoint().OnRequestStart(handlerCtx)
	var streamID string
	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		streamID = sid.GetID()
	} else {
		streamID = runtime.NewStreamID()
		handlerCtx = runtime.WithStreamId(handlerCtx, streamID)
	}
	if span != nil {
		tracing.SpanAttrs(span, tracing.StringAttr("stream_id", streamID), tracing.BoolAttr("has_result", ec.hasResult))
	}
	result := makeNoStreamingResult[HandlerState, T, ResR, R, E](handlerState, sender, span)
	if ec.hasResult {
		ec.pending.Set(streamID, result)
	}

	if handlerCtx, err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, req, result, sender); err != nil {
		if ec.hasResult {
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
		}
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		err = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		var zeroRes ResR
		return zeroRes, err
	}
	tracing.SpanEvent(span, "consume_message")
	ec.handler.Eof(handlerCtx, ec.sc, handlerState)
	tracing.SpanEvent(span, "eof")

	if !ec.hasResult {
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		if err != nil {
			var zeroRes ResR
			return zeroRes, err
		}
		var res ResR
		select {
		case res = <-replyCh:
		default:
		}
		return res, nil
	}

	select {
	case res := <-replyCh:
		tracing.SpanEvent(span, "result_received")
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return res, err
	case <-handlerCtx.Done():
		tracing.SpanError(span, handlerCtx.Err())
		if span != nil {
			tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
		}
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		err = ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		var zeroRes ResR
		return zeroRes, err
	}
}

func MakeGRPCNoStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], UnaryHandler[ReqT, ResR], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCDataSourceEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, nil, fmt.Errorf("invalid endpoint config type for GRPCNoStreamingEndpointConsumer for the stream %q", stream.GetName())
	}
	if handler == nil {
		return nil, nil, fmt.Errorf("handler is nil for GRPCNoStreamingEndpointConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]{
		grpcTypedEndpointConsumer: grpcTypedEndpointConsumer[T, R, E]{
			DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](endpoint, stream),
			hasResult:                  stream.GetResultStream() != nil,
			tracer:                     tr,
		},
		handler: handler,
	}
	ec.sc = runtime.MakeStreamContext[T, R, E](
		ec.Stream(),
		ec.Stream().GetResultStream(),
		runtime.CollectFunc[T](ec.Out),
		runtime.CollectFunc[E](ec.Stream().GetErrorStream().Consume),
	)
	if ec.hasResult {
		stream.SetResultConsumer(&resultConsumerProxy[R]{consumer: ec})
	}
	endpoint.consumer = ec
	env.RegisterEndpointConsumer(ec)
	return ec, ec.handle, nil
}

func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
