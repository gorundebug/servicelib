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

	"github.com/gorundebug/servicelib/datasource/internal/callbackstore"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
)

// unarySender sends a single result through a buffered channel.
type unarySender[R, ResR any] struct {
	ch   chan<- ResR
	span tracing.Span
}

func (s *unarySender[R, ResR]) Send(_ context.Context, value ResR) error {
	select {
	case s.ch <- value:
		if s.span != nil {
			s.span.AddEvent("send")
		}
		return nil
	default:
		err := fmt.Errorf("result already sent")
		if s.span != nil {
			tracing.SpanError(s.span, err)
		}
		if s.span != nil {
			s.span.AddEvent("send.error", tracing.StringAttr("error", err.Error()))
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
	messageCallbackMap callbackstore.Store[ResultCallback[HandlerState, T, ResR, R, E]]
}

func makeNoStreamingResult[HandlerState, T, ResR, R, E any](
	handlerState HandlerState,
	sender *unarySender[R, ResR],
	span tracing.Span,
) *noStreamingResult[HandlerState, T, ResR, R, E] {
	return &noStreamingResult[HandlerState, T, ResR, R, E]{
		handlerState: handlerState,
		sender:       sender,
		span:         span,
	}
}

func (r *noStreamingResult[HandlerState, T, ResR, R, E]) SetResultCallback(
	messageID string,
	cb ResultCallback[HandlerState, T, ResR, R, E],
) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap.Set(messageID, cb)
}

func (r *noStreamingResult[HandlerState, T, ResR, R, E]) Done() {
}

// noopResultContext is a zero-size ResultContext used when the stream has no result path.
// SetResultCallback is accepted but never fired; Done is a no-op.
type noopResultContext[HandlerState, T, ResR, R, E any] struct{}

func (noopResultContext[HandlerState, T, ResR, R, E]) SetResultCallback(_ string, _ ResultCallback[HandlerState, T, ResR, R, E]) {
}

func (noopResultContext[HandlerState, T, ResR, R, E]) Done() {}

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
		if result.span != nil {
			result.span.AddEvent("late_result")
		}
		return
	}

	messageID := ec.handler.GetMessageID(ctx, ec.sc, result.handlerState, value)

	result.cbMu.Lock()
	resultCallback, ok := result.messageCallbackMap.Get(messageID)
	result.cbMu.Unlock()
	if !ok || resultCallback == nil {
		ec.Endpoint().OnUnknownMessageID(ctx, sid.GetID(), messageID)
		if result.span != nil {
			result.span.AddEvent("unknown_message_id", tracing.StringAttr("message_id", messageID))
		}
		return
	}
	if resultCallback(ctx, ec.sc, result.handlerState, value, result.sender) {
		var duplicate bool
		result.cbMu.Lock()
		duplicate = !result.messageCallbackMap.Remove(messageID)
		result.cbMu.Unlock()
		if duplicate {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), messageID)
			if result.span != nil {
				result.span.AddEvent("duplicate_message_id", tracing.StringAttr("message_id", messageID))
			}
		}
	}
	if result.span != nil {
		result.span.AddEvent("result_consumed", tracing.StringAttr("message_id", messageID))
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
	ctx = applyIncomingStreamID(ctx)
	if ec.tracingEnabled {
		ctx = runtime.ApplyDataSourceEndpointTracing(
			ctx, ec.Endpoint().GetRuntimeEnvironment(), ec.Endpoint().GetID(),
		)
	}
	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "grpc.input", ec.spanAttributes[:]...)
		defer span.End()
	}
	replyCh := make(chan ResR, 1)
	sender := &unarySender[R, ResR]{ch: replyCh, span: span}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		if span != nil {
			tracing.SpanError(span, err)
		}
		if span != nil {
			span.AddEvent("begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		var zeroRes ResR
		return zeroRes, err
	}
	if span != nil {
		span.AddEvent("begin_request")
	}
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
	var result *noStreamingResult[HandlerState, T, ResR, R, E]
	var resultCtx ResultContext[HandlerState, T, ResR, R, E]
	if ec.hasResult {
		result = makeNoStreamingResult[HandlerState, T, ResR, R, E](handlerState, sender, span)
		if err := ec.pending.Set(streamID, result); err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
			_ = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			var zeroRes ResR
			return zeroRes, err
		}
		ec.Endpoint().OnPendingAdd(handlerCtx, streamID)
		resultCtx = result
	} else {
		resultCtx = noopResultContext[HandlerState, T, ResR, R, E]{}
	}

	if handlerCtx, err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, req, resultCtx, sender); err != nil {
		if ec.hasResult {
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
			ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		}
		if span != nil {
			tracing.SpanError(span, err)
		}
		if span != nil {
			span.AddEvent("consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		err = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		if err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		var zeroRes ResR
		return zeroRes, err
	}
	if span != nil {
		span.AddEvent("consume_message")
	}
	ec.handler.Eof(handlerCtx, ec.sc, handlerState)
	if span != nil {
		span.AddEvent("eof")
	}

	if !ec.hasResult {
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
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
		if span != nil {
			span.AddEvent("result_received")
		}
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
		}
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return res, err
	case <-handlerCtx.Done():
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		select {
		case res := <-replyCh:
			if span != nil {
				span.AddEvent("result_received")
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
			if err != nil {
				if span != nil {
					tracing.SpanError(span, err)
				}
			}
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return res, err
		default:
			if span != nil {
				tracing.SpanError(span, handlerCtx.Err())
			}
			if span != nil {
				span.AddEvent("context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
			if err != nil {
				if span != nil {
					tracing.SpanError(span, err)
				}
			}
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			var zeroRes ResR
			return zeroRes, err
		}
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
	tracingEngine := env.Tracing()
	tracingEnabled := tracingEngine != nil
	if tracingEngine != nil {
		tr = tracingEngine.Tracer(env.ServiceConfig().Name)
	}
	ec := &noStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]{
		grpcTypedEndpointConsumer: grpcTypedEndpointConsumer[T, R, E]{
			DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](endpoint, stream),
			hasResult:                  stream.GetResultStream() != nil,
			tracer:                     tr,
			tracingEnabled:             tracingEnabled,
		},
		handler: handler,
	}
	if ec.tracer != nil {
		ec.spanAttributes = runtime.MakeEndpointSpanAttributes(ec.Stream(), ec.Endpoint())
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
