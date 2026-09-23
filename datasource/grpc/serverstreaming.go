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

// serverStreamingResult holds callback state for a server-streaming call.
// Identical to bidiStreamingResult: the user registers callbacks per messageID
// and calls result.Done() when all responses have been sent.
type serverStreamingResult[HandlerState, T, ResR, R, E any] struct {
	once               sync.Once
	handlerState       HandlerState
	sender             *streamSender[R, ResR]
	span               tracing.Span
	doneCh             chan struct{}
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap callbackstore.Store[ResultCallback[HandlerState, T, ResR, R, E]]
}

func makeServerStreamingResult[HandlerState, T, ResR, R, E any](
	handlerState HandlerState,
	doneCh chan struct{},
	sender *streamSender[R, ResR],
	span tracing.Span,
) *serverStreamingResult[HandlerState, T, ResR, R, E] {
	return &serverStreamingResult[HandlerState, T, ResR, R, E]{
		once:         sync.Once{},
		handlerState: handlerState,
		sender:       sender,
		span:         span,
		doneCh:       doneCh,
	}
}

func (r *serverStreamingResult[HandlerState, T, ResR, R, E]) SetResultCallback(
	messageID string,
	cb ResultCallback[HandlerState, T, ResR, R, E],
) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap.Set(messageID, cb)
}

func (r *serverStreamingResult[HandlerState, T, ResR, R, E]) Done() {
	r.once.Do(func() {
		if r.span != nil {
			r.span.AddEvent("done_called")
		}
		close(r.doneCh)
	})
}

// serverStreamingEndpointConsumer handles server-streaming gRPC calls via a user-supplied handler.
// The client sends one request; the handler processes it via a single ConsumeMessage call
// followed by an immediate Eof, then streams responses until result.Done() is called.
type serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedEndpointConsumer[T, R, E]
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	pending *store.RotatingMap[string, *serverStreamingResult[HandlerState, T, ResR, R, E]]
}

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *serverStreamingResult[HandlerState, T, ResR, R, E]](pendingRotationInterval)
		if err := ec.pending.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) consumeResult(ctx context.Context, value R) {
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

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) handle(ctx context.Context, req ReqT, server ServerStreamingServer[ResR]) error {
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
	sender := &streamSender[R, ResR]{sendFn: server.Send, active: true, span: span}

	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		if span != nil {
			tracing.SpanError(span, err)
		}
		if span != nil {
			span.AddEvent("begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		sender.close()
		return err
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
	var doneCh chan struct{}
	var result *serverStreamingResult[HandlerState, T, ResR, R, E]
	var resultCtx ResultContext[HandlerState, T, ResR, R, E]
	if ec.hasResult {
		doneCh = make(chan struct{})
		result = makeServerStreamingResult[HandlerState, T, ResR, R, E](handlerState, doneCh, sender, span)
		if err := ec.pending.Set(streamID, result); err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
			_ = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return err
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
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
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
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
	}

	select {
	case <-doneCh:
		if span != nil {
			span.AddEvent("done_received")
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
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
	case <-handlerCtx.Done():
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		select {
		case <-doneCh:
			if span != nil {
				span.AddEvent("done_received")
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		default:
			if span != nil {
				tracing.SpanError(span, handlerCtx.Err())
			}
			if span != nil {
				span.AddEvent("context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
		}
		if err != nil {
			if span != nil {
				tracing.SpanError(span, err)
			}
		}
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
	}
}

func MakeGRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], ServerStreamingHandler[ReqT, ResR], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCDataSourceEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, nil, fmt.Errorf("invalid endpoint config type for GRPCServerStreamingEndpointConsumer for the stream %q", stream.GetName())
	}
	if handler == nil {
		return nil, nil, fmt.Errorf("handler is nil for GRPCServerStreamingEndpointConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	tracingEngine := env.Tracing()
	tracingEnabled := tracingEngine != nil
	if tracingEngine != nil {
		tr = tracingEngine.Tracer(env.ServiceConfig().Name)
	}
	ec := &serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]{
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

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *serverStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
