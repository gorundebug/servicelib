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

// bidiStreamingResult holds callback state for a bidi-streaming call.
type bidiStreamingResult[HandlerState, T, ResR, R, E any] struct {
	once               sync.Once
	handlerState       HandlerState
	sender             *streamSender[R, ResR]
	span               tracing.Span
	doneCh             chan struct{}
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap map[string]ResultCallback[HandlerState, T, ResR, R, E]
}

func makeBidiStreamingResult[HandlerState, T, ResR, R, E any](
	handlerState HandlerState,
	doneCh chan struct{},
	sender *streamSender[R, ResR],
	span tracing.Span,
) *bidiStreamingResult[HandlerState, T, ResR, R, E] {
	return &bidiStreamingResult[HandlerState, T, ResR, R, E]{
		once:               sync.Once{},
		handlerState:       handlerState,
		sender:             sender,
		span:               span,
		doneCh:             doneCh,
		messageCallbackMap: make(map[string]ResultCallback[HandlerState, T, ResR, R, E]),
	}
}

func (r *bidiStreamingResult[HandlerState, T, ResR, R, E]) SetResultCallback(
	messageID string,
	cb ResultCallback[HandlerState, T, ResR, R, E],
) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()

	r.messageCallbackMap[messageID] = cb
}

func (r *bidiStreamingResult[HandlerState, T, ResR, R, E]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done_called")
		close(r.doneCh)
	})
}

// bidiStreamingEndpointConsumer handles bidi-streaming gRPC calls via a user-supplied handler.
type bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedEndpointConsumer[T, R, E]
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	pending *store.RotatingMap[string, *bidiStreamingResult[HandlerState, T, ResR, R, E]]
}

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *bidiStreamingResult[HandlerState, T, ResR, R, E]](pendingRotationInterval)
		if err := ec.pending.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) consumeResult(ctx context.Context, value R) {
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
		var duplicate bool
		result.cbMu.Lock()
		if _, exists := result.messageCallbackMap[messageID]; exists {
			delete(result.messageCallbackMap, messageID)
		} else {
			duplicate = true
		}
		result.cbMu.Unlock()
		if duplicate {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), messageID)
			if result.span != nil {
				tracing.SpanEvent(result.span, "duplicate_message_id", tracing.StringAttr("message_id", messageID))
			}
		}
	}
	if result.span != nil {
		tracing.SpanEvent(result.span, "result_consumed", tracing.StringAttr("message_id", messageID))
	}
}

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) handle(ctx context.Context, server BidiStreamingServer[ReqT, ResR]) error {
	if _, ok := runtime.StreamIdFromContext(ctx); !ok {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("x-stream-id"); len(vals) > 0 && vals[0] != "" {
				ctx = runtime.WithStreamId(ctx, vals[0])
			}
		}
	}
	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "grpc.input",
			tracing.StringAttr("stream", ec.Stream().GetName()),
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
		)
		defer span.End()
	}
	sender := &streamSender[R, ResR]{sendFn: server.Send, active: true, span: span}

	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		sender.close()
		return err
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
	var doneCh chan struct{}
	var result *bidiStreamingResult[HandlerState, T, ResR, R, E]
	var resultCtx ResultContext[HandlerState, T, ResR, R, E]
	if ec.hasResult {
		doneCh = make(chan struct{})
		result = makeBidiStreamingResult[HandlerState, T, ResR, R, E](handlerState, doneCh, sender, span)
		if err := ec.pending.Set(streamID, result); err != nil {
			tracing.SpanError(span, err)
			_ = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return err
		}
		ec.Endpoint().OnPendingAdd(handlerCtx, streamID)
		resultCtx = result
	} else {
		resultCtx = noopResultContext[HandlerState, T, ResR, R, E]{}
	}

	msgCount := 0
	for {
		req, err := server.Recv()
		if errors.Is(err, io.EOF) {
			if span != nil {
				tracing.SpanEvent(span, "eof", tracing.Int64Attr("messages_received", int64(msgCount)))
			}
			ec.handler.Eof(handlerCtx, ec.sc, handlerState)
			break
		}
		if err != nil {
			if ec.hasResult {
				result.mu.Lock()
				defer result.mu.Unlock()
				ec.pending.Pop(streamID)
				ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
			}
			tracing.SpanError(span, err)
			if span != nil {
				tracing.SpanEvent(span, "recv.error", tracing.StringAttr("error", err.Error()))
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			if err != nil {
				tracing.SpanError(span, err)
			}
			sender.close()
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return err
		}

		if handlerCtx, err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, req, resultCtx, sender); err != nil {
			if ec.hasResult {
				result.mu.Lock()
				defer result.mu.Unlock()
				ec.pending.Pop(streamID)
				ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
			}
			tracing.SpanError(span, err)
			if span != nil {
				tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
			}
			err = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			if err != nil {
				tracing.SpanError(span, err)
			}
			sender.close()
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return err
		}
		msgCount++
	}

	if !ec.hasResult {
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
	}

	select {
	case <-doneCh:
		tracing.SpanEvent(span, "done_received")
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		err = ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err

	case <-handlerCtx.Done():
		tracing.SpanError(span, handlerCtx.Err())
		if span != nil {
			tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
		}
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		err = ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
		if err != nil {
			tracing.SpanError(span, err)
		}
		sender.close()
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return err
	}
}

func MakeGRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], BidiStreamingHandler[ReqT, ResR], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCDataSourceEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, nil, fmt.Errorf("invalid endpoint config type for GRPCBidiStreamingCustomEndpointConsumer for the stream %q", stream.GetName())
	}
	if handler == nil {
		return nil, nil, fmt.Errorf("handler is nil for GRPCBidiStreamingCustomEndpointConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]{
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

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *bidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
