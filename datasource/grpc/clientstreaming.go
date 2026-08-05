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

// clientStreamingSender calls server.SendAndClose on the first Send and then
// signals Done. Subsequent Send calls are no-ops (once semantics).
type clientStreamingSender[R, ResR any] struct {
    once         sync.Once
    sendAndClose func(ResR) error
    done         func()
    span         tracing.Span
}

func (s *clientStreamingSender[R, ResR]) Send(_ context.Context, value ResR) error {
    var err error
    s.once.Do(func() {
        err = s.sendAndClose(value)
        if err != nil {
            tracing.SpanError(s.span, err)
            if s.span != nil {
                tracing.SpanEvent(s.span, "send.error", tracing.StringAttr("error", err.Error()))
            }
        } else {
            tracing.SpanEvent(s.span, "send")
        }
        s.done()
    })
    return err
}

// clientStreamingResult holds callback state for a client-streaming call.
type clientStreamingResult[HandlerState, T, ResR, R, E any] struct {
    once               sync.Once
    handlerState       HandlerState
    sender             *clientStreamingSender[R, ResR]
    span               tracing.Span
    doneCh             chan struct{}
    mu                 sync.RWMutex
    cbMu               sync.Mutex
    messageCallbackMap map[string]ResultCallback[HandlerState, T, ResR, R, E]
}

func makeClientStreamingResult[HandlerState, T, ResR, R, E any](
    handlerState HandlerState,
    doneCh chan struct{},
    sender *clientStreamingSender[R, ResR],
    span tracing.Span,
) *clientStreamingResult[HandlerState, T, ResR, R, E] {
    return &clientStreamingResult[HandlerState, T, ResR, R, E]{
        handlerState:       handlerState,
        sender:             sender,
        span:               span,
        doneCh:             doneCh,
        messageCallbackMap: make(map[string]ResultCallback[HandlerState, T, ResR, R, E]),
    }
}

func (r *clientStreamingResult[HandlerState, T, ResR, R, E]) SetResultCallback(
    messageID string,
    cb ResultCallback[HandlerState, T, ResR, R, E],
) {
    r.cbMu.Lock()
    defer r.cbMu.Unlock()
    r.messageCallbackMap[messageID] = cb
}

func (r *clientStreamingResult[HandlerState, T, ResR, R, E]) Done() {
    r.once.Do(func() {
        tracing.SpanEvent(r.span, "done_called")
        close(r.doneCh)
    })
}

// clientStreamingEndpointConsumer handles client-streaming gRPC calls via a user-supplied handler.
type clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
    grpcTypedEndpointConsumer[T, R, E]
    handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
    pending *store.RotatingMap[string, *clientStreamingResult[HandlerState, T, ResR, R, E]]
}

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
    if ec.hasResult {
        ec.pending = store.MakeRotatingMap[string, *clientStreamingResult[HandlerState, T, ResR, R, E]](pendingRotationInterval)
        if err := ec.pending.Start(ctx); err != nil {
            return err
        }
    }
    return nil
}

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
    if ec.pending != nil {
        ec.pending.Stop(ctx)
    }
}

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) consumeResult(ctx context.Context, value R) {
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

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) handle(ctx context.Context, server ClientStreamingServer[ReqT, ResR]) error {
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
    var doneCh chan struct{}
    handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
    if err != nil {
        tracing.SpanError(span, err)
        if span != nil {
            tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
        }
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

    sender := &clientStreamingSender[R, ResR]{
        sendAndClose: server.SendAndClose,
        span:         span,
    }
    var result *clientStreamingResult[HandlerState, T, ResR, R, E]
    var resultCtx ResultContext[HandlerState, T, ResR, R, E]
    if ec.hasResult {
        doneCh = make(chan struct{})
        result = makeClientStreamingResult[HandlerState, T, ResR, R, E](handlerState, doneCh, sender, span)
        sender.done = result.Done
        if err := ec.pending.Set(streamID, result); err != nil {
            tracing.SpanError(span, err)
            _ = ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
            ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
            return err
        }
        ec.Endpoint().OnPendingAdd(handlerCtx, streamID)
        resultCtx = result
    } else {
        sender.done = func() {}
        resultCtx = noopResultContext[HandlerState, T, ResR, R, E]{}
    }

    msgCount := 0
    for {
        req, recvErr := server.Recv()
        if errors.Is(recvErr, io.EOF) {
            if span != nil {
                tracing.SpanEvent(span, "eof", tracing.Int64Attr("messages_received", int64(msgCount)))
            }
            ec.handler.Eof(handlerCtx, ec.sc, handlerState)
            break
        }
        if recvErr != nil {
            if ec.hasResult {
                result.mu.Lock()
                defer result.mu.Unlock()
                ec.pending.Pop(streamID)
                ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
            }
            tracing.SpanError(span, recvErr)
            if span != nil {
                tracing.SpanEvent(span, "recv.error", tracing.StringAttr("error", recvErr.Error()))
            }
            endErr := ec.handler.EndRequest(handlerCtx, ec.sc, recvErr, handlerState)
            if endErr != nil {
                tracing.SpanError(span, endErr)
            }
            ec.Endpoint().OnRequestEnd(handlerCtx, startTime, endErr)
            return endErr
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
            endErr := ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
            if endErr != nil {
                tracing.SpanError(span, endErr)
            }
            ec.Endpoint().OnRequestEnd(handlerCtx, startTime, endErr)
            return endErr
        }
        msgCount++
    }

    if !ec.hasResult {
        var zero ResR
        sendErr := sender.Send(handlerCtx, zero)
        endErr := ec.handler.EndRequest(handlerCtx, ec.sc, sendErr, handlerState)
        if endErr != nil {
            tracing.SpanError(span, endErr)
        }
        ec.Endpoint().OnRequestEnd(handlerCtx, startTime, endErr)
        return endErr
    }

    select {
    case <-doneCh:
        tracing.SpanEvent(span, "done_received")
        result.mu.Lock()
        defer result.mu.Unlock()
        ec.pending.Pop(streamID)
        ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
        endErr := ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
        if endErr != nil {
            tracing.SpanError(span, endErr)
        }
        ec.Endpoint().OnRequestEnd(handlerCtx, startTime, endErr)
        return endErr
    case <-handlerCtx.Done():
        tracing.SpanError(span, handlerCtx.Err())
        if span != nil {
            tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
        }
        result.mu.Lock()
        defer result.mu.Unlock()
        ec.pending.Pop(streamID)
        ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
        endErr := ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
        if endErr != nil {
            tracing.SpanError(span, endErr)
        }
        ec.Endpoint().OnRequestEnd(handlerCtx, startTime, endErr)
        return endErr
    }
}

func MakeGRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
    stream runtime.TypedInputStream[T, R, E],
    handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], ClientStreamingHandler[ReqT, ResR], error) {
    env := stream.GetRuntimeEnvironment()
    endpoint, err := createGRPCDataSourceEndpoint(stream.GetEndpointId(), env)
    if err != nil {
        return nil, nil, err
    }
    if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
        return nil, nil, fmt.Errorf("invalid endpoint config type for GRPCClientStreamingEndpointConsumer for the stream %q", stream.GetName())
    }
    if handler == nil {
        return nil, nil, fmt.Errorf("handler is nil for GRPCClientStreamingEndpointConsumer for the stream %q", stream.GetName())
    }
    var tr tracing.Tracer
    if t := env.Tracing(); t != nil {
        tr = t.Tracer(env.ServiceConfig().Name)
    }
    ec := &clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]{
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

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
    return ec.Endpoint().GetID()
}

func (ec *clientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
    return ec.handler
}
