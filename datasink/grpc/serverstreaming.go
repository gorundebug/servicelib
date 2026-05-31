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

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"google.golang.org/grpc/metadata"
)

// ServerStreamingGRPCStream is the minimal interface satisfied by a gRPC
// server-streaming client (grpc.ServerStreamingClient[Res]).
type ServerStreamingGRPCStream[ResR any] interface {
	Recv() (ResR, error)
}

type grpcServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	grpcTypedSinkEndpointConsumer[T, R, E]
	handler  EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	clientFn ServerStreamingClientFunction[ReqT, ResR]
}

func (ec *grpcServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Consume(ctx context.Context, value T) {
	var span tracing.Span
	if ec.tracer != nil {
		ctx, span = ec.tracer.Start(ctx, "grpc.output",
			tracing.StringAttr("endpoint", ec.endpoint.GetName()),
		)
		defer span.End()
	}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		ec.endpoint.OnBeginRequestFailed(ctx, err)
		return
	}
	tracing.SpanEvent(span, "begin_request")
	startTime := ec.endpoint.OnRequestStart(handlerCtx)

	sender := &requestSender[ReqT]{}
	if err := ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, value, sender, nopResultContext{}); err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "consume_message")

	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		handlerCtx = metadata.AppendToOutgoingContext(handlerCtx, "x-stream-id", sid.GetID())
	}

	grpcStream, err := ec.clientFn(handlerCtx, sender.req)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "grpc_call.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "grpc_call")

	msgCount := 0
	for {
		res, err := grpcStream.Recv()
		if errors.Is(err, io.EOF) {
			if span != nil {
				tracing.SpanEvent(span, "eof", tracing.Int64Attr("messages_received", int64(msgCount)))
			}
			break
		}
		if err != nil {
			tracing.SpanError(span, err)
			if span != nil {
				tracing.SpanEvent(span, "recv.error", tracing.StringAttr("error", err.Error()))
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
			return
		}
		if err := ec.handler.HandleResponse(handlerCtx, ec.sc, handlerState, res); err != nil {
			tracing.SpanError(span, err)
			if span != nil {
				tracing.SpanEvent(span, "handle_response.error", tracing.StringAttr("error", err.Error()))
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
			return
		}
		msgCount++
	}

	ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
	ec.endpoint.OnRequestEnd(handlerCtx, startTime, nil)
}

func (ec *grpcServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(_ context.Context) error {
	return nil
}

func (ec *grpcServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(_ context.Context) {
}

func MakeGRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	clientFn ServerStreamingClientFunction[ReqT, ResR],
) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for GRPCServerStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createGRPCSinkEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.GrpcEndpointConfig); !ok {
		return nil, fmt.Errorf("invalid endpoint config type for GRPCServerStreamingSinkConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &grpcServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E]{
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
