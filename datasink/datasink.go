/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasink

import (
	"context"

	grpcsink "github.com/gorundebug/servicelib/datasink/grpc"
	httpsink "github.com/gorundebug/servicelib/datasink/http"
	"github.com/gorundebug/servicelib/datasink/kafka"
	"github.com/gorundebug/servicelib/datasink/localsink"
	temporalsink "github.com/gorundebug/servicelib/datasink/temporal"
	"github.com/gorundebug/servicelib/runtime"
)

func TemporalEndpointConsumer[T, E any](stream runtime.TypedSinkStream[T, E]) (runtime.Consumer[T], error) {
	return temporalsink.MakeDirectEndpointConsumer(stream)
}

func TemporalEndpointConsumerWithHandler[HandlerState, T, E any](stream runtime.TypedSinkStream[T, E], handler temporalsink.EndpointHandler[HandlerState, T]) (runtime.Consumer[T], error) {
	return temporalsink.MakeEndpointConsumer(stream, handler)
}

func TemporalEndpointConsumerWithResult[T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E]) (runtime.Consumer[T], error) {
	return temporalsink.MakeDirectEndpointConsumerWithResult(stream)
}

func TemporalEndpointConsumerWithResultAndHandler[HandlerState, T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E], handler temporalsink.EndpointHandler[HandlerState, T]) (runtime.Consumer[T], error) {
	return temporalsink.MakeEndpointConsumerWithResult(stream, handler)
}

func CustomEndpointConsumer[HandlerState, T, E any](stream runtime.TypedSinkStream[T, E], handler localsink.EndpointHandler[HandlerState, T, E]) (runtime.Consumer[T], error) {
	return localsink.MakeCustomEndpointConsumer[HandlerState, T, E](stream, handler)
}

func SaramaKafkaEndpointConsumer[HandlerState, T, E any](stream runtime.TypedSinkStream[T, E], handler kafka.EndpointHandler[HandlerState, T, E], opts ...kafka.SaramaKafkaSinkOption[HandlerState, T, E]) (runtime.Consumer[T], error) {
	return kafka.MakeSaramaKafkaEndpointConsumer[HandlerState, T, E](stream, handler, opts...)
}

func GRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler grpcsink.EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	streamFactory func(ctx context.Context) (grpcsink.BidiStreamingGRPCStream[ReqT, ResR], error),
) (runtime.Consumer[T], error) {
	return grpcsink.MakeGRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler, streamFactory)
}

func GRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler grpcsink.EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	callFn func(ctx context.Context, req ReqT) (grpcsink.ServerStreamingGRPCStream[ResR], error),
) (runtime.Consumer[T], error) {
	return grpcsink.MakeGRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler, callFn)
}

func GRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler grpcsink.EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	streamFactory func(ctx context.Context) (grpcsink.ClientStreamingGRPCStream[ReqT, ResR], error),
) (runtime.Consumer[T], error) {
	return grpcsink.MakeGRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler, streamFactory)
}

func GRPCNoStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler grpcsink.EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
	callFn func(ctx context.Context, req ReqT) (ResR, error),
) (runtime.Consumer[T], error) {
	return grpcsink.MakeGRPCNoStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler, callFn)
}

func NetHTTPSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	client httpsink.Client,
	handler httpsink.EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], error) {
	return httpsink.MakeNetHTTPEndpointConsumer[HandlerState, ReqT, ResR](stream, client, handler)
}
