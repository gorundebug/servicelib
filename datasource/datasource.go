/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasource

import (
    "github.com/gorundebug/servicelib/datasource/grpc"
    "github.com/gorundebug/servicelib/datasource/http"
    "github.com/gorundebug/servicelib/datasource/kafka"
    "github.com/gorundebug/servicelib/datasource/localsource"
    "github.com/gorundebug/servicelib/runtime"
)

func CustomEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedInputStream[T, R, E], dataProducer localsource.DataProducer[T], handler localsource.EndpointHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
    return localsource.MakeCustomEndpointConsumer[HandlerState](stream, dataProducer, handler)
}

func NetHTTPEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler http.EndpointHandler[HandlerState, ReqT, ResR, T, R, E]) (runtime.Consumer[T], http.HTTPHandler, error) {
	return http.MakeNetHTTPEndpointConsumer[HandlerState, ReqT, ResR](stream, handler)
}

func SaramaKafkaEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler kafka.EndpointHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
    return kafka.MakeSaramaKafkaEndpointConsumer[HandlerState](stream, handler)
}

func GRPCNoStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler grpc.EndpointHandler[HandlerState, ReqT, ResR, T, R, E]) (runtime.Consumer[T], grpc.UnaryHandler[ReqT, ResR], error) {
	return grpc.MakeGRPCNoStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler)
}

func GRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler grpc.EndpointHandler[HandlerState, ReqT, ResR, T, R, E]) (runtime.Consumer[T], grpc.ServerStreamingHandler[ReqT, ResR], error) {
	return grpc.MakeGRPCServerStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler)
}

func GRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler grpc.EndpointHandler[HandlerState, ReqT, ResR, T, R, E]) (runtime.Consumer[T], grpc.ClientStreamingHandler[ReqT, ResR], error) {
	return grpc.MakeGRPCClientStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler)
}

func GRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler grpc.EndpointHandler[HandlerState, ReqT, ResR, T, R, E]) (runtime.Consumer[T], grpc.BidiStreamingHandler[ReqT, ResR], error) {
	return grpc.MakeGRPCBidiStreamingEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](stream, handler)
}
