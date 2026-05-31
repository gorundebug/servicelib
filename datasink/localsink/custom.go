/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsink

import (
	"context"
	"fmt"
	"sync"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// EndpointHandler processes pipeline values and optionally pushes results back.
//
// Pipeline lifecycle (one call per Consume):
//
//	GetStreamID → BeginRequest → ConsumeMessage → EndRequest
//
// GetStreamID returns the logical stream identifier for the incoming value.
// It is used to correlate related messages.
//
// BeginRequest initialises per-message handler state. Unlike gRPC sink handlers,
// BeginRequest does not return an error; the framework always proceeds to
// ConsumeMessage. Use the returned context to carry per-request values.
//
// ConsumeMessage processes value T. Push results back into the pipeline by calling
// resultStream.Out. Returning a non-nil error passes it to EndRequest.
//
// EndRequest finalises the request. Its error argument is the non-nil error
// returned by ConsumeMessage, or nil on success. EndRequest does not return an error.
type EndpointHandler[HandlerState, T, R any] interface {
	GetStreamID(ctx context.Context, value T) string
	BeginRequest(ctx context.Context, stream runtime.Stream) (context.Context, HandlerState)
	ConsumeMessage(ctx context.Context, stream runtime.Stream, handlerState HandlerState, value T, resultStream runtime.Collect[R]) error
	EndRequest(ctx context.Context, stream runtime.Stream, err error, handlerState HandlerState)
}

type customOutputDataSink interface {
	runtime.DataSink
	WaitGroup() *sync.WaitGroup
}

type customEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type customSinkEndpoint interface {
	runtime.SinkEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type customDataSink struct {
	*runtime.OutputDataSink
	wg sync.WaitGroup
}

type customEndpoint struct {
	*runtime.DataSinkEndpoint
	consumer customEndpointConsumer
}

func (ds *customDataSink) Start(ctx context.Context) error {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(customSinkEndpoint).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *customDataSink) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint customSinkEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(customSinkEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

func (ds *customDataSink) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ep *customEndpoint) Start(ctx context.Context) error {
	return ep.consumer.Start(ctx)
}

func (ep *customEndpoint) Stop(ctx context.Context) {
	ep.consumer.Stop(ctx)
}

type collector[R any] struct {
	consumer runtime.Consumer[R]
}

func (c *collector[R]) Out(ctx context.Context, value R) {
	c.consumer.Consume(ctx, value)
}

type typedCustomEndpointConsumer[HandlerState, T, R any] struct {
	*runtime.DataSinkEndpointConsumer[T, R]
	handler      EndpointHandler[HandlerState, T, R]
	sinkCallback runtime.SinkCallback[T]
	tracer       tracing.Tracer
}

func (ep *typedCustomEndpointConsumer[HandlerState, T, R]) SetSinkCallback(callback runtime.SinkCallback[T]) {
	ep.sinkCallback = callback
}

func (ep *typedCustomEndpointConsumer[HandlerState, T, R]) Consume(ctx context.Context, value T) {
	var span tracing.Span
	if ep.tracer != nil {
		ctx, span = ep.tracer.Start(ctx, "local.output",
			tracing.StringAttr("endpoint", ep.Endpoint().GetName()),
		)
		defer span.End()
	}
	stream := ep.Stream()
	streamID := ep.handler.GetStreamID(ctx, value)
	handlerCtx := runtime.WithStreamId(ctx, streamID)
	if span != nil {
		tracing.SpanAttrs(span, tracing.StringAttr("stream_id", streamID))
	}
	handlerCtx, handlerState := ep.handler.BeginRequest(handlerCtx, stream)
	tracing.SpanEvent(span, "begin_request")
	startTime := ep.Endpoint().OnRequestStart(handlerCtx)
	rs := &collector[R]{consumer: stream.GetErrorStream()}
	err := ep.handler.ConsumeMessage(handlerCtx, stream, handlerState, value, rs)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
	} else {
		tracing.SpanEvent(span, "consume_message")
	}
	ep.handler.EndRequest(handlerCtx, stream, err, handlerState)
	ep.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
	if ep.sinkCallback != nil {
		ep.sinkCallback.Done(ctx, value, err)
	}
}

func (ep *typedCustomEndpointConsumer[HandlerState, T, R]) Start(_ context.Context) error {
	return nil
}

func (ep *typedCustomEndpointConsumer[HandlerState, T, R]) Stop(_ context.Context) {
}

func getCustomDataSink(id int, env runtime.RuntimeEnvironment) (runtime.DataSink, error) {
	dataSink := env.GetDataSink(id)
	if dataSink != nil {
		return dataSink, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for datasink with id=%d not found", id)
	}
	outputDS, err := runtime.MakeOutputDataSink(cfg, env)
	if err != nil {
		return nil, err
	}
	ds := &customDataSink{
		OutputDataSink: outputDS,
	}
	var outputDataSink customOutputDataSink = ds
	env.AddDataSink(outputDataSink)
	return ds, nil
}

func getCustomSinkEndpoint(id int, env runtime.RuntimeEnvironment) (*customEndpoint, error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for sink endpoint with id=%d not found", id)
	}
	endpointCfg, ok := cfg.(*config.CustomEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("config for endpoint %q has invalid type", cfg.GetName())
	}
	dataSink, err := getCustomDataSink(endpointCfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", endpointCfg.GetName())
	}
	sinkEndpoint, err := runtime.MakeDataSinkEndpoint(dataSink, id, env)
	if err != nil {
		return nil, err
	}
	ep := &customEndpoint{
		DataSinkEndpoint: sinkEndpoint,
	}
	dataSink.AddEndpoint(ep)
	return ep, nil
}

func MakeCustomEndpointConsumer[HandlerState, T, R any](stream runtime.TypedSinkStream[T, R], handler EndpointHandler[HandlerState, T, R]) (runtime.Consumer[T], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := getCustomSinkEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	typedEndpointConsumer := &typedCustomEndpointConsumer[HandlerState, T, R]{
		DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T, R](endpoint, stream),
		handler:                  handler,
		tracer:                   tr,
	}
	stream.SetSinkConsumer(typedEndpointConsumer)
	endpoint.consumer = typedEndpointConsumer
	return typedEndpointConsumer, nil
}
