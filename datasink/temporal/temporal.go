/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

// Package temporal submits ordinary sink values to a symmetric Temporal
// endpoint. A plain sink completes on durable acceptance; a sink-with-result
// waits for the endpoint Workflow result.
package temporal

import (
	"context"
	"fmt"
	"sync"

	datasourcetemporal "github.com/gorundebug/servicelib/datasource/temporal"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/serde"
)

// EndpointHandler owns only per-submission application identity/lifecycle.
// Serialization, Temporal submission, retry, and result transport stay in the
// framework adapter and do not alter the target graph node.
type EndpointHandler[HandlerState, T any] interface {
	GetMessageID(context.Context, T) string
	BeginRequest(context.Context, runtime.Stream) (context.Context, HandlerState)
	EndRequest(context.Context, runtime.Stream, error, HandlerState)
}

type directEndpointHandler[T any] struct{}

func (directEndpointHandler[T]) GetMessageID(ctx context.Context, _ T) string {
	if id, ok := runtime.StreamIdFromContext(ctx); ok {
		return id.GetID()
	}
	return ""
}

func (directEndpointHandler[T]) BeginRequest(
	ctx context.Context,
	_ runtime.Stream,
) (context.Context, struct{}) {
	return ctx, struct{}{}
}

func (directEndpointHandler[T]) EndRequest(
	context.Context,
	runtime.Stream,
	error,
	struct{},
) {
}

type outputDataSink struct {
	*runtime.OutputDataSink
	wg sync.WaitGroup
}

func (*outputDataSink) Start(context.Context) error { return nil }

func (ds *outputDataSink) Stop(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		ds.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

type sinkEndpoint struct{ *runtime.DataSinkEndpoint }

type endpointConsumer[HandlerState, T, R, E any] struct {
	endpoint    *sinkEndpoint
	stream      runtime.RuntimeStream
	handler     EndpointHandler[HandlerState, T]
	connector   *datasourcetemporal.Connector
	inputSerde  serde.StreamSerde[T]
	resultSerde serde.StreamSerde[R]
	waitResult  bool
	done        func(context.Context, T, R, error)
	sinkDone    runtime.SinkCallback[T]
	dataSink    *outputDataSink
	tracer      tracing.Tracer
}

func (ec *endpointConsumer[HandlerState, T, R, E]) Endpoint() runtime.SinkEndpoint {
	return ec.endpoint
}

func (ec *endpointConsumer[HandlerState, T, R, E]) GetID() int { return ec.endpoint.GetID() }

func (ec *endpointConsumer[HandlerState, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}

func (ec *endpointConsumer[HandlerState, T, R, E]) SetSinkCallback(callback runtime.SinkCallback[T]) {
	ec.sinkDone = callback
}

func (ec *endpointConsumer[HandlerState, T, R, E]) Consume(ctx context.Context, value T) {
	ec.dataSink.wg.Add(1)
	defer ec.dataSink.wg.Done()

	// Durable acceptance is part of consuming a Temporal sink value. In
	// particular, an upstream Temporal Activity must not finish (and cancel its
	// context) before the next Workflow has been accepted by the server. The
	// target graph still runs through its ordinary input consumer; only the
	// transport boundary is synchronous here.
	ec.submit(ctx, value)
}

func (ec *endpointConsumer[HandlerState, T, R, E]) submit(ctx context.Context, value T) {
	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "temporal.output",
			tracing.StringAttr("stream", ec.stream.Stream().GetName()),
			tracing.StringAttr("endpoint", ec.endpoint.GetName()),
		)
		defer span.End()
	}
	handlerCtx, state := ec.handler.BeginRequest(ctx, ec.stream.Stream())
	start := ec.endpoint.OnRequestStart(handlerCtx)
	var resultValue R
	var err error
	defer func() {
		ec.handler.EndRequest(handlerCtx, ec.stream.Stream(), err, state)
		ec.endpoint.OnRequestEnd(handlerCtx, start, err)
		if ec.done != nil {
			ec.done(handlerCtx, value, resultValue, err)
		}
		if ec.sinkDone != nil {
			ec.sinkDone.Done(handlerCtx, value, err)
		}
	}()

	payload, err := ec.inputSerde.Serialize(value)
	if err != nil {
		tracing.SpanError(span, err)
		return
	}
	messageID := ec.handler.GetMessageID(handlerCtx, value)
	if messageID == "" {
		messageID = runtime.NewStreamID()
	}
	streamID := messageID
	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		streamID = sid.GetID()
	}
	priority, _ := runtime.PriorityFromContext(handlerCtx)
	envelope := datasourcetemporal.EndpointEnvelope{
		Version: 1, EndpointID: ec.endpoint.GetID(), MessageID: messageID,
		StreamID: streamID, Priority: priority,
		Payload: payload,
	}
	if deadline, ok := handlerCtx.Deadline(); ok {
		envelope.DeadlineUnixNano = deadline.UTC().UnixNano()
	}
	result, submitErr := ec.connector.SubmitEndpoint(handlerCtx, ec.endpoint.GetID(), envelope, ec.waitResult)
	if submitErr != nil {
		err = submitErr
		tracing.SpanError(span, err)
		return
	}
	if ec.waitResult {
		resultValue, err = ec.resultSerde.Deserialize(result.Payload)
		if err != nil {
			tracing.SpanError(span, err)
		}
	}
}

func getOrCreateDataSink(id int, env runtime.RuntimeEnvironment) (*outputDataSink, *datasourcetemporal.Connector, error) {
	connector, err := datasourcetemporal.MakeConnector(id, env)
	if err != nil {
		return nil, nil, err
	}
	if existing := env.GetDataSink(id); existing != nil {
		ds, ok := existing.(*outputDataSink)
		if !ok {
			return nil, nil, fmt.Errorf("data sink id=%d is not a Go Temporal sink", id)
		}
		return ds, connector, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	base, err := runtime.MakeOutputDataSink(cfg, env)
	if err != nil {
		return nil, nil, err
	}
	ds := &outputDataSink{OutputDataSink: base}
	env.AddDataSink(ds)
	return ds, connector, nil
}

func createEndpoint(id int, env runtime.RuntimeEnvironment) (*sinkEndpoint, *outputDataSink, *datasourcetemporal.Connector, error) {
	configured := env.RuntimeConfig().GetEndpointConfigByID(id)
	cfg, ok := configured.(*config.TemporalEndpointConfig)
	if !ok {
		return nil, nil, nil, fmt.Errorf("endpoint id=%d is not Temporal", id)
	}
	ds, connector, err := getOrCreateDataSink(cfg.IdDataConnector, env)
	if err != nil {
		return nil, nil, nil, err
	}
	if ds.GetEndpoint(id) != nil {
		return nil, nil, nil, fmt.Errorf("Temporal sink endpoint %q already exists", cfg.Name)
	}
	base, err := runtime.MakeDataSinkEndpoint(ds, id, env)
	if err != nil {
		return nil, nil, nil, err
	}
	ep := &sinkEndpoint{DataSinkEndpoint: base}
	ds.AddEndpoint(ep)
	return ep, ds, connector, nil
}

func makeConsumer[HandlerState, T, R, E any](
	env runtime.RuntimeEnvironment,
	stream runtime.RuntimeStream,
	endpointID int,
	inputSerde serde.StreamSerde[T],
	resultSerde serde.StreamSerde[R],
	waitResult bool,
	handler EndpointHandler[HandlerState, T],
	done func(context.Context, T, R, error),
) (*endpointConsumer[HandlerState, T, R, E], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for Temporal sink stream %q", stream.Stream().GetName())
	}
	ep, ds, connector, err := createEndpoint(endpointID, env)
	if err != nil {
		return nil, err
	}
	consumer := &endpointConsumer[HandlerState, T, R, E]{
		endpoint: ep, stream: stream, handler: handler, connector: connector,
		inputSerde: inputSerde, resultSerde: resultSerde, waitResult: waitResult,
		done: done, dataSink: ds,
	}
	if tracer := env.Tracing(); tracer != nil {
		consumer.tracer = tracer.Tracer(env.ServiceConfig().Name)
	}
	env.RegisterEndpointConsumer(consumer)
	return consumer, nil
}

// MakeEndpointConsumer creates a submission-only Temporal sink.
func MakeEndpointConsumer[HandlerState, T, E any](
	stream runtime.TypedSinkStream[T, E],
	handler EndpointHandler[HandlerState, T],
) (runtime.Consumer[T], error) {
	consumer, err := makeConsumer[HandlerState, T, struct{}, E](
		stream.GetRuntimeEnvironment(), stream, stream.GetEndpointId(), runtime.MakeSerde[T](stream.GetRuntimeEnvironment()),
		nil, false, handler,
		func(ctx context.Context, _ T, _ struct{}, err error) {
			if err != nil {
				if value, ok := any(err).(E); ok {
					stream.GetErrorStream().Consume(ctx, value)
				}
			}
		},
	)
	if err != nil {
		return nil, err
	}
	stream.SetSinkConsumer(consumer)
	return consumer, nil
}

// MakeDirectEndpointConsumer creates the generated submission-only Temporal
// sink. Its stable execution identity is the existing stream ID; no endpoint
// business function is introduced by the transport.
func MakeDirectEndpointConsumer[T, E any](
	stream runtime.TypedSinkStream[T, E],
) (runtime.Consumer[T], error) {
	return MakeEndpointConsumer[struct{}](stream, directEndpointHandler[T]{})
}

// MakeEndpointConsumerWithResult creates a Temporal sink that waits for and
// emits the endpoint Workflow result.
func MakeEndpointConsumerWithResult[HandlerState, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	handler EndpointHandler[HandlerState, T],
) (runtime.Consumer[T], error) {
	env := stream.GetRuntimeEnvironment()
	consumer, err := makeConsumer[HandlerState, T, R, E](
		env, stream, stream.GetEndpointId(), runtime.MakeSerde[T](env), stream.GetSerde(), true, handler,
		func(ctx context.Context, _ T, result R, err error) {
			if err == nil {
				stream.ConsumeResult(ctx, result)
			} else if value, ok := any(err).(E); ok {
				stream.GetErrorStream().Consume(ctx, value)
			}
		},
	)
	if err != nil {
		return nil, err
	}
	stream.SetSinkConsumer(consumer)
	return consumer, nil
}

// MakeDirectEndpointConsumerWithResult creates the generated Temporal sink
// that waits for the existing endpoint result boundary.
func MakeDirectEndpointConsumerWithResult[T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
) (runtime.Consumer[T], error) {
	return MakeEndpointConsumerWithResult[struct{}](stream, directEndpointHandler[T]{})
}

func (ec *endpointConsumer[HandlerState, T, R, E]) Start(context.Context) error { return nil }
func (ec *endpointConsumer[HandlerState, T, R, E]) Stop(context.Context)        {}
