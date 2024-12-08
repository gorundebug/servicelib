/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsink

import (
	"context"
	"github.com/gorundebug/servicelib/runtime"
	"sync"
)

type DataConsumer[T any] interface {
	runtime.SinkConsumer[T]
	Start(context.Context) error
	Stop(context.Context)
}

type CustomOutputDataSink interface {
	runtime.DataSink
	WaitGroup() *sync.WaitGroup
}

type CustomEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type CustomSinkEndpoint interface {
	runtime.SinkEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type CustomDataSink struct {
	*runtime.OutputDataSink
	wg sync.WaitGroup
}

type CustomEndpoint struct {
	*runtime.DataSinkEndpoint
}

func (ds *CustomDataSink) Start(ctx context.Context) error {
	endpoints := ds.OutputDataSink.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(CustomSinkEndpoint).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CustomDataSink) Stop(ctx context.Context) {
	endpoints := ds.OutputDataSink.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint CustomSinkEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(CustomSinkEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf("Stop custom data sink %q after timeout.", ds.GetName())
	}
}

func (ds *CustomDataSink) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ep *CustomEndpoint) Start(ctx context.Context) error {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		if err := endpointConsumers.At(i).(CustomEndpointConsumer).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ep *CustomEndpoint) Stop(ctx context.Context) {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		endpointConsumers.At(i).(CustomEndpointConsumer).Stop(ctx)
	}
}

type TypedCustomEndpointConsumer[T, R any] struct {
	*runtime.DataSinkEndpointConsumer[T, R]
	dataConsumer DataConsumer[T]
}

func (ep *TypedCustomEndpointConsumer[T, R]) Consume(value T) error {
	return ep.dataConsumer.Consume(value)
}

func (ep *TypedCustomEndpointConsumer[T, R]) Start(ctx context.Context) error {
	if err := ep.dataConsumer.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (ep *TypedCustomEndpointConsumer[T, R]) Stop(ctx context.Context) {
	endpoint := ep.Endpoint()
	dataSink := endpoint.GetDataSink().(CustomOutputDataSink)
	dataSink.WaitGroup().Add(1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		ep.dataConsumer.Stop(ctx)
	}()
	go func() {
		defer dataSink.WaitGroup().Done()
		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()
		select {
		case <-c:
		case <-ctx.Done():
			dataSink.GetEnvironment().Log().Warnf(
				"Custom data sink endpoint %q for the stream %q stopped by timeout.",
				endpoint.GetName(),
				ep.Stream().GetName())
		}
	}()
}

func getCustomDataSink(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSink {
	dataSink := env.GetDataSink(id)
	if dataSink != nil {
		return dataSink
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
	customDataSink := &CustomDataSink{
		OutputDataSink: runtime.MakeOutputDataSink(cfg, env),
	}
	var outputDataSink CustomOutputDataSink = customDataSink
	env.AddDataSink(outputDataSink)
	return customDataSink
}

func getCustomSinkEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.SinkEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	dataSink := getCustomDataSink(cfg.IdDataConnector, env)
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	customEndpoint := &CustomEndpoint{
		DataSinkEndpoint: runtime.MakeDataSinkEndpoint(dataSink, cfg, env),
	}
	var sinkEndpoint CustomSinkEndpoint = customEndpoint
	dataSink.AddEndpoint(sinkEndpoint)
	return customEndpoint
}

func MakeCustomEndpointSink[T, R any](stream runtime.TypedSinkStream[T, R], dataConsumer DataConsumer[T]) runtime.SinkConsumer[T] {
	env := stream.GetEnvironment()
	endpoint := getCustomSinkEndpoint(stream.GetEndpointId(), env)
	var consumer runtime.SinkConsumer[T]
	var endpointConsumer CustomEndpointConsumer
	typedEndpointConsumer := &TypedCustomEndpointConsumer[T, R]{
		DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T, R](endpoint, stream),
		dataConsumer:             dataConsumer,
	}
	endpointConsumer = typedEndpointConsumer
	consumer = typedEndpointConsumer
	stream.SetSinkConsumer(typedEndpointConsumer)
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
