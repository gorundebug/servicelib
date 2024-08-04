/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsink

import (
	"gitlab.com/gorundebug/servicelib/runtime"
)

type DataConsumer[T any] interface {
	runtime.Consumer[T]
	Start() error
	Stop()
}

type CustomDataSink struct {
	*runtime.OutputDataSink
}

type CustomEndpoint struct {
	*runtime.DataSinkEndpoint
}

type CustomEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start() error
	Stop()
}

func (ds *CustomDataSink) Start() error {
	endpoints := ds.OutputDataSink.GetEndpoints()
	for _, endpoint := range endpoints {
		if err := endpoint.(*CustomEndpoint).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CustomDataSink) Stop() {
	endpoints := ds.OutputDataSink.GetEndpoints()
	for _, endpoint := range endpoints {
		endpoint.(*CustomEndpoint).Stop()
	}
}

func (ep *CustomEndpoint) Start() error {
	endpointConsumers := ep.GetEndpointConsumers()
	for _, endpointConsumer := range endpointConsumers {
		if err := endpointConsumer.(CustomEndpointConsumer).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (ep *CustomEndpoint) Stop() {
	endpointConsumers := ep.GetEndpointConsumers()
	for _, endpointConsumer := range endpointConsumers {
		endpointConsumer.(CustomEndpointConsumer).Stop()
	}
}

type TypedCustomEndpointConsumer[T any] struct {
	*runtime.DataSinkEndpointConsumer[T]
	dataConsumer DataConsumer[T]
}

func (ep *TypedCustomEndpointConsumer[T]) Consume(value T) {
	ep.dataConsumer.Consume(value)
}

func (ep *TypedCustomEndpointConsumer[T]) Start() error {
	if err := ep.dataConsumer.Start(); err != nil {
		return err
	}
	return nil
}

func (ep *TypedCustomEndpointConsumer[T]) Stop() {
	ep.dataConsumer.Stop()
}

func getCustomDataSink(id int, execRuntime runtime.StreamExecutionRuntime) *CustomDataSink {
	dataSink := execRuntime.GetDataSink(id)
	if dataSink != nil {
		return dataSink.(*CustomDataSink)
	}
	cfg := execRuntime.GetConfig().GetDataConnectorById(id)
	customDataSink := &CustomDataSink{
		OutputDataSink: runtime.MakeOutputDataSink(cfg, execRuntime),
	}
	execRuntime.AddDataSink(customDataSink)
	return customDataSink
}

func getCustomSinkEndpoint(id int, execRuntime runtime.StreamExecutionRuntime) *CustomEndpoint {
	cfg := execRuntime.GetConfig().GetEndpointConfigById(id)
	dataSink := getCustomDataSink(cfg.IdDataConnector, execRuntime)
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return endpoint.(*CustomEndpoint)
	}
	customEndpoint := &CustomEndpoint{
		DataSinkEndpoint: runtime.MakeDataSinkEndpoint(dataSink, cfg, execRuntime),
	}
	var sinkEndpoint runtime.SinkEndpoint = customEndpoint
	dataSink.AddEndpoint(sinkEndpoint)
	return customEndpoint
}

func MakeCustomEndpointSink[T any](stream runtime.SinkTypedStream[T], dataConsumer DataConsumer[T]) runtime.Consumer[T] {
	execRuntime := stream.GetRuntime()
	endpoint := getCustomSinkEndpoint(stream.GetEndpointId(), execRuntime)
	var consumer runtime.Consumer[T]
	var endpointConsumer runtime.OutputEndpointConsumer
	typedEndpointConsumer := &TypedCustomEndpointConsumer[T]{
		DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T](endpoint),
		dataConsumer:             dataConsumer,
	}
	endpointConsumer = typedEndpointConsumer
	consumer = typedEndpointConsumer
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
