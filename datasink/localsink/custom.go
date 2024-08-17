/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsink

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime"
	"sync"
	"time"
)

type DataConsumer[T any] interface {
	runtime.Consumer[T]
	Start() error
	Stop()
}

type CustomEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start() error
	Stop()
}

type CustomSinkEndpoint interface {
	runtime.SinkEndpoint
	Start() error
	Stop()
}

type CustomDataSink struct {
	*runtime.OutputDataSink
}

type CustomEndpoint struct {
	*runtime.DataSinkEndpoint
}

func (ds *CustomDataSink) Start() error {
	endpoints := ds.OutputDataSink.GetEndpoints()
	for _, endpoint := range endpoints {
		if err := endpoint.(CustomSinkEndpoint).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CustomDataSink) Stop() {
	endpoints := ds.OutputDataSink.GetEndpoints()
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(endpoint CustomSinkEndpoint) {
			defer wg.Done()
			endpoint.Stop()
		}(endpoint.(CustomSinkEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
	case <-time.After(time.Duration(ds.GetRuntime().GetServiceConfig().ShutdownTimeout) * time.Millisecond):
		log.Warnf("Stop custom datasink '%s' after timeout.", ds.GetName())
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

func getCustomDataSink(id int, execRuntime runtime.StreamExecutionRuntime) runtime.DataSink {
	dataSink := execRuntime.GetDataSink(id)
	if dataSink != nil {
		return dataSink
	}
	cfg := execRuntime.GetConfig().GetDataConnectorById(id)
	customDataSink := &CustomDataSink{
		OutputDataSink: runtime.MakeOutputDataSink(cfg, execRuntime),
	}
	var outputDataSink runtime.DataSink = customDataSink
	execRuntime.AddDataSink(outputDataSink)
	return customDataSink
}

func getCustomSinkEndpoint(id int, execRuntime runtime.StreamExecutionRuntime) runtime.SinkEndpoint {
	cfg := execRuntime.GetConfig().GetEndpointConfigById(id)
	dataSink := getCustomDataSink(cfg.IdDataConnector, execRuntime)
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	customEndpoint := &CustomEndpoint{
		DataSinkEndpoint: runtime.MakeDataSinkEndpoint(dataSink, cfg, execRuntime),
	}
	var sinkEndpoint CustomSinkEndpoint = customEndpoint
	dataSink.AddEndpoint(sinkEndpoint)
	return customEndpoint
}

func MakeCustomEndpointSink[T any](stream runtime.TypedSinkStream[T], dataConsumer DataConsumer[T]) runtime.Consumer[T] {
	execRuntime := stream.GetRuntime()
	endpoint := getCustomSinkEndpoint(stream.GetEndpointId(), execRuntime)
	var consumer runtime.Consumer[T]
	var endpointConsumer CustomEndpointConsumer
	typedEndpointConsumer := &TypedCustomEndpointConsumer[T]{
		DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T](endpoint),
		dataConsumer:             dataConsumer,
	}
	endpointConsumer = typedEndpointConsumer
	consumer = typedEndpointConsumer
	stream.SetConsumer(typedEndpointConsumer)
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
