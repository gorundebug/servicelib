/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsource

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime"
	"sync"
	"time"
)

const defaultShutdownTimeout = 30 * time.Second

type DataProducer[T any] interface {
	Start(consumer runtime.Consumer[T]) error
	Stop()
}

type CustomInputDataSource interface {
	runtime.DataSource
	WaitGroup() *sync.WaitGroup
}

type CustomDataSource struct {
	*runtime.InputDataSource
	wg   sync.WaitGroup
	stop bool
}

type CustomEndpoint struct {
	*runtime.DataSourceEndpoint
	delay time.Duration
}

type CustomInputEndpoint interface {
	runtime.InputEndpoint
	Start() error
	Stop()
	NextMessage()
}

type CustomEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start() error
	Stop()
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

func (ep *CustomEndpoint) NextMessage() {
	if ep.delay > 0 {
		time.Sleep(ep.delay)
	}
}

type TypedCustomEndpointConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	dataProducer DataProducer[T]
}

func (ep *TypedCustomEndpointConsumer[T]) Consume(value T) {
	ep.Endpoint().(CustomInputEndpoint).NextMessage()
	ep.DataSourceEndpointConsumer.Consume(value)
}

func (ep *TypedCustomEndpointConsumer[T]) Start() error {
	endpoint := ep.Endpoint()
	dataSource := endpoint.GetDataSource().(CustomInputDataSource)
	dataSource.WaitGroup().Add(1)
	go func() {
		defer dataSource.WaitGroup().Done()
		if err := ep.dataProducer.Start(ep); err != nil {
			log.Panicln(err)
		}
	}()
	return nil
}

func (ep *TypedCustomEndpointConsumer[T]) Stop() {
	ep.dataProducer.Stop()
}

func (ds *CustomDataSource) Start() error {
	endpoints := ds.InputDataSource.GetEndpoints()
	for _, endpoint := range endpoints {
		if err := endpoint.(CustomInputEndpoint).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CustomDataSource) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ds *CustomDataSource) Stop() {
	endpoints := ds.InputDataSource.GetEndpoints()
	for _, endpoint := range endpoints {
		ds.wg.Add(1)
		go func(endpoint CustomInputEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop()
		}(endpoint.(CustomInputEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-time.After(defaultShutdownTimeout):
		log.Warnf("Stop custom datasource '%s' after timeout.", ds.GetName())
	}
}

func getCustomDataSource(id int, execRuntime runtime.StreamExecutionRuntime) runtime.DataSource {
	dataSource := execRuntime.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := execRuntime.GetConfig().GetDataConnectorById(id)
	customDataSource := &CustomDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, execRuntime),
	}
	var inputDataSource CustomInputDataSource = customDataSource
	execRuntime.AddDataSource(inputDataSource)
	return customDataSource
}

func getCustomDataSourceEndpoint(id int, execRuntime runtime.StreamExecutionRuntime) runtime.InputEndpoint {
	cfg := execRuntime.GetConfig().GetEndpointConfigById(id)
	dataSource := getCustomDataSource(cfg.IdDataConnector, execRuntime)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	customEndpoint := &CustomEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, cfg, execRuntime),
		delay:              time.Duration(cfg.Properties["delay"].(int)) * time.Microsecond,
	}
	var inputEndpoint CustomInputEndpoint = customEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return customEndpoint
}

func MakeCustomEndpointConsumer[T any](stream runtime.TypedInputStream[T], dataProducer DataProducer[T]) runtime.Consumer[T] {
	execRuntime := stream.GetRuntime()
	endpoint := getCustomDataSourceEndpoint(stream.GetEndpointId(), execRuntime)
	var consumer runtime.Consumer[T]
	var endpointConsumer CustomEndpointConsumer
	typedEndpointConsumer := &TypedCustomEndpointConsumer[T]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
		dataProducer:               dataProducer,
	}
	endpointConsumer = typedEndpointConsumer
	consumer = typedEndpointConsumer
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
