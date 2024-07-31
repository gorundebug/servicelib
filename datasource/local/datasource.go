/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package local

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime"
	"sync"
	"time"
)

const defaultShutdownTimeout = 30 * time.Second

type DataProvider[T any] interface {
	GetNextMessage() (T, bool)
}

type CustomDataSource struct {
	*runtime.InputDataSource
	wg   sync.WaitGroup
	stop bool
}

type CustomInputEndpoint interface {
	runtime.InputEndpoint
	NextMessage() bool
}

type CustomEndpoint struct {
	*runtime.DataSourceEndpoint
	timeout time.Duration
}

type CustomTypedEndpoint[T any] struct {
	*CustomEndpoint
	dataProvider DataProvider[T]
}

func (ep *CustomTypedEndpoint[T]) NextMessage() bool {
	msg, done := ep.dataProvider.GetNextMessage()
	if done {
		return false
	}
	endpointConsumers := ep.GetEndpointConsumers()
	requestData := CustomEndpointRequestData[T]{Msg: msg}
	for _, endpointConsumer := range endpointConsumers {
		endpointConsumer.EndpointRequest(&requestData)
	}
	if ep.timeout > 0 {
		time.Sleep(ep.timeout)
	}
	return true
}

func (ds *CustomDataSource) Start() error {
	endpoints := ds.InputDataSource.GetEndpoints()
	for _, endpoint := range endpoints {
		ds.wg.Add(1)
		go func(endpoint CustomInputEndpoint) {
			defer ds.wg.Done()
			for {
				if ds.stop || !endpoint.NextMessage() {
					break
				}
			}
		}(endpoint.(CustomInputEndpoint))
	}
	return nil
}

func (ds *CustomDataSource) Stop() {
	ds.stop = true
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

type CustomEndpointRequestData[T any] struct {
	Msg T
}

type CustomEndpointTypedConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	endpoint *CustomTypedEndpoint[T]
}

func (ec *CustomEndpointTypedConsumer[T]) EndpointRequest(requestData runtime.EndpointRequestData) {
	customEndpointRequestData := requestData.(*CustomEndpointRequestData[T])
	ec.Consume(customEndpointRequestData.Msg)
}

func getCustomDataSource(id int, execRuntime runtime.StreamExecutionRuntime) *CustomDataSource {
	dataSource := execRuntime.GetDataSource(id)
	if dataSource != nil {
		return dataSource.(*CustomDataSource)
	}
	cfg := execRuntime.GetConfig().GetDataConnectorById(id)
	customDataSource := &CustomDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, execRuntime),
		stop:            false,
	}
	execRuntime.AddDataSource(customDataSource)
	return customDataSource
}

func getCustomDataSourceEndpoint[T any](id int, execRuntime runtime.StreamExecutionRuntime, dataProvider DataProvider[T]) *CustomTypedEndpoint[T] {
	cfg := execRuntime.GetConfig().GetEndpointConfigById(id)
	dataSource := getCustomDataSource(cfg.IdDataConnector, execRuntime)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint.(*CustomTypedEndpoint[T])
	}
	customEndpoint := &CustomTypedEndpoint[T]{
		CustomEndpoint: &CustomEndpoint{
			DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, cfg, execRuntime),
			timeout:            time.Duration(runtime.GetConfigProperty[int](cfg, "timeout")) * time.Millisecond,
		},
		dataProvider: dataProvider,
	}
	var customInputEndpoint CustomInputEndpoint = customEndpoint
	dataSource.AddEndpoint(customInputEndpoint)
	return customEndpoint
}

func MakeCustomEndpointConsumer[T any](stream runtime.InputTypedStream[T], dataProvider DataProvider[T]) runtime.TypedEndpointConsumer[T] {
	execRuntime := stream.GetRuntime()
	endpoint := getCustomDataSourceEndpoint[T](stream.GetEndpointId(), execRuntime, dataProvider)
	var endpointConsumer runtime.TypedEndpointConsumer[T]
	endpointConsumer = &CustomEndpointTypedConsumer[T]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](stream),
		endpoint:                   endpoint,
	}
	endpoint.AddEndpointConsumer(endpointConsumer)
	return endpointConsumer
}
