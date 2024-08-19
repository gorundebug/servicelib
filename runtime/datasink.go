/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"golang.org/x/exp/maps"
)

type DataSink interface {
	Start() error
	Stop(context.Context)
	GetDataConnector() *DataConnector
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	AddEndpoint(SinkEndpoint)
	GetEndpoint(id int) SinkEndpoint
	GetEndpoints() []SinkEndpoint
}

type SinkEndpoint interface {
	GetConfig() *EndpointConfig
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	GetDataSink() DataSink
	AddEndpointConsumer(consumer OutputEndpointConsumer)
	GetEndpointConsumers() []OutputEndpointConsumer
}

type OutputDataSink struct {
	dataConnector *DataConnector
	runtime       StreamExecutionRuntime
	endpoints     map[int]SinkEndpoint
}

func MakeOutputDataSink(dataConnector *DataConnector, runtime StreamExecutionRuntime) *OutputDataSink {
	return &OutputDataSink{
		dataConnector: dataConnector,
		runtime:       runtime,
		endpoints:     make(map[int]SinkEndpoint),
	}
}

func (ds *OutputDataSink) GetDataConnector() *DataConnector {
	return ds.dataConnector
}

func (ds *OutputDataSink) GetName() string {
	return ds.dataConnector.Name
}

func (ds *OutputDataSink) GetId() int {
	return ds.dataConnector.Id
}

func (ds *OutputDataSink) GetRuntime() StreamExecutionRuntime {
	return ds.runtime
}

func (ds *OutputDataSink) GetEndpoint(id int) SinkEndpoint {
	return ds.endpoints[id]
}

func (ds *OutputDataSink) GetEndpoints() []SinkEndpoint {
	return maps.Values(ds.endpoints)
}

func (ds *OutputDataSink) AddEndpoint(endpoint SinkEndpoint) {
	ds.endpoints[endpoint.GetId()] = endpoint
}

type DataSinkEndpoint struct {
	config            *EndpointConfig
	runtime           StreamExecutionRuntime
	dataSink          DataSink
	endpointConsumers []OutputEndpointConsumer
}

func MakeDataSinkEndpoint(dataSink DataSink, config *EndpointConfig, runtime StreamExecutionRuntime) *DataSinkEndpoint {
	return &DataSinkEndpoint{
		dataSink:          dataSink,
		config:            config,
		runtime:           runtime,
		endpointConsumers: make([]OutputEndpointConsumer, 0),
	}
}

func (ep *DataSinkEndpoint) GetConfig() *EndpointConfig {
	return ep.config
}

func (ep *DataSinkEndpoint) GetName() string {
	return ep.config.Name
}

func (ep *DataSinkEndpoint) GetId() int {
	return ep.config.Id
}

func (ep *DataSinkEndpoint) GetRuntime() StreamExecutionRuntime {
	return ep.runtime
}

func (ep *DataSinkEndpoint) GetDataSink() DataSink {
	return ep.dataSink
}

func (ep *DataSinkEndpoint) AddEndpointConsumer(endpointConsumer OutputEndpointConsumer) {
	ep.endpointConsumers = append(ep.endpointConsumers, endpointConsumer)
}

func (ep *DataSinkEndpoint) GetEndpointConsumers() []OutputEndpointConsumer {
	return ep.endpointConsumers
}

type OutputEndpointConsumer interface {
	Endpoint() SinkEndpoint
}

type DataSinkEndpointConsumer[T any] struct {
	endpoint SinkEndpoint
}

func (ec *DataSinkEndpointConsumer[T]) Endpoint() SinkEndpoint {
	return ec.endpoint
}

func MakeDataSinkEndpointConsumer[T any](endpoint SinkEndpoint) *DataSinkEndpointConsumer[T] {
	return &DataSinkEndpointConsumer[T]{
		endpoint: endpoint,
	}
}
