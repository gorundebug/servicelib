/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	"io"
)

type DataSource interface {
	Start() error
	Stop()
	GetConfig() *DataSourceConfig
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	AddEndpoint(Endpoint)
	GetEndpoint(id int) Endpoint
}

type Endpoint interface {
	GetConfig() *EndpointConfig
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	GetDataSource() DataSource
	AddEndpointConsumer(consumer EndpointConsumer)
	GetEndpointConsumers() []EndpointConsumer
}

type InputDataSource struct {
	config    *DataSourceConfig
	runtime   StreamExecutionRuntime
	endpoints map[int]Endpoint
}

func MakeInputDataSource(config *DataSourceConfig, runtime StreamExecutionRuntime) *InputDataSource {
	return &InputDataSource{
		config:    config,
		runtime:   runtime,
		endpoints: make(map[int]Endpoint),
	}
}

func (ds *InputDataSource) GetConfig() *DataSourceConfig {
	return ds.config
}

func (ds *InputDataSource) GetName() string {
	return ds.config.Name
}

func (ds *InputDataSource) GetId() int {
	return ds.config.ID
}

func (ds *InputDataSource) GetRuntime() StreamExecutionRuntime {
	return ds.runtime
}

func (ds *InputDataSource) GetEndpoint(id int) Endpoint {
	return ds.endpoints[id]
}

func (ds *InputDataSource) AddEndpoint(endpoint Endpoint) {
	ds.endpoints[endpoint.GetId()] = endpoint
}

type EndpointRequestData interface {
	GetBody() (io.ReadCloser, error)
}

type EndpointConsumer interface {
	EndpointRequest(requestData EndpointRequestData)
}

type TypedEndpointConsumer[T any] interface {
	EndpointConsumer
	Consumer[T]
}

type DataSourceEndpointConsumer[T any] struct {
	inputStream InputTypedStream[T]
}

func (ds *DataSourceEndpointConsumer[T]) Consume(value T) {
	ds.inputStream.Consume(value)
}

func MakeDataSourceEndpointConsumer[T any](inputStream InputTypedStream[T]) *DataSourceEndpointConsumer[T] {
	return &DataSourceEndpointConsumer[T]{
		inputStream: inputStream,
	}
}

type DataSourceEndpoint struct {
	config           *EndpointConfig
	runtime          StreamExecutionRuntime
	dataSource       DataSource
	endpoinConsumers []EndpointConsumer
}

func MakeDataSourceEndpoint(dataSource DataSource, config *EndpointConfig, runtime StreamExecutionRuntime) *DataSourceEndpoint {
	return &DataSourceEndpoint{
		dataSource:       dataSource,
		config:           config,
		runtime:          runtime,
		endpoinConsumers: make([]EndpointConsumer, 0),
	}
}

func (ep *DataSourceEndpoint) GetConfig() *EndpointConfig {
	return ep.config
}

func (ep *DataSourceEndpoint) GetName() string {
	return ep.config.Name
}

func (ep *DataSourceEndpoint) GetId() int {
	return ep.config.ID
}

func (ep *DataSourceEndpoint) GetRuntime() StreamExecutionRuntime {
	return ep.runtime
}

func (ep *DataSourceEndpoint) GetDataSource() DataSource {
	return ep.dataSource
}

func (ep *DataSourceEndpoint) AddEndpointConsumer(endpointConsumer EndpointConsumer) {
	ep.endpoinConsumers = append(ep.endpoinConsumers, endpointConsumer)
}

func (ep *DataSourceEndpoint) GetEndpointConsumers() []EndpointConsumer {
	return ep.endpoinConsumers
}
