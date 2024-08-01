/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import "golang.org/x/exp/maps"

type DataSource interface {
	Start() error
	Stop()
	GetDataConnector() *DataConnector
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	AddEndpoint(InputEndpoint)
	GetEndpoint(id int) []InputEndpoint
	GetEndpoints() [][]InputEndpoint
}

type InputEndpoint interface {
	GetConfig() *EndpointConfig
	GetName() string
	GetId() int
	GetRuntime() StreamExecutionRuntime
	GetDataSource() DataSource
	AddEndpointConsumer(consumer EndpointConsumer)
	GetEndpointConsumers() []EndpointConsumer
}

type InputDataSource struct {
	dataConnector *DataConnector
	runtime       StreamExecutionRuntime
	endpoints     map[int][]InputEndpoint
}

func MakeInputDataSource(dataConnector *DataConnector, runtime StreamExecutionRuntime) *InputDataSource {
	return &InputDataSource{
		dataConnector: dataConnector,
		runtime:       runtime,
		endpoints:     make(map[int][]InputEndpoint),
	}
}

func (ds *InputDataSource) GetDataConnector() *DataConnector {
	return ds.dataConnector
}

func (ds *InputDataSource) GetName() string {
	return ds.dataConnector.Name
}

func (ds *InputDataSource) GetId() int {
	return ds.dataConnector.ID
}

func (ds *InputDataSource) GetRuntime() StreamExecutionRuntime {
	return ds.runtime
}

func (ds *InputDataSource) GetEndpoint(id int) []InputEndpoint {
	return ds.endpoints[id]
}

func (ds *InputDataSource) GetEndpoints() [][]InputEndpoint {
	return maps.Values(ds.endpoints)
}

func (ds *InputDataSource) AddEndpoint(endpoint InputEndpoint) {
	ds.endpoints[endpoint.GetId()] = append(ds.endpoints[endpoint.GetId()], endpoint)
}

type EndpointRequestData interface {
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
