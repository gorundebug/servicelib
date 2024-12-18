/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/config"
	"golang.org/x/exp/maps"
)

type DataSource interface {
	DataConnector
	Start(context.Context) error
	Stop(context.Context)
	GetDataConnector() *config.DataConnectorConfig
	GetEnvironment() ServiceExecutionEnvironment
	AddEndpoint(InputEndpoint)
	GetEndpoint(id int) InputEndpoint
	GetEndpoints() Collection[InputEndpoint]
}

type InputEndpoint interface {
	Endpoint
	GetConfig() *config.EndpointConfig
	GetEnvironment() ServiceExecutionEnvironment
	GetDataSource() DataSource
	AddEndpointConsumer(consumer InputEndpointConsumer)
	GetEndpointConsumers() Collection[InputEndpointConsumer]
}

type InputEndpointConsumer interface {
	Endpoint() InputEndpoint
}

type InputDataSource struct {
	id          int
	environment ServiceExecutionEnvironment
	endpoints   map[int]InputEndpoint
}

func MakeInputDataSource(dataConnector *config.DataConnectorConfig, environment ServiceExecutionEnvironment) *InputDataSource {
	return &InputDataSource{
		id:          dataConnector.Id,
		environment: environment,
		endpoints:   make(map[int]InputEndpoint),
	}
}

func (ds *InputDataSource) GetDataConnector() *config.DataConnectorConfig {
	return ds.environment.AppConfig().GetDataConnectorById(ds.id)
}

func (ds *InputDataSource) GetName() string {
	return ds.GetDataConnector().Name
}

func (ds *InputDataSource) GetId() int {
	return ds.id
}

func (ds *InputDataSource) GetEnvironment() ServiceExecutionEnvironment {
	return ds.environment
}

func (ds *InputDataSource) GetEndpoint(id int) InputEndpoint {
	return ds.endpoints[id]
}

func (ds *InputDataSource) GetEndpoints() Collection[InputEndpoint] {
	return NewCollection(maps.Values(ds.endpoints))
}

func (ds *InputDataSource) AddEndpoint(endpoint InputEndpoint) {
	ds.endpoints[endpoint.GetId()] = endpoint
}

type DataSourceEndpoint struct {
	id                int
	environment       ServiceExecutionEnvironment
	dataSource        DataSource
	endpointConsumers []InputEndpointConsumer
}

func MakeDataSourceEndpoint(dataSource DataSource, id int, environment ServiceExecutionEnvironment) *DataSourceEndpoint {
	return &DataSourceEndpoint{
		dataSource:        dataSource,
		id:                id,
		environment:       environment,
		endpointConsumers: make([]InputEndpointConsumer, 0),
	}
}

func (ep *DataSourceEndpoint) GetConfig() *config.EndpointConfig {
	return ep.environment.AppConfig().GetEndpointConfigById(ep.id)
}

func (ep *DataSourceEndpoint) GetName() string {
	return ep.GetConfig().Name
}

func (ep *DataSourceEndpoint) GetId() int {
	return ep.GetConfig().Id
}

func (ep *DataSourceEndpoint) GetEnvironment() ServiceExecutionEnvironment {
	return ep.environment
}

func (ep *DataSourceEndpoint) GetDataSource() DataSource {
	return ep.dataSource
}

func (ep *DataSourceEndpoint) GetDataConnector() DataConnector {
	return ep.dataSource
}

func (ep *DataSourceEndpoint) AddEndpointConsumer(endpointConsumer InputEndpointConsumer) {
	ep.endpointConsumers = append(ep.endpointConsumers, endpointConsumer)
}

func (ep *DataSourceEndpoint) GetEndpointConsumers() Collection[InputEndpointConsumer] {
	return NewCollection(ep.endpointConsumers)
}

type DataSourceEndpointConsumer[T any] struct {
	inputStream TypedInputStream[T]
	endpoint    InputEndpoint
}

func (ec *DataSourceEndpointConsumer[T]) Consume(value T) {
	ec.inputStream.Consume(value)
}

func (ec *DataSourceEndpointConsumer[T]) Endpoint() InputEndpoint {
	return ec.endpoint
}

func (ec *DataSourceEndpointConsumer[T]) Stream() TypedInputStream[T] {
	return ec.inputStream
}

func MakeDataSourceEndpointConsumer[T any](endpoint InputEndpoint, inputStream TypedInputStream[T]) *DataSourceEndpointConsumer[T] {
	ec := &DataSourceEndpointConsumer[T]{
		inputStream: inputStream,
		endpoint:    endpoint,
	}
	return ec
}
