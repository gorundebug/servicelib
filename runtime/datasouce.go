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
	"github.com/gorundebug/servicelib/runtime/serde"
	"golang.org/x/exp/maps"
)

type DataSource interface {
	DataConnector
	Start(context.Context) error
	Stop(context.Context)
	GetDataConnector() *config.DataConnectorConfig
	GetRuntime() StreamExecutionRuntime
	AddEndpoint(InputEndpoint)
	GetEndpoint(id int) InputEndpoint
	GetEndpoints() []InputEndpoint
}

type InputEndpoint interface {
	Endpoint
	GetConfig() *config.EndpointConfig
	GetRuntime() StreamExecutionRuntime
	GetDataSource() DataSource
	AddEndpointConsumer(consumer InputEndpointConsumer)
	GetEndpointConsumers() []InputEndpointConsumer
}

type InputEndpointConsumer interface {
	Endpoint() InputEndpoint
}

type InputDataSource struct {
	dataConnector *config.DataConnectorConfig
	runtime       StreamExecutionRuntime
	endpoints     map[int]InputEndpoint
}

func MakeInputDataSource(dataConnector *config.DataConnectorConfig, runtime StreamExecutionRuntime) *InputDataSource {
	return &InputDataSource{
		dataConnector: dataConnector,
		runtime:       runtime,
		endpoints:     make(map[int]InputEndpoint),
	}
}

func (ds *InputDataSource) GetDataConnector() *config.DataConnectorConfig {
	return ds.dataConnector
}

func (ds *InputDataSource) GetName() string {
	return ds.dataConnector.Name
}

func (ds *InputDataSource) GetId() int {
	return ds.dataConnector.Id
}

func (ds *InputDataSource) GetRuntime() StreamExecutionRuntime {
	return ds.runtime
}

func (ds *InputDataSource) GetEndpoint(id int) InputEndpoint {
	return ds.endpoints[id]
}

func (ds *InputDataSource) GetEndpoints() []InputEndpoint {
	return maps.Values(ds.endpoints)
}

func (ds *InputDataSource) AddEndpoint(endpoint InputEndpoint) {
	ds.endpoints[endpoint.GetId()] = endpoint
}

type DataSourceEndpoint struct {
	config            *config.EndpointConfig
	runtime           StreamExecutionRuntime
	dataSource        DataSource
	endpointConsumers []InputEndpointConsumer
}

func MakeDataSourceEndpoint(dataSource DataSource, config *config.EndpointConfig, runtime StreamExecutionRuntime) *DataSourceEndpoint {
	return &DataSourceEndpoint{
		dataSource:        dataSource,
		config:            config,
		runtime:           runtime,
		endpointConsumers: make([]InputEndpointConsumer, 0),
	}
}

func (ep *DataSourceEndpoint) GetConfig() *config.EndpointConfig {
	return ep.config
}

func (ep *DataSourceEndpoint) GetName() string {
	return ep.config.Name
}

func (ep *DataSourceEndpoint) GetId() int {
	return ep.config.Id
}

func (ep *DataSourceEndpoint) GetRuntime() StreamExecutionRuntime {
	return ep.runtime
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

func (ep *DataSourceEndpoint) GetEndpointConsumers() []InputEndpointConsumer {
	return ep.endpointConsumers
}

type DataSourceEndpointConsumer[T any] struct {
	inputStream TypedInputStream[T]
	endpoint    InputEndpoint
	reader      TypedEndpointReader[T]
}

func (ec *DataSourceEndpointConsumer[T]) Consume(value T) {
	ec.inputStream.Consume(value)
}

func (ec *DataSourceEndpointConsumer[T]) Endpoint() InputEndpoint {
	return ec.endpoint
}

func MakeDataSourceEndpointConsumer[T any](endpoint InputEndpoint, inputStream TypedInputStream[T]) *DataSourceEndpointConsumer[T] {
	ec := &DataSourceEndpointConsumer[T]{
		inputStream: inputStream,
		endpoint:    endpoint,
	}
	reader := endpoint.GetRuntime().GetEndpointReader(endpoint, inputStream, serde.GetSerdeType[T]())
	if reader != nil {
		ec.reader = reader.(TypedEndpointReader[T])
	}
	return ec
}

func (ec *DataSourceEndpointConsumer[T]) GetEndpointReader() TypedEndpointReader[T] {
	return ec.reader
}
