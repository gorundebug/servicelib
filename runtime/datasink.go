/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"gitlab.com/gorundebug/servicelib/runtime/config"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
	"golang.org/x/exp/maps"
)

type DataSink interface {
	DataConnector
	Start(context.Context) error
	Stop(context.Context)
	GetDataConnector() *config.DataConnectorConfig
	GetRuntime() StreamExecutionRuntime
	AddEndpoint(SinkEndpoint)
	GetEndpoint(id int) SinkEndpoint
	GetEndpoints() []SinkEndpoint
}

type SinkEndpoint interface {
	Endpoint
	GetConfig() *config.EndpointConfig
	GetRuntime() StreamExecutionRuntime
	GetDataSink() DataSink
	AddEndpointConsumer(consumer OutputEndpointConsumer)
	GetEndpointConsumers() []OutputEndpointConsumer
}

type OutputDataSink struct {
	dataConnector *config.DataConnectorConfig
	runtime       StreamExecutionRuntime
	endpoints     map[int]SinkEndpoint
}

func MakeOutputDataSink(dataConnector *config.DataConnectorConfig, runtime StreamExecutionRuntime) *OutputDataSink {
	return &OutputDataSink{
		dataConnector: dataConnector,
		runtime:       runtime,
		endpoints:     make(map[int]SinkEndpoint),
	}
}

func (ds *OutputDataSink) GetDataConnector() *config.DataConnectorConfig {
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
	config            *config.EndpointConfig
	runtime           StreamExecutionRuntime
	dataSink          DataSink
	endpointConsumers []OutputEndpointConsumer
}

func MakeDataSinkEndpoint(dataSink DataSink, config *config.EndpointConfig, runtime StreamExecutionRuntime) *DataSinkEndpoint {
	return &DataSinkEndpoint{
		dataSink:          dataSink,
		config:            config,
		runtime:           runtime,
		endpointConsumers: make([]OutputEndpointConsumer, 0),
	}
}

func (ep *DataSinkEndpoint) GetConfig() *config.EndpointConfig {
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

func (ep *DataSinkEndpoint) GetDataConnector() DataConnector {
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
	stream   TypedSinkStream[T]
	writer   TypedEndpointWriter[T]
}

func (ec *DataSinkEndpointConsumer[T]) Endpoint() SinkEndpoint {
	return ec.endpoint
}

func MakeDataSinkEndpointConsumer[T any](endpoint SinkEndpoint, stream TypedSinkStream[T]) *DataSinkEndpointConsumer[T] {
	ec := &DataSinkEndpointConsumer[T]{
		endpoint: endpoint,
		stream:   stream,
	}
	writer := endpoint.GetRuntime().GetEndpointReader(endpoint, stream, serde.GetSerdeType[T]())
	if writer != nil {
		ec.writer = writer.(TypedEndpointWriter[T])
	}
	return ec
}

func (ec *DataSinkEndpointConsumer[T]) GetEndpointWriter() TypedEndpointWriter[T] {
	return ec.writer
}
