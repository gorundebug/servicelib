/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
    "context"
    "reflect"
    "time"
)

type StreamExecutionEnvironment interface {
    GetSerde(valueType reflect.Type) (Serializer, error)
    GetConfig() *ServiceAppConfig
    GetServiceConfig() *ServiceConfig
    StreamsInit(ctx context.Context)
    SetConfig(config Config)
    Start(context.Context) error
    Stop(context.Context)
    AddDataSource(dataSource DataSource)
    GetDataSource(id int) DataSource
    AddDataSink(dataSink DataSink)
    GetDataSink(id int) DataSink
    GetConsumeTimeout(from int, to int) time.Duration
    GetEndpointReader(endpoint Endpoint, stream StreamBase, valueType reflect.Type) EndpointReader
    GetEndpointWriter(endpoint Endpoint, stream StreamBase, valueType reflect.Type) EndpointWriter
}

type Caller[T any] interface {
    Consume(value T)
}

type StreamExecutionRuntime interface {
    StreamExecutionEnvironment
    reloadConfig(Config)
    serviceInit(name string, runtime StreamExecutionRuntime, config Config)
    getSerde(valueType reflect.Type) (Serializer, error)
    registerStream(stream StreamBase)
    registerSerde(tp reflect.Type, serializer StreamSerializer)
    getRegisteredSerde(tp reflect.Type) StreamSerializer
}

type DataConnector interface {
    GetName() string
    GetId() int
}

type Endpoint interface {
    GetName() string
    GetId() int
    GetDataConnector() DataConnector
}
