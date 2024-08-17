/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"reflect"
)

type StreamExecutionEnvironment interface {
	GetSerde(valueType reflect.Type) (Serializer, error)
	GetConfig() *ServiceAppConfig
	GetServiceConfig() *ServiceConfig
	ServiceInit(config Config)
	ConfigReload(config Config)
	Start() error
	Stop()
	AddDataSource(dataSource DataSource)
	GetDataSource(id int) DataSource
	AddDataSink(dataSink DataSink)
	GetDataSink(id int) DataSink
}

type Caller[T any] interface {
	Consume(value T)
}

type StreamExecutionRuntime interface {
	StreamExecutionEnvironment
	configReload(Config)
	streamsInit(name string, runtime StreamExecutionRuntime, config Config)
	getSerde(valueType reflect.Type) (Serializer, error)
	registerStream(stream StreamBase)
	registerSerde(tp reflect.Type, serializer StreamSerializer)
	getRegisteredSerde(tp reflect.Type) StreamSerializer
}
