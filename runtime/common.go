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
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/httproute"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"io"
	"reflect"
	"time"
)

type ServiceExecutionEnvironment interface {
	environment.ServiceEnvironment
	GetSerde(valueType reflect.Type) (serde.Serializer, error)
	ServiceInit() error
	StreamsInit(ctx context.Context)
	SetConfig(config config.Config)
	Start(context.Context) error
	Stop(context.Context)
	AddDataSource(dataSource DataSource)
	GetDataSource(id int) DataSource
	GetTaskPool(name string) pool.TaskPool
	GetPriorityTaskPool(name string) pool.PriorityTaskPool
	AddDataSink(dataSink DataSink)
	GetDataSink(id int) DataSink
	GetConsumeTimeout(from int, to int) time.Duration
	GetEndpointReader(endpoint Endpoint, stream Stream, valueType reflect.Type) EndpointReader
	GetEndpointWriter(endpoint Endpoint, stream Stream, valueType reflect.Type) EndpointWriter
	Delay(duration time.Duration, f func())
	GetRuntime() ServiceExecutionRuntime
	Release()
	GetHttpRoute() httproute.HttpRoute
}

type DelayFunc[T any] func(T) error

type DataConnector interface {
	GetName() string
	GetId() int
}

type Endpoint interface {
	GetName() string
	GetId() int
	GetDataConnector() DataConnector
}

type EndpointReader interface {
}

type EndpointWriter interface {
}

type TypedEndpointReader[T any] interface {
	EndpointReader
	Read(io.Reader) (T, error)
}

type TypedEndpointWriter[T any] interface {
	EndpointWriter
	Write(T, io.Writer) error
}

type Stream interface {
	GetName() string
	GetTransformationName() string
	GetTypeName() string
	GetId() int
	GetConfig() *config.StreamConfig
	GetEnvironment() ServiceExecutionEnvironment
	GetConsumers() []Stream
	Validate() error
}

type TypedStream[T any] interface {
	Stream
	GetConsumer() TypedStreamConsumer[T]
	GetSerde() serde.StreamSerde[T]
	SetConsumer(TypedStreamConsumer[T])
}

type Consumer[T any] interface {
	Consume(T)
}

type SinkCallback[T any] interface {
	Done(T, error)
}

type SinkConsumer[T any] interface {
	Consumer[T]
	SetSinkCallback(SinkCallback[T])
}

type TypedConsumedStream[T any] interface {
	TypedStream[T]
	Consumer[T]
}

type TypedTransformConsumedStream[T, R any] interface {
	TypedStream[R]
	Consumer[T]
}

type TypedJoinConsumedStream[K comparable, T1, T2, R any] interface {
	TypedTransformConsumedStream[datastruct.KeyValue[K, T1], R]
	ConsumeRight(datastruct.KeyValue[K, T2])
}

type TypedMultiJoinConsumedStream[K comparable, T, R any] interface {
	TypedTransformConsumedStream[datastruct.KeyValue[K, T], R]
	ConsumeRight(int, datastruct.KeyValue[K, interface{}])
}

type TypedLinkStream[T any] interface {
	TypedConsumedStream[T]
	SetSource(TypedConsumedStream[T])
}

type TypedSplitStream[T any] interface {
	TypedConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedBinarySplitStream[T any] interface {
	TypedBinaryConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedBinaryKVSplitStream[T any] interface {
	TypedBinaryKVConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedInputStream[T any] interface {
	TypedConsumedStream[T]
	GetEndpointId() int
}

type TypedSinkStream[T, R any] interface {
	TypedTransformConsumedStream[T, R]
	GetEndpointId() int
	SetSinkConsumer(SinkConsumer[T])
}

type BinaryConsumer interface {
	ConsumeBinary([]byte)
}

type BinaryKVConsumer interface {
	ConsumeBinary([]byte, []byte)
}

type TypedBinaryConsumedStream[T any] interface {
	TypedConsumedStream[T]
	BinaryConsumer
}

type TypedBinaryKVConsumedStream[T any] interface {
	TypedConsumedStream[T]
	BinaryKVConsumer
}

type TypedStreamConsumer[T any] interface {
	Stream
	Consumer[T]
}

type ConsumerFunc[T any] func(T) error

func (f ConsumerFunc[T]) Consume(value T) error {
	return f(value)
}

type BinaryConsumerFunc func([]byte) error

func (f BinaryConsumerFunc) Consume(data []byte) error {
	return f(data)
}

type BinaryKVConsumerFunc func([]byte, []byte) error

func (f BinaryKVConsumerFunc) Consume(key []byte, value []byte) error {
	return f(key, value)
}
