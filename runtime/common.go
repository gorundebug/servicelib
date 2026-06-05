/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"net/http"
	"reflect"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
)

type RuntimeEnvironment interface {
	environment.ServiceEnvironment

	ServiceInit() error
	Start(context.Context) error
	ReloadConfig()
	Stop(context.Context)
	Release()

	GetRuntime() ServiceExecutionRuntime

	MetricsEngine() metrics.MetricsEngine
	TracingEngine() tracing.TracingEngine
	HasCustomHTTPServer() bool

	GetTaskPool(name string) pool.TaskPool
	GetPriorityTaskPool(name string) pool.PriorityTaskPool

	AddDataSource(dataSource DataSource)
	GetDataSource(id int) DataSource
	AddDataSink(dataSink DataSink)
	GetDataSink(id int) DataSink

	Delay(ctx context.Context, duration time.Duration, f func()) error

	GetSerde(valueType reflect.Type) (serde.Serializer, error)
	RegisterHTTPHandler(path string, handler http.Handler)
	RegisterStream(stream RuntimeStream)
	RegisterEndpointConsumer(consumer RuntimeEndpointConsumer)
	RegisterStorage(storage store.Storage)
	CreateKeyValueJoinStorage(storageType api.JoinStorageType, cfg store.JoinStorageConfig, stream Stream) store.Storage
}

type DelayFunc[T any] func(T) error

type DataConnector interface {
	GetName() string
	GetID() int
}

type Endpoint interface {
	GetName() string
	GetID() int
	GetDataConnector() DataConnector
}

type Stream interface {
	GetName() string
	GetTransformationName() string
	GetTypeName() string
	GetID() int
	GetConfig() config.StreamConfig
	GetEnvironment() environment.ServiceEnvironment
}

type TypedSerializedStream[T any] interface {
	Stream
	GetSerde() serde.StreamSerde[T]
}

// RuntimeLinkInfo holds the resolved call semantics for a registered stream link.
type RuntimeLinkInfo struct {
	From          int
	To            int
	CallSemantics config.CallSemanticsConfig
}

type RuntimeStream interface {
	Build() error
	GetRuntimeEnvironment() RuntimeEnvironment
	Stream() Stream
	GetConsumers() []Stream
	FunctionImplementation() interface{}
	GetErrorConsumer() RuntimeStream
	GetValueType() reflect.Type
	GetKeyType() reflect.Type
}

type RuntimeEndpointConsumer interface {
	GetID() int
	FunctionImplementation() interface{}
}

type TypedSerializedRuntimeStream[T any] interface {
	RuntimeStream
	TypedSerializedStream[T]
}

type TypedStream[T any] interface {
	TypedSerializedRuntimeStream[T]
	GetConsumer() TypedStreamConsumer[T]
	SetConsumer(TypedStreamConsumer[T]) error
}

type Consumer[T any] interface {
	Consume(context.Context, T)
}

type SinkCallback[T any] interface {
	Done(context.Context, T, error)
}

type TypedConsumedStream[T any] interface {
	TypedStream[T]
	Consumer[T]
}

type TypedTransformConsumedStream[T, R any] interface {
	TypedStream[R]
	Consumer[T]
}

type TypedProcessConsumedStream[T, R, E any] interface {
	TypedTransformConsumedStream[T, R]
	GetErrorStream() TypedConsumedStream[E]
}

type TypedJoinConsumedStream[K comparable, T1, T2, R any] interface {
	TypedTransformConsumedStream[datastruct.KeyValue[K, T1], R]
	ConsumeRight(context.Context, datastruct.KeyValue[K, T2])
}

type TypedMultiJoinConsumedStream[K comparable, T, R any] interface {
	TypedTransformConsumedStream[datastruct.KeyValue[K, T], R]
	ConsumeRight(context.Context, int, datastruct.KeyValue[K, interface{}])
}

type TypedLinkStream[T any] interface {
	TypedConsumedStream[T]
	SetSource(TypedConsumedStream[T]) error
}

type TypedSplitStream[T any] interface {
	TypedConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type WhenStream interface {
	Stream
	ConsumeCase(ctx context.Context, value any)
	SetIndex(index int)
	GetWhenConsumer() Stream
	GetType() reflect.Type
}

type TypedWhenStream[T any] interface {
	TypedConsumedStream[T]
	WhenStream
}

type TypedCaseStream[T any] interface {
	TypedConsumedStream[T]
	AddStream(WhenStream) error
}

type BinaryConsumer interface {
	ConsumeBinary(context.Context, []byte, []byte) error
}

type TypedInputStream[T, R, E any] interface {
	TypedConsumedStream[T]
	GetErrorStream() TypedConsumedStream[E]
	GetEndpointId() int
	SetResultConsumer(resultConsumer Consumer[R])
	GetResultStream() TypedSerializedStream[R]
	SetSource(TypedStream[R]) error
}

type TypedSinkStream[T, E any] interface {
	RuntimeStream
	TypedStreamConsumer[T]
	GetErrorStream() TypedConsumedStream[E]
	SetSinkConsumer(Consumer[T])
	GetEndpointId() int
}

type TypedSinkStreamWithResult[T, R, E any] interface {
	TypedTransformConsumedStream[T, R]
	GetErrorStream() TypedConsumedStream[E]
	ConsumeResult(ctx context.Context, value R)
	SetSinkConsumer(Consumer[T])
	GetEndpointId() int
}

type TypedStreamConsumer[T any] interface {
	Stream
	Consumer[T]
}

type ConsumerFunc[T any] func(context.Context, T)

func (f ConsumerFunc[T]) Consume(ctx context.Context, value T) {
	f(ctx, value)
}

type ErrorType[T any] struct {
	Value T
	Error error
}

// StreamContext bundles the typed stream, result stream, output collector, and
// error collector for datasource endpoint handlers.
type StreamContext[T, R, E any] struct {
	Stream       TypedSerializedStream[T]
	ResultStream TypedSerializedStream[R]
	Logger       log.Logger

	collect      Collect[T]
	errorCollect Collect[E]
}

func (s *StreamContext[T, R, E]) Collect(ctx context.Context, value T) {
	s.collect.Out(ctx, value)
}

func (s *StreamContext[T, R, E]) ErrorCollect(ctx context.Context, value E) {
	s.errorCollect.Out(ctx, value)
}

// MakeStreamContext creates a StreamContext for datasource endpoint handlers.
func MakeStreamContext[T, R, E any](
	stream TypedSerializedStream[T],
	resultStream TypedSerializedStream[R],
	collect Collect[T],
	errorCollect Collect[E],
) StreamContext[T, R, E] {
	return StreamContext[T, R, E]{
		Stream:       stream,
		ResultStream: resultStream,
		Logger:       stream.GetEnvironment().Log(),
		collect:      collect,
		errorCollect: errorCollect,
	}
}

// SinkStreamContext bundles the result stream, output collector, and error collector
// for datasink endpoint handlers. The stream provides serde access for the result type R.
type SinkStreamContext[T, R, E any] struct {
	Stream TypedSerializedStream[R]
	Logger log.Logger

	collect      Collect[R]
	errorCollect Collect[E]
}

func (s *SinkStreamContext[T, R, E]) Collect(ctx context.Context, value R) {
	s.collect.Out(ctx, value)
}

func (s *SinkStreamContext[T, R, E]) ErrorCollect(ctx context.Context, value E) {
	s.errorCollect.Out(ctx, value)
}

func (s *SinkStreamContext[T, R, E]) Log() log.Logger {
	return s.Stream.GetEnvironment().Log()
}

// MakeSinkStreamContext creates a SinkStreamContext for datasink endpoint handlers.
func MakeSinkStreamContext[T, R, E any](
	stream TypedSerializedStream[R],
	collect Collect[R],
	errorCollect Collect[E],
) SinkStreamContext[T, R, E] {
	return SinkStreamContext[T, R, E]{
		Stream:       stream,
		Logger:       stream.GetEnvironment().Log(),
		collect:      collect,
		errorCollect: errorCollect,
	}
}
