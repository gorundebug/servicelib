/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package transformation

import (
	"gitlab.com/gorundebug/servicelib/runtime"
)

func Map[T, R any](name string, stream runtime.TypedStream[T], f runtime.MapFunction[T, R]) runtime.TypedTransformConsumedStream[T, R] {
	return runtime.MakeMapStream[T, R](name, stream, f)
}

func AppSink[T any](name string, stream runtime.TypedStream[T], consumer runtime.Consumer[T]) runtime.TypedStreamConsumer[T] {
	return runtime.MakeAppSinkStream[T](name, stream, consumer)
}

func Filter[T any](name string, stream runtime.TypedStream[T], f runtime.FilterFunction[T]) runtime.TypedConsumedStream[T] {
	return runtime.MakeFilterStream[T](name, stream, f)
}

func FlatMap[T, R any](name string, stream runtime.TypedStream[T], f runtime.FlatMapFunction[T, R]) runtime.TypedTransformConsumedStream[T, R] {
	return runtime.MakeFlatMapStream[T, R](name, stream, f)
}

func FlatMapIterable[T, R any](name string, stream runtime.TypedStream[T]) runtime.TypedTransformConsumedStream[T, R] {
	return runtime.MakeFlatMapIterableStream[T, R](name, stream)
}

func ForEach[T any](name string, stream runtime.TypedStream[T], f runtime.ForEachFunction[T]) runtime.TypedConsumedStream[T] {
	return runtime.MakeForEachStream[T](name, stream, f)
}

func Input[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedInputStream[T] {
	return runtime.MakeInputStream[T](name, streamExecutionRuntime)
}

func Join[K comparable, T1, T2, R any](name string, stream runtime.TypedStream[runtime.KeyValue[K, T1]],
	streamRight runtime.TypedStream[runtime.KeyValue[K, T2]],
	f runtime.JoinFunction[K, T1, T2, R]) runtime.TypedJoinConsumedStream[K, T1, T2, R] {
	return runtime.MakeJoinStream(name, stream, streamRight, f)
}

func KeyBy[T any, K comparable, V any](name string, stream runtime.TypedStream[T], f runtime.KeyByFunction[T, K, V]) runtime.TypedTransformConsumedStream[T, runtime.KeyValue[K, V]] {
	return runtime.MakeKeyByStream[T, K, V](name, stream, f)
}

func Link[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedLinkStream[T] {
	return runtime.MakeLinkStream[T](name, streamExecutionRuntime)
}

func Merge[T any](name string, streams ...runtime.TypedStream[T]) runtime.TypedConsumedStream[T] {
	return runtime.MakeMergeStream[T](name, streams...)
}

func MultiJoin[K comparable, T, R any](
	name string, leftStream runtime.TypedStream[runtime.KeyValue[K, T]],
	f runtime.MultiJoinFunction[K, T, R]) runtime.TypedMultiJoinConsumedStream[K, T, R] {
	return runtime.MakeMultiJoinStream[K, T, R](name, leftStream, f)
}

func MultiJoinLink[K comparable, T1, T2, R any](
	multiJoin runtime.TypedStream[R],
	stream runtime.TypedStream[runtime.KeyValue[K, T2]]) {
	runtime.MakeMultiJoinLink[K, T1, T2, R](multiJoin, stream)
}

func Parallels[T, R any](name string, stream runtime.TypedStream[T], f runtime.ParallelsFunction[T, R]) runtime.TypedTransformConsumedStream[T, R] {
	return runtime.MakeParallelsStream[T, R](name, stream, f)
}

func Sink[T any](name string, stream runtime.TypedStream[T]) runtime.TypedSinkStream[T] {
	return runtime.MakeSinkStream[T](name, stream)
}

func Split[T any](name string, stream runtime.TypedStream[T]) runtime.TypedSplitStream[T] {
	return runtime.MakeSplitStream[T](name, stream)
}

func SplitInStub[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedBinaryConsumedStream[T] {
	return runtime.MakeInputSplitStream[T](name, streamExecutionRuntime)
}

func InStub[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedBinaryConsumedStream[T] {
	return runtime.MakeInStubStream[T](name, streamExecutionRuntime)
}

func SplitInStubKV[K comparable, V any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedBinaryKVSplitStream[runtime.KeyValue[K, V]] {
	runtime.RegisterKeyValueSerde[K, V](streamExecutionRuntime)
	return runtime.MakeInputKVSplitStream[runtime.KeyValue[K, V]](name, streamExecutionRuntime)
}

func InStubKV[K comparable, V any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedBinaryKVConsumedStream[runtime.KeyValue[K, V]] {
	runtime.RegisterKeyValueSerde[K, V](streamExecutionRuntime)
	return runtime.MakeInStubKVStream[runtime.KeyValue[K, V]](name, streamExecutionRuntime)
}

func OutStub[T any](name string, stream runtime.TypedStream[T]) runtime.TypedStreamConsumer[T] {
	return runtime.MakeOutStubStream[T](name, stream)
}
