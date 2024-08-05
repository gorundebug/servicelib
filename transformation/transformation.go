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

func Map[T, R any](name string, stream runtime.TypedStream[T], f runtime.MapFunction[T, R]) runtime.TypedStream[R] {
	return runtime.MakeMapStream[T, R](name, stream, f)
}

func AppSink[T any](name string, stream runtime.TypedStream[T], consumer runtime.Consumer[T]) runtime.Consumer[T] {
	return runtime.MakeAppSinkStream[T](name, stream, consumer)
}

func Filter[T any](name string, stream runtime.TypedStream[T], f runtime.FilterFunction[T]) runtime.TypedStream[T] {
	return runtime.MakeFilterStream[T](name, stream, f)
}

func FlatMap[T, R any](name string, stream runtime.TypedStream[T], f runtime.FlatMapFunction[T, R]) runtime.TypedStream[R] {
	return runtime.MakeFlatMapStream[T, R](name, stream, f)
}

func FlatMapIterable[T []any | string, R any](name string, stream runtime.TypedStream[T]) runtime.TypedStream[R] {
	return runtime.MakeFlatMapIterableStream[T, R](name, stream)
}

func Foreach[T any](name string, stream runtime.TypedStream[T], f runtime.ForeachFunction[T]) runtime.TypedStream[T] {
	return runtime.MakeForeachStream[T](name, stream, f)
}

func Input[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedInputStream[T] {
	return runtime.MakeInputStream[T](name, streamExecutionRuntime)
}

func Join[K comparable, T1, T2, R any](name string, stream runtime.TypedStream[runtime.KeyValue[K, T1]],
	streamRight runtime.TypedStream[runtime.KeyValue[K, T2]],
	f runtime.JoinFunction[K, T1, T2, R]) runtime.TypedStream[R] {
	return runtime.MakeJoinStream(name, stream, streamRight, f)
}

func KeyBy[T any, K comparable, V any](name string, stream runtime.TypedStream[T], f runtime.KeyByFunction[T, K, V]) runtime.TypedStream[runtime.KeyValue[K, V]] {
	return runtime.MakeKeyByStream[T, K, V](name, stream, f)
}

func Link[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) {
	runtime.MakeLinkStream[T](name, streamExecutionRuntime)
}

func Merge[T any](name string, streams ...runtime.TypedStream[T]) runtime.TypedStream[T] {
	return runtime.MakeMergeStream[T](name, streams...)
}

func MultiJoin[K comparable, T, R any](
	name string, leftStream runtime.TypedStream[runtime.KeyValue[K, T]],
	f runtime.MultiJoinFunction[K, T, R]) runtime.TypedStream[R] {
	return runtime.MakeMultiJoinStream[K, T, R](name, leftStream, f)
}

func Parallels[T, R any](name string, stream runtime.TypedStream[T], f runtime.ParallelsFunction[T, R]) runtime.TypedStream[R] {
	return runtime.MakeParallelsStream[T, R](name, stream, f)
}

func Sink[T any](name string, stream runtime.TypedStream[T], consumer runtime.Consumer[T]) runtime.TypedSinkStream[T] {
	return runtime.MakeSinkStream[T](name, stream, consumer)
}

func Split[T any](name string, stream runtime.TypedStream[T], count int) runtime.TypedSplitStream[T] {
	return runtime.MakeSplitStream[T](name, stream, count)
}

func InStub[T any](name string, streamExecutionRuntime runtime.StreamExecutionRuntime) runtime.TypedStream[T] {
	return runtime.MakeInStubStream[T](name, streamExecutionRuntime)
}

func OutStub[T any](name string, stream runtime.TypedStream[T]) runtime.TypedStream[T] {
	return runtime.MakeOutStubStream[T](name, stream)
}