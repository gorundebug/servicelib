/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"time"
)

type FilterFunction[T any] interface {
	Filter(Stream, T) bool
}

type FlatMapFunction[T, R any] interface {
	FlatMap(Stream, T, Collect[R])
}

type JoinFunction[K comparable, T1, T2, R any] interface {
	Join(Stream, K, []T1, []T2, Collect[R]) bool
}

type KeyByFunction[T any, K comparable, V any] interface {
	KeyBy(Stream, T) datastruct.KeyValue[K, V]
}

type MapFunction[T, R any] interface {
	Map(Stream, T) R
}

type SinkFunction[T, R any] interface {
	Sink(Stream, T, error, Collect[R])
}

type MultiJoinFunction[K comparable, T, R any] interface {
	MultiJoin(Stream, K, [][]interface{}, Collect[R]) bool
}

type ParallelsFunction[T, R any] interface {
	Parallels(Stream, T, Collect[R])
}

type ForEachFunction[T any] interface {
	ForEach(Stream, T)
}

type DelayFunction[T any] interface {
	Duration(Stream, T) time.Duration
}
