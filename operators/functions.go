/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/datastruct"
)

type FilterFunction[T any] interface {
	Filter(context.Context, runtime.Stream, T) bool
}

type FilterHandler[T any] func(context.Context, runtime.Stream, T) bool

func (f FilterHandler[T]) Filter(ctx context.Context, s runtime.Stream, v T) bool {
	return f(ctx, s, v)
}

type FlatMapFunction[T, R any] interface {
	FlatMap(context.Context, runtime.Stream, T, runtime.Collect[R])
}

type FlatMapHandler[T, R any] func(context.Context, runtime.Stream, T, runtime.Collect[R])

func (f FlatMapHandler[T, R]) FlatMap(ctx context.Context, s runtime.Stream, v T, c runtime.Collect[R]) {
	f(ctx, s, v, c)
}

type JoinFunction[K comparable, T1, T2, R any] interface {
	Join(context.Context, runtime.Stream, K, []T1, []T2, runtime.Collect[R]) bool
}

type JoinHandler[K comparable, T1, T2, R any] func(context.Context, runtime.Stream, K, []T1, []T2, runtime.Collect[R]) bool

func (f JoinHandler[K, T1, T2, R]) Join(ctx context.Context, s runtime.Stream, k K, l []T1, r []T2, c runtime.Collect[R]) bool {
	return f(ctx, s, k, l, r, c)
}

type KeyByFunction[T any, K comparable, V any] interface {
	KeyBy(context.Context, runtime.Stream, T, runtime.Collect[datastruct.KeyValue[K, V]])
}

type KeyByHandler[T any, K comparable, V any] func(context.Context, runtime.Stream, T, runtime.Collect[datastruct.KeyValue[K, V]])

func (f KeyByHandler[T, K, V]) KeyBy(ctx context.Context, s runtime.Stream, v T, c runtime.Collect[datastruct.KeyValue[K, V]]) {
	f(ctx, s, v, c)
}

type MapFunction[T, R any] interface {
	Map(context.Context, runtime.Stream, T, runtime.Collect[R])
}

type MapHandler[T, R any] func(context.Context, runtime.Stream, T, runtime.Collect[R])

func (f MapHandler[T, R]) Map(ctx context.Context, s runtime.Stream, v T, c runtime.Collect[R]) {
	f(ctx, s, v, c)
}

type MultiJoinFunction[K comparable, T, R any] interface {
	MultiJoin(context.Context, runtime.Stream, K, [][]interface{}, runtime.Collect[R]) bool
}

type MultiJoinHandler[K comparable, T, R any] func(context.Context, runtime.Stream, K, [][]interface{}, runtime.Collect[R]) bool

func (f MultiJoinHandler[K, T, R]) MultiJoin(ctx context.Context, s runtime.Stream, k K, v [][]interface{}, c runtime.Collect[R]) bool {
	return f(ctx, s, k, v, c)
}

type ProcessFunction[T, R, E any] interface {
	Process(context.Context, runtime.Stream, T, runtime.Collect[R], runtime.Collect[E])
}

type ProcessHandler[T, R, E any] func(context.Context, runtime.Stream, T, runtime.Collect[R], runtime.Collect[E])

func (f ProcessHandler[T, R, E]) Process(ctx context.Context, s runtime.Stream, v T, out runtime.Collect[R], errOut runtime.Collect[E]) {
	f(ctx, s, v, out, errOut)
}

type DelayFunction[T any] interface {
	Duration(context.Context, runtime.Stream, T) time.Duration
	DelayError(context.Context, runtime.Stream, T, error, runtime.Collect[T])
}

type When interface {
	GetType() reflect.Type
	GetWhenConsumer() runtime.Stream
}

type BuildSwitchFunction[T any] interface {
	BuildSwitch(runtime.Stream, []When) (func(T) int, error)
}

type BuildSwitchHandler[T any] func(runtime.Stream, []When) (func(T) int, error)

func (f BuildSwitchHandler[T]) BuildSwitch(s runtime.Stream, whenItems []When) (func(T) int, error) {
	return f(s, whenItems)
}

func DefaultBuildSwitch[T any](whenItems []When) func(T) int {
	typeMap := make(map[reflect.Type]int)

	for i, w := range whenItems {
		typeMap[w.GetType()] = i
	}

	return func(v T) int {
		if idx, ok := typeMap[reflect.TypeOf(v)]; ok {
			return idx
		}
		panic(fmt.Sprintf("unknown type %T", v))
	}
}
