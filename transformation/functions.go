/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package transformation

import "github.com/gorundebug/servicelib/operators"

type FilterFunction[T any] = operators.FilterFunction[T]
type FilterHandler[T any] = operators.FilterHandler[T]

type FlatMapFunction[T, R any] = operators.FlatMapFunction[T, R]
type FlatMapHandler[T, R any] = operators.FlatMapHandler[T, R]

type JoinFunction[K comparable, T1, T2, R any] = operators.JoinFunction[K, T1, T2, R]
type JoinHandler[K comparable, T1, T2, R any] = operators.JoinHandler[K, T1, T2, R]

type KeyByFunction[T any, K comparable, V any] = operators.KeyByFunction[T, K, V]
type KeyByHandler[T any, K comparable, V any] = operators.KeyByHandler[T, K, V]

type MapFunction[T, R any] = operators.MapFunction[T, R]
type MapHandler[T, R any] = operators.MapHandler[T, R]

type MultiJoinFunction[K comparable, T, R any] = operators.MultiJoinFunction[K, T, R]
type MultiJoinHandler[K comparable, T, R any] = operators.MultiJoinHandler[K, T, R]

type ProcessFunction[T, R, E any] = operators.ProcessFunction[T, R, E]
type ProcessHandler[T, R, E any] = operators.ProcessHandler[T, R, E]

type DelayFunction[T any] = operators.DelayFunction[T]

type When = operators.When
type BuildSwitchFunction[T any] = operators.BuildSwitchFunction[T]
type BuildSwitchHandler[T any] = operators.BuildSwitchHandler[T]

func DefaultBuildSwitch[T any](whenItems []When) func(T) int {
	return operators.DefaultBuildSwitch[T](whenItems)
}
