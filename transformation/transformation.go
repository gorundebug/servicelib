/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package transformation

import (
    "github.com/gorundebug/servicelib/operators"
    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
    "github.com/gorundebug/servicelib/runtime/datastruct"
)

func Map[T, R any](cfg *config.MapStreamConfig, stream runtime.TypedStream[T], f operators.MapFunction[T, R]) (runtime.TypedTransformConsumedStream[T, R], error) {
    return operators.MakeMapStream[T, R](cfg, stream, f)
}

func Filter[T any](cfg *config.FilterStreamConfig, stream runtime.TypedStream[T], f operators.FilterFunction[T]) (runtime.TypedConsumedStream[T], error) {
    return operators.MakeFilterStream[T](cfg, stream, f)
}

func FlatMap[T, R any](cfg *config.FlatMapStreamConfig, stream runtime.TypedStream[T], f operators.FlatMapFunction[T, R]) (runtime.TypedTransformConsumedStream[T, R], error) {
    return operators.MakeFlatMapStream[T, R](cfg, stream, f)
}

func FlatMapIterable[T, R any](cfg *config.FlatMapIterableStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedTransformConsumedStream[T, R], error) {
    return operators.MakeFlatMapIterableStream[T, R](cfg, stream)
}

func Process[T, O, R any](cfg *config.ProcessStreamConfig, stream runtime.TypedStream[T], f operators.ProcessFunction[T, O, R]) (runtime.TypedProcessConsumedStream[T, O, R], error) {
    return operators.MakeProcessStream[T, O, R](cfg, stream, f)
}

func Input[T, R, E any](cfg *config.InputStreamConfig, env runtime.RuntimeEnvironment) (runtime.TypedInputStream[T, R, E], error) {
    return operators.MakeInputStream[T, R, E](cfg, env)
}

func InputKV[K comparable, V any, R, E any](cfg *config.InputStreamConfig, env runtime.RuntimeEnvironment) (runtime.TypedInputStream[datastruct.KeyValue[K, V], R, E], error) {
    return operators.MakeInputKVStream[K, V, R, E](cfg, env)
}

func Join[K comparable, T1, T2, R any](cfg *config.JoinStreamConfig, stream runtime.TypedStream[datastruct.KeyValue[K, T1]],
    streamRight runtime.TypedStream[datastruct.KeyValue[K, T2]],
    f operators.JoinFunction[K, T1, T2, R]) (runtime.TypedJoinConsumedStream[K, T1, T2, R], error) {
    return operators.MakeJoinStream(cfg, stream, streamRight, f)
}

func KeyBy[T any, K comparable, V any](cfg *config.KeyByStreamConfig, stream runtime.TypedStream[T],
    f operators.KeyByFunction[T, K, V]) (runtime.TypedTransformConsumedStream[T, datastruct.KeyValue[K, V]], error) {
    return operators.MakeKeyByStream[T, K, V](cfg, stream, f)
}

func Link[T any](cfg config.StreamConfig, env runtime.RuntimeEnvironment) (runtime.TypedLinkStream[T], error) {
    return operators.MakeLinkStream[T](cfg, env)
}

func Merge[T any](cfg *config.MergeStreamConfig, stream runtime.TypedStream[T],
    streams ...runtime.TypedStream[T]) (runtime.TypedConsumedStream[T], error) {
    return operators.MakeMergeStream[T](cfg, stream, streams...)
}

func MultiJoin[K comparable, T, R any](
    cfg *config.MultiJoinStreamConfig, leftStream runtime.TypedStream[datastruct.KeyValue[K, T]],
    f operators.MultiJoinFunction[K, T, R]) (runtime.TypedMultiJoinConsumedStream[K, T, R], error) {
    return operators.MakeMultiJoinStream[K, T, R](cfg, leftStream, f)
}

func MultiJoinLink[K comparable, T1, T2, R any](
    multiJoinStream runtime.TypedMultiJoinConsumedStream[K, T1, R],
    rightStream runtime.TypedStream[datastruct.KeyValue[K, T2]]) error {
    return operators.MakeMultiJoinLink[K, T1, T2, R](multiJoinStream, rightStream)
}

func WhenStream[T, R any](streamConfig *config.WhenStreamConfig, caseStream runtime.TypedCaseStream[T]) (runtime.TypedConsumedStream[R], error) {
    return operators.MakeWhenStream[T, R](streamConfig, caseStream)
}

func CaseStream[T any](streamConfig *config.CaseStreamConfig, stream runtime.TypedStream[T], f operators.BuildSwitchFunction[T]) (runtime.TypedCaseStream[T], error) {
    return operators.MakeCaseStream[T](streamConfig, stream, f)
}

func Sink[T, E any](cfg *config.SinkStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSinkStream[T, E], error) {
    return operators.MakeSinkStream[T, E](cfg, stream)
}

func SinkWithResult[T, R, E any](cfg *config.SinkStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSinkStreamWithResult[T, R, E], error) {
    return operators.MakeSinkStreamWithResult[T, R, E](cfg, stream)
}

func Split[T any](cfg *config.SplitStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSplitStream[T], error) {
    return operators.MakeSplitStream[T](cfg, stream)
}

func Delay[T any](cfg *config.DelayStreamConfig, stream runtime.TypedStream[T], f operators.DelayFunction[T]) (runtime.TypedConsumedStream[T], error) {
    return operators.MakeDelayStream[T](cfg, stream, f)
}
