/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
    "context"
    "reflect"

    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
)

var _ runtime.TypedConsumedStream[any] = (*FilterStream[any])(nil)

type FilterFunctionContext[T any] struct {
    runtime.StreamFunction[T]
    stream runtime.TypedStream[T]
    f      FilterFunction[T]
}

func (f *FilterFunctionContext[T]) call(ctx context.Context, value T) bool {
    f.BeforeCall()
    defer f.AfterCall()
    result := f.f.Filter(ctx, f.stream, value)
    return result
}

type FilterStream[T any] struct {
    runtime.ConsumedStream[T]
    source runtime.TypedStream[T]
    f      FilterFunctionContext[T]
}

func MakeFilterStream[T any](streamConfig *config.FilterStreamConfig, stream runtime.TypedStream[T], f FilterFunction[T]) (runtime.TypedConsumedStream[T], error) {
    env := stream.GetRuntimeEnvironment()
    filterStream := &FilterStream[T]{
        ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, stream.GetSerde()),
        source:         stream,
        f: FilterFunctionContext[T]{
            stream: nil,
            f:      f,
        },
    }
    filterStream.f.stream = filterStream
    if err := stream.SetConsumer(filterStream); err != nil {
        return nil, err
    }
    env.RegisterStream(filterStream)
    return filterStream, nil
}

func (s *FilterStream[T]) FunctionImplementation() interface{} {
    return s.f.f
}

func (s *FilterStream[T]) GetErrorConsumer() runtime.RuntimeStream {
    return nil
}

func (s *FilterStream[T]) GetValueType() reflect.Type {
    var t T
    return reflect.TypeOf(&t).Elem()
}

func (s *FilterStream[T]) GetKeyType() reflect.Type {
    return nil
}

func (s *FilterStream[T]) Stream() runtime.Stream {
    return s
}

func (s *FilterStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) error {
    return s.SetDownstream(consumer, s)
}

func (s *FilterStream[T]) Consume(ctx context.Context, value T) {
    if s.TracingEnabled(ctx) {
        newCtx, span := s.StartSpan(ctx, "stream.filter")
        ctx = newCtx
        defer span.End()
    }
    if s.f.call(ctx, value) {
        s.Emit(ctx, value)
    }
}
