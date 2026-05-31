/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
    "context"
    "fmt"
    "reflect"

    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
)

var _ runtime.TypedTransformConsumedStream[any, any] = (*FlatMapIterableStream[any, any])(nil)

type FlatMapIterableStream[T, R any] struct {
    runtime.ConsumedStream[R]
}

func MakeFlatMapIterableStream[T, R any](streamConfig *config.FlatMapIterableStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedTransformConsumedStream[T, R], error) {
    env := stream.GetRuntimeEnvironment()
    tpT := reflect.TypeOf((*T)(nil)).Elem()
    tpR := reflect.TypeOf((*R)(nil)).Elem()
    if tpT.Kind() != reflect.Array && tpT.Kind() != reflect.Slice && tpT.Kind() != reflect.String {
        return nil, fmt.Errorf("type %s is not an array or slice", tpT.Name())
    }
    if tpT.Kind() != reflect.String {
        tpE := tpT.Elem()
        if tpE != tpR {
            return nil, fmt.Errorf("element type %s does not equals to type %s", tpE.Name(), tpR.Name())
        }
    } else if tpR.Kind() != reflect.Int32 && tpR.Kind() != reflect.Uint8 {
        return nil, fmt.Errorf("element type %s is not rune or byte", tpR.Name())
    }
    flatMapStreamIterable := &FlatMapIterableStream[T, R]{

        ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
    }
    stream.SetConsumer(flatMapStreamIterable)
    env.RegisterStream(flatMapStreamIterable)
    return flatMapStreamIterable, nil
}

func isRuneType(value interface{}) bool {
    switch value.(type) {
    case rune:
        return true
    }
    return false
}

func (s *FlatMapIterableStream[T, R]) Stream() runtime.Stream {
    return s
}

func (s *FlatMapIterableStream[T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
    s.SetDownstream(consumer, s)
}

func (s *FlatMapIterableStream[T, R]) Consume(ctx context.Context, value T) {
    ctx, span := s.StartSpan(ctx, "stream.flatmap_iterable")
    defer span.End()
    var r R
    val := reflect.ValueOf(value)
    if val.Kind() == reflect.String && isRuneType(r) {
        var intf interface{} = value
        str := intf.(string)
        for _, v := range str {
            s.Emit(ctx, reflect.ValueOf(v).Interface().(R))
        }
    } else {
        l := val.Len()
        for i := 0; i < l; i++ {
            s.Emit(ctx, val.Index(i).Interface().(R))
        }
    }
}
