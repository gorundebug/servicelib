/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
    "context"

    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
    "github.com/gorundebug/servicelib/runtime/datastruct"
)

var _ runtime.TypedTransformConsumedStream[any, datastruct.KeyValue[int, any]] = (*KeyByStream[any, int, any])(nil)

type KeyByFunctionContext[T any, K comparable, V any] struct {
    runtime.StreamFunction[datastruct.KeyValue[K, V]]
    stream runtime.TypedStream[datastruct.KeyValue[K, V]]
    f      KeyByFunction[T, K, V]
}

func (f *KeyByFunctionContext[T, K, V]) call(ctx context.Context, value T, out runtime.Collect[datastruct.KeyValue[K, V]]) {
    f.BeforeCall()
    defer f.AfterCall()
    f.f.KeyBy(ctx, f.stream, value, out)
}

type KeyByStream[T any, K comparable, V any] struct {
    runtime.ConsumedStream[datastruct.KeyValue[K, V]]
    source runtime.TypedStream[T]
    f      KeyByFunctionContext[T, K, V]
}

func MakeKeyByStream[T any, K comparable, V any](streamConfig *config.KeyByStreamConfig, stream runtime.TypedStream[T], f KeyByFunction[T, K, V]) (runtime.TypedTransformConsumedStream[T, datastruct.KeyValue[K, V]], error) {
    env := stream.GetRuntimeEnvironment()
    keyByStream := &KeyByStream[T, K, V]{

        ConsumedStream: runtime.MakeConsumedStream[datastruct.KeyValue[K, V]](streamConfig.ID, env, runtime.MakeKeyValueSerde[K, V](env)),
        source:         stream,
        f: KeyByFunctionContext[T, K, V]{
            stream: nil,
            f:      f,
        },
    }
    keyByStream.f.stream = keyByStream
    stream.SetConsumer(keyByStream)
    env.RegisterStream(keyByStream)
    return keyByStream, nil
}

func (s *KeyByStream[T, K, V]) Stream() runtime.Stream {
    return s
}

func (s *KeyByStream[T, K, V]) SetConsumer(consumer runtime.TypedStreamConsumer[datastruct.KeyValue[K, V]]) {
    s.SetDownstream(consumer, s)
}

func (s *KeyByStream[T, K, V]) Consume(ctx context.Context, value T) {
    ctx, span := s.StartSpan(ctx, "stream.keyby")
    defer span.End()
    s.f.call(ctx, value, s)
}

func (s *KeyByStream[T, K, V]) Out(ctx context.Context, value datastruct.KeyValue[K, V]) {
    s.Emit(ctx, value)
}
