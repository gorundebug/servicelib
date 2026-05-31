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

var _ runtime.TypedInputStream[any, any, any] = (*InputStream[any, any, any])(nil)
var _ runtime.TypedInputStream[datastruct.KeyValue[int, any], any, any] = (*InputKVStream[int, any, any, any])(nil)

type resultLink[T, R, E any] struct {
    streamLink
    inputStream  *InputStream[T, R, E]
    resultSource runtime.TypedStream[R]
}

func (s *resultLink[T, R, E]) Consume(ctx context.Context, value R) {
    s.inputStream.consumeResult(ctx, value)
}

type InputStream[T, R, E any] struct {
    runtime.ConsumedStream[T]
    resultConsumer runtime.Consumer[R]
    resultLink     *resultLink[T, R, E]
    errorConsumer  *ErrorStream[E]
}

func MakeInputStream[T, R, E any](
    streamConfig *config.InputStreamConfig,
    env runtime.RuntimeEnvironment,
) (runtime.TypedInputStream[T, R, E], error) {
    inputStream := &InputStream[T, R, E]{
        ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, runtime.MakeSerde[T](env)),
        errorConsumer:  MakeErrorStream[E](streamConfig.ID, env),
        resultConsumer: nil,
        resultLink:     nil,
    }
    env.RegisterStream(inputStream)
    return inputStream, nil
}

func (s *InputStream[T, R, E]) GetEndpointId() int {
    return s.GetConfig().(*config.InputStreamConfig).IdEndpoint
}

func (s *InputStream[T, R, E]) Consume(ctx context.Context, value T) {
    ctx, span := s.StartSpan(ctx, "stream.input")
    defer span.End()
    s.Emit(ctx, value)
}

func (s *InputStream[T, R, E]) consumeResult(ctx context.Context, value R) {
    if s.resultConsumer != nil {
        s.resultConsumer.Consume(ctx, value)
    }
}

func (s *InputStream[T, R, E]) SetResultConsumer(resultConsumer runtime.Consumer[R]) {
    s.resultConsumer = resultConsumer
}

func (s *InputStream[T, R, E]) GetResultStream() runtime.TypedSerializedStream[R] {
    if s.resultLink != nil {
        return s.resultLink.resultSource
    }
    return nil
}

func (s *InputStream[T, R, E]) SetSource(source runtime.TypedStream[R]) {
    link := &resultLink[T, R, E]{
        streamLink:   streamLink{stream: s},
        inputStream:  s,
        resultSource: source,
    }
    s.resultLink = link
    source.SetConsumer(link)
}

func (s *InputStream[T, R, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
    return s.errorConsumer
}

func (s *InputStream[T, R, E]) Stream() runtime.Stream {
    return s
}

func (s *InputStream[T, R, E]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
    s.SetDownstream(consumer, s)
}

func (s *InputStream[T, R, E]) GetConsumers() []runtime.Stream {
    return append(s.ConsumedStream.GetConsumers(), s.errorConsumer.GetConsumers()...)
}

type resultLinkKV[K comparable, V, R, E any] struct {
    streamLink
    inputStream  *InputKVStream[K, V, R, E]
    resultSource runtime.TypedStream[R]
}

func (s *resultLinkKV[K, V, R, E]) Consume(ctx context.Context, value R) {
    s.inputStream.consumeResult(ctx, value)
}

type InputKVStream[K comparable, V, R, E any] struct {
    runtime.ConsumedStream[datastruct.KeyValue[K, V]]
    resultLink     *resultLinkKV[K, V, R, E]
    resultConsumer runtime.Consumer[R]
    errorConsumer  *ErrorStream[E]
}

func MakeInputKVStream[K comparable, V, R, E any](
    streamConfig *config.InputStreamConfig,
    env runtime.RuntimeEnvironment,
) (runtime.TypedInputStream[datastruct.KeyValue[K, V], R, E], error) {
    inputStream := &InputKVStream[K, V, R, E]{
        ConsumedStream: runtime.MakeConsumedStream[datastruct.KeyValue[K, V]](streamConfig.ID, env, runtime.MakeKeyValueSerde[K, V](env)),
        errorConsumer:  MakeErrorStream[E](streamConfig.ID, env),
    }
    env.RegisterStream(inputStream)
    return inputStream, nil
}

func (s *InputKVStream[K, V, R, E]) GetEndpointId() int {
    return s.GetConfig().(*config.InputStreamConfig).IdEndpoint
}

func (s *InputKVStream[K, V, R, E]) Consume(ctx context.Context, value datastruct.KeyValue[K, V]) {
    ctx, span := s.StartSpan(ctx, "stream.input")
    defer span.End()
    s.Emit(ctx, value)
}

func (s *InputKVStream[K, V, R, E]) SetResultConsumer(consumer runtime.Consumer[R]) {
    s.resultConsumer = consumer
}

func (s *InputKVStream[K, V, R, E]) SetSource(source runtime.TypedStream[R]) {
    link := &resultLinkKV[K, V, R, E]{
        streamLink:   streamLink{stream: s},
        inputStream:  s,
        resultSource: source,
    }
    s.resultLink = link
    source.SetConsumer(link)
}

func (s *InputKVStream[K, V, R, E]) GetResultStream() runtime.TypedSerializedStream[R] {
    if s.resultLink != nil {
        return s.resultLink.resultSource
    }
    return nil
}

func (s *InputKVStream[K, V, R, E]) consumeResult(ctx context.Context, value R) {
    if s.resultConsumer != nil {
        s.resultConsumer.Consume(ctx, value)
    }
}

func (s *InputKVStream[K, V, R, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
    return s.errorConsumer
}

func (s *InputKVStream[K, V, R, E]) Stream() runtime.Stream {
    return s
}

func (s *InputKVStream[K, V, R, E]) SetConsumer(consumer runtime.TypedStreamConsumer[datastruct.KeyValue[K, V]]) {
    s.SetDownstream(consumer, s)
}

func (s *InputKVStream[K, V, R, E]) GetConsumers() []runtime.Stream {
    return append(s.ConsumedStream.GetConsumers(), s.errorConsumer.GetConsumers()...)
}
