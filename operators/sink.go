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
)

var _ runtime.TypedSinkStream[any, any] = (*SinkStream[any, any])(nil)
var _ runtime.TypedSinkStreamWithResult[any, any, any] = (*SinkStreamWithResult[any, any, any])(nil)

type SinkStream[T, E any] struct {
    runtime.ServiceStream[T]
    source        runtime.TypedStream[T]
    sinkConsumer  runtime.Consumer[T]
    errorConsumer *ErrorStream[E]
}

func MakeSinkStream[T, E any](streamConfig *config.SinkStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSinkStream[T, E], error) {
    env := stream.GetRuntimeEnvironment()

    sinkStream := &SinkStream[T, E]{
        ServiceStream: runtime.MakeServiceStream[T](streamConfig.ID, env),
        source:        stream,
        errorConsumer: MakeErrorStream[E](streamConfig.ID, env),
    }
    stream.SetConsumer(sinkStream)
    env.RegisterStream(sinkStream)
    return sinkStream, nil
}

func (s *SinkStream[T, E]) Consume(ctx context.Context, value T) {
    if s.sinkConsumer != nil {
        ctx, span := s.StartSpan(ctx, "stream.sink")
        defer span.End()
        s.sinkConsumer.Consume(ctx, value)
    }
}

func (s *SinkStream[T, E]) Build() error {
    return nil
}

func (s *SinkStream[T, E]) Stream() runtime.Stream {
    return s
}

func (s *SinkStream[T, E]) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
    return s.source.GetRuntimeEnvironment()
}

func (s *SinkStream[T, E]) GetConsumers() []runtime.Stream {
    return s.errorConsumer.GetConsumers()
}

func (s *SinkStream[T, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
    return s.errorConsumer
}

func (s *SinkStream[T, E]) SetSinkConsumer(consumer runtime.Consumer[T]) {
    s.sinkConsumer = consumer
}

func (s *SinkStream[T, E]) GetEndpointId() int {
    return s.GetConfig().(*config.SinkStreamConfig).IdEndpoint
}

type SinkStreamWithResult[T, R, E any] struct {
    runtime.ConsumedStream[R]
    source        runtime.TypedStream[T]
    sinkConsumer  runtime.Consumer[T]
    errorConsumer *ErrorStream[E]
}

func MakeSinkStreamWithResult[T, R, E any](streamConfig *config.SinkStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSinkStreamWithResult[T, R, E], error) {
    env := stream.GetRuntimeEnvironment()

    sinkStream := &SinkStreamWithResult[T, R, E]{
        ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
        source:         stream,
        errorConsumer:  MakeErrorStream[E](streamConfig.ID, env),
    }
    stream.SetConsumer(sinkStream)
    env.RegisterStream(sinkStream)
    return sinkStream, nil
}

func (s *SinkStreamWithResult[T, R, E]) ConsumeResult(ctx context.Context, value R) {
    s.Emit(ctx, value)
}

func (s *SinkStreamWithResult[T, R, E]) Consume(ctx context.Context, value T) {
    if s.sinkConsumer != nil {
        ctx, span := s.StartSpan(ctx, "stream.sink")
        defer span.End()
        s.sinkConsumer.Consume(ctx, value)
    }
}

func (s *SinkStreamWithResult[T, R, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
    return s.errorConsumer
}

func (s *SinkStreamWithResult[T, R, E]) Stream() runtime.Stream {
    return s
}

func (s *SinkStreamWithResult[T, R, E]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
    s.SetDownstream(consumer, s)
}

func (s *SinkStreamWithResult[T, R, E]) GetConsumers() []runtime.Stream {
    return append(s.ConsumedStream.GetConsumers(), s.errorConsumer.GetConsumers()...)
}

func (s *SinkStreamWithResult[T, R, E]) SetSinkConsumer(consumer runtime.Consumer[T]) {
    s.sinkConsumer = consumer
}

func (s *SinkStreamWithResult[T, R, E]) GetEndpointId() int {
    return s.GetConfig().(*config.SinkStreamConfig).IdEndpoint
}
