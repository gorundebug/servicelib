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

var _ runtime.TypedProcessConsumedStream[any, any, any] = (*ProcessStream[any, any, any])(nil)

type ProcessFunctionContext[T, R, E any] struct {
    runtime.StreamFunction[R]
    stream runtime.TypedStream[R]
    f      ProcessFunction[T, R, E]
}

func (f *ProcessFunctionContext[T, R, E]) call(ctx context.Context, value T, out runtime.Collect[R], errOut runtime.Collect[E]) {
    f.BeforeCall()
    defer f.AfterCall()
    f.f.Process(ctx, f.stream, value, out, errOut)
}

type ProcessStream[T, R, E any] struct {
    runtime.ConsumedStream[R]
    errorConsumer       *ErrorStream[E]
    source              runtime.TypedStream[T]
    f                   ProcessFunctionContext[T, R, E]
    downstreamCollector runtime.CollectFunc[R]
    errorCollector      runtime.CollectFunc[E]
}

func MakeProcessStream[T, R, E any](streamConfig *config.ProcessStreamConfig, stream runtime.TypedStream[T], f ProcessFunction[T, R, E]) (runtime.TypedProcessConsumedStream[T, R, E], error) {
    env := stream.GetRuntimeEnvironment()

    processStream := &ProcessStream[T, R, E]{
        ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
        errorConsumer:  MakeErrorStream[E](streamConfig.ID, env),
        source:         stream,
        f: ProcessFunctionContext[T, R, E]{
            stream: nil,
            f:      f,
        },
    }
    processStream.f.stream = processStream

    processStream.downstreamCollector = processStream.Emit
    processStream.errorCollector = processStream.errorConsumer.Consume

    stream.SetConsumer(processStream)
    env.RegisterStream(processStream)
    return processStream, nil
}

func (s *ProcessStream[T, R, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
    return s.errorConsumer
}

func (s *ProcessStream[T, R, E]) Stream() runtime.Stream {
    return s
}

func (s *ProcessStream[T, R, E]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
    s.SetDownstream(consumer, s)
}

func (s *ProcessStream[T, R, E]) GetConsumers() []runtime.Stream {
    return append(s.ConsumedStream.GetConsumers(), s.errorConsumer.GetConsumers()...)
}

func (s *ProcessStream[T, R, E]) Consume(ctx context.Context, value T) {
    ctx, span := s.StartSpan(ctx, "stream.process")
    defer span.End()
    s.f.call(ctx, value, s.downstreamCollector, s.errorCollector)
}
