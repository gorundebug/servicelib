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
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

var _ runtime.TypedConsumedStream[any] = (*DelayStream[any])(nil)

type DelayFunctionContext[T any] struct {
	runtime.StreamFunction[T]
	stream runtime.TypedStream[T]
	f      DelayFunction[T]
}

func (f *DelayFunctionContext[T]) call(ctx context.Context, value T) time.Duration {
	f.BeforeCall()
	defer f.AfterCall()
	return f.f.Duration(ctx, f.stream, value)
}

func (f *DelayFunctionContext[T]) callError(ctx context.Context, value T, err error, out runtime.Collect[T]) {
	f.BeforeCall()
	defer f.AfterCall()
	f.f.DelayError(ctx, f.stream, value, err, out)
}

type DelayStream[T any] struct {
	runtime.ConsumedStream[T]
	source              runtime.TypedStream[T]
	f                   DelayFunctionContext[T]
	downstreamCollector runtime.CollectFunc[T]
}

func MakeDelayStream[T any](streamConfig *config.DelayStreamConfig, stream runtime.TypedStream[T], f DelayFunction[T]) (runtime.TypedConsumedStream[T], error) {
	env := stream.GetRuntimeEnvironment()
	delayStream := &DelayStream[T]{

		ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, stream.GetSerde()),
		source:         stream,
		f: DelayFunctionContext[T]{
			stream: nil,
			f:      f,
		},
	}
	delayStream.f.stream = delayStream
	delayStream.downstreamCollector = delayStream.Emit
	if err := stream.SetConsumer(delayStream); err != nil {
		return nil, err
	}
	env.RegisterStream(delayStream)
	return delayStream, nil
}

func (s *DelayStream[T]) FunctionImplementation() interface{} {
	return s.f.f
}

func (s *DelayStream[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *DelayStream[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *DelayStream[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *DelayStream[T]) Stream() runtime.Stream {
	return s
}

func (s *DelayStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) error {
	return s.SetDownstream(consumer, s)
}

func (s *DelayStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.delay")
	duration := s.f.call(ctx, value)
	if duration > 0 {
		if durable, err := runtime.BeginDurableDelay(ctx, duration); durable {
			if err != nil {
				tracing.SpanError(span, err)
				s.f.callError(ctx, value, err, s.downstreamCollector)
				span.End()
				return
			}
			s.downstreamCollector.Out(ctx, value)
			span.End()
			return
		}
		if err := s.GetRuntimeEnvironment().Delay(ctx, duration, func() {
			defer span.End()
			if err := ctx.Err(); err != nil {
				tracing.SpanEvent(span, "delay.skipped", tracing.StringAttr("reason", err.Error()))
				return
			}
			s.downstreamCollector.Out(ctx, value)
		}); err != nil {
			tracing.SpanError(span, err)
			s.f.callError(ctx, value, err, s.downstreamCollector)
			span.End()
		}
		return
	}
	defer span.End()
	s.downstreamCollector.Out(ctx, value)
}
