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
	result := f.f.Duration(ctx, f.stream, value)
	return result
}

type DelayStream[T any] struct {
	runtime.ConsumedStream[T]
	source runtime.TypedStream[T]
	f      DelayFunctionContext[T]
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
	stream.SetConsumer(delayStream)
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

func (s *DelayStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
	s.SetDownstream(consumer, s)
}

func (s *DelayStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.delay")
	defer span.End()
	duration := s.f.call(ctx, value)
	if duration > 0 {
		s.GetRuntimeEnvironment().Delay(duration, func() {
			s.Emit(ctx, value)
		})
	} else {
		s.Emit(ctx, value)
	}
}
