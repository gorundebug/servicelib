/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"time"
)

type DelayFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       DelayFunction[T]
}

func (f *DelayFunctionContext[T]) call(value T) time.Duration {
	f.BeforeCall()
	result := f.f.Duration(f.context, value)
	f.AfterCall()
	return result
}

type DelayStream[T any] struct {
	ConsumedStream[T]
	source TypedStream[T]
	f      DelayFunctionContext[T]
}

func MakeDelayStream[T any](name string, stream TypedStream[T], f DelayFunction[T]) *DelayStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	delayStream := &DelayStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		f: DelayFunctionContext[T]{
			f: f,
		},
	}
	delayStream.f.context = delayStream
	stream.SetConsumer(delayStream)
	runtime.registerStream(delayStream)
	return delayStream
}

func (s *DelayStream[T]) Consume(value T) {
	duration := s.f.call(value)
	if duration > 0 {
		s.environment.Delay(duration, func() {
			if s.caller != nil {
				s.caller.Consume(value)
			}
		})
	} else if s.caller != nil {
		s.caller.Consume(value)
	}
}
