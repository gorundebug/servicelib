/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type DelayFunction[T any] interface {
	Duration(Stream, T) time.Duration
}

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
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	delayStream := &DelayStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				config:      streamConfig,
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
	if s.caller != nil {
		duration := s.f.call(value)
		if duration > 0 {
			s.environment.Delay(duration, func() {
				s.caller.Consume(value)
			})
		} else {
			s.caller.Consume(value)
		}
	}
}
