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
	*ConsumedStream[T]
	source TypedStream[T]
	f      DelayFunctionContext[T]
}

func MakeDelayStream[T any](name string, stream TypedStream[T], f DelayFunction[T]) *DelayStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	delayStream := &DelayStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			StreamBase: &StreamBase[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		f: DelayFunctionContext[T]{
			f: f,
		},
	}
	delayStream.f.context = delayStream
	stream.setConsumer(delayStream)
	runtime.registerStream(delayStream)
	return delayStream
}

func (s *DelayStream[T]) Consume(value T) {
	_ = s.f.call(value)
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
