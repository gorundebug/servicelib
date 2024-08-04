/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
)

type ForeachFunction[T any] interface {
	ForEach(T)
}

type ForeachFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       ForeachFunction[T]
}

func (f *ForeachFunctionContext[T]) call(value T) {
	f.BeforeCall()
	f.f.ForEach(value)
	f.AfterCall()
}

type ForeachStream[T any] struct {
	ConsumedStream[T]
	f ForeachFunctionContext[T]
}

func MakeForeachStream[T any](name string, stream TypedStream[T], f ForeachFunction[T]) *ForeachStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	foreachStream := ForeachStream[T]{
		ConsumedStream: ConsumedStream[T]{
			Stream: Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: ForeachFunctionContext[T]{
			f: f,
		},
	}
	foreachStream.f.context = &foreachStream
	stream.setConsumer(&foreachStream)
	runtime.registerStream(&foreachStream)
	return &foreachStream
}

func (s *ForeachStream[T]) Consume(value T) {
	s.f.call(value)
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
