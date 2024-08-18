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

type ForEachFunction[T any] interface {
	ForEach(T)
}

type ForEachFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       ForEachFunction[T]
}

func (f *ForEachFunctionContext[T]) call(value T) {
	f.BeforeCall()
	f.f.ForEach(value)
	f.AfterCall()
}

type ForEachStream[T any] struct {
	*ConsumedStream[T]
	f ForEachFunctionContext[T]
}

func MakeForEachStream[T any](name string, stream TypedStream[T], f ForEachFunction[T]) *ForEachStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	forEachStream := &ForEachStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: ForEachFunctionContext[T]{
			f: f,
		},
	}
	forEachStream.f.context = forEachStream
	stream.setConsumer(forEachStream)
	runtime.registerStream(forEachStream)
	return forEachStream
}

func (s *ForEachStream[T]) Consume(value T) {
	s.f.call(value)
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
