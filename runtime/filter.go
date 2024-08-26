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

type FilterFunction[T any] interface {
	Filter(T) bool
}

type FilterFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       FilterFunction[T]
}

func (f *FilterFunctionContext[T]) call(value T) bool {
	f.BeforeCall()
	result := f.f.Filter(value)
	f.AfterCall()
	return result
}

type FilterStream[T any] struct {
	*ConsumedStream[T]
	source TypedStream[T]
	f      FilterFunctionContext[T]
}

func MakeFilterStream[T any](name string, stream TypedStream[T], f FilterFunction[T]) *FilterStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	filterStream := &FilterStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		f: FilterFunctionContext[T]{
			f: f,
		},
	}
	filterStream.f.context = filterStream
	stream.setConsumer(filterStream)
	runtime.registerStream(filterStream)
	return filterStream
}

func (s *FilterStream[T]) Consume(value T) {
	if s.caller != nil {
		if s.f.call(value) {
			s.caller.Consume(value)
		}
	}
}
