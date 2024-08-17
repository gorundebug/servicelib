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

type MapFunction[T, R any] interface {
	Map(T) R
}

type MapFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       MapFunction[T, R]
}

func (f *MapFunctionContext[T, R]) call(value T) R {
	f.BeforeCall()
	result := f.f.Map(value)
	f.AfterCall()
	return result
}

type MapStream[T, R any] struct {
	ConsumedStream[R]
	f MapFunctionContext[T, R]
}

func MakeMapStream[T, R any](name string, stream TypedStream[T], f MapFunction[T, R]) *MapStream[T, R] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	mapStream := &MapStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			Stream: Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: MapFunctionContext[T, R]{
			f: f,
		},
	}
	mapStream.f.context = mapStream
	stream.setConsumer(mapStream)
	runtime.registerStream(mapStream)
	return mapStream
}

func (s *MapStream[T, R]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(s.f.call(value))
	}
}
