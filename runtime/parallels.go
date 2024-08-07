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

type ParallelsFunction[T, R any] interface {
	Parallels(T, Collect[R])
}

type ParallelsFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       ParallelsFunction[T, R]
}

func (f *ParallelsFunctionContext[T, R]) call(value T, out Collect[R]) {
	f.BeforeCall()
	f.f.Parallels(value, out)
	f.AfterCall()
}

type ParallelsStream[T, R any] struct {
	ConsumedStream[R]
	f ParallelsFunctionContext[T, R]
}

func MakeParallelsStream[T, R any](name string, stream TypedStream[T], f ParallelsFunction[T, R]) *ParallelsStream[T, R] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}

	parallelsStream := &ParallelsStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			Stream: Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: ParallelsFunctionContext[T, R]{
			f: f,
		},
	}
	parallelsStream.f.context = parallelsStream
	stream.setConsumer(parallelsStream)
	runtime.registerStream(parallelsStream)
	return parallelsStream
}

func (s *ParallelsStream[T, R]) Consume(value T) {
	if s.caller != nil {
		s.f.call(value, makeParallelsCollector[R](s.caller))
	}
}
