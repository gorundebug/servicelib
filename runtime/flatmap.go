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

type FlatMapFunction[T, R any] interface {
	FlatMap(T, Collect[R])
}

type FlatMapFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       FlatMapFunction[T, R]
}

func (f FlatMapFunctionContext[T, R]) call(value T, out Collect[R]) {
	f.BeforeCall()
	f.f.FlatMap(value, out)
	f.AfterCall()
}

type FlatMapStream[T, R any] struct {
	ConsumedStream[R]
	f FlatMapFunctionContext[T, R]
}

func MakeFlatMapStream[T, R any](name string, stream TypedStream[T], f FlatMapFunction[T, R]) *FlatMapStream[T, R] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	flatMapStream := &FlatMapStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			Stream: Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: FlatMapFunctionContext[T, R]{
			f: f,
		},
	}
	flatMapStream.f.context = flatMapStream
	stream.setConsumer(flatMapStream)
	runtime.registerStream(flatMapStream)
	return flatMapStream
}

func (s *FlatMapStream[T, R]) Consume(value T) {
	if s.caller != nil {
		s.f.call(value, makeCollector[R](s.caller))
	}
}
