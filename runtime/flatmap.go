/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
)

type FlatMapFunction[T, R any] interface {
	FlatMap(Stream, T, Collect[R])
}

type FlatMapFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       FlatMapFunction[T, R]
}

func (f FlatMapFunctionContext[T, R]) call(value T, out Collect[R]) {
	f.BeforeCall()
	f.f.FlatMap(f.context, value, out)
	f.AfterCall()
}

type FlatMapStream[T, R any] struct {
	*ConsumedStream[R]
	serdeIn serde.StreamSerde[T]
	source  TypedStream[T]
	f       FlatMapFunctionContext[T, R]
}

func MakeFlatMapStream[T, R any](name string, stream TypedStream[T], f FlatMapFunction[T, R]) *FlatMapStream[T, R] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	flatMapStream := &FlatMapStream[T, R]{
		ConsumedStream: &ConsumedStream[R]{
			StreamBase: &StreamBase[R]{
				runtime: runtime,
				config:  streamConfig,
			},
			serde: MakeSerde[R](runtime),
		},
		serdeIn: stream.GetSerde(),
		source:  stream,
		f: FlatMapFunctionContext[T, R]{
			f: f,
		},
	}
	flatMapStream.f.context = flatMapStream
	stream.SetConsumer(flatMapStream)
	runtime.registerStream(flatMapStream)
	return flatMapStream
}

func (s *FlatMapStream[T, R]) Consume(value T) {
	if s.caller != nil {
		s.f.call(value, makeCollector[R](s.caller))
	}
}
