/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/serde"
	log "github.com/sirupsen/logrus"
)

type ParallelsFunction[T, R any] interface {
	Parallels(Stream, T, Collect[R])
}

type ParallelsFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       ParallelsFunction[T, R]
}

func (f *ParallelsFunctionContext[T, R]) call(value T, out Collect[R]) {
	f.BeforeCall()
	f.f.Parallels(f.context, value, out)
	f.AfterCall()
}

type ParallelsStream[T, R any] struct {
	ConsumedStream[R]
	source  TypedStream[T]
	serdeIn serde.StreamSerde[T]
	f       ParallelsFunctionContext[T, R]
}

func MakeParallelsStream[T, R any](name string, stream TypedStream[T], f ParallelsFunction[T, R]) *ParallelsStream[T, R] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetAppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}

	parallelsStream := &ParallelsStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			StreamBase: StreamBase[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
		serdeIn: stream.GetSerde(),
		source:  stream,
		f: ParallelsFunctionContext[T, R]{
			f: f,
		},
	}
	parallelsStream.f.context = parallelsStream
	stream.SetConsumer(parallelsStream)
	runtime.registerStream(parallelsStream)
	return parallelsStream
}

func (s *ParallelsStream[T, R]) Consume(value T) {
	if s.caller != nil {
		s.f.call(value, makeParallelsCollector[R](s.caller))
	}
}
