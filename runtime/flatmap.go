/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

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
	ConsumedStream[R]
	source TypedStream[T]
	f      FlatMapFunctionContext[T, R]
}

func MakeFlatMapStream[T, R any](name string, stream TypedStream[T], f FlatMapFunction[T, R]) *FlatMapStream[T, R] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	flatMapStream := &FlatMapStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			ServiceStream: ServiceStream[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
		source: stream,
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
	s.f.call(value, makeCollector[R](s.caller))
}
