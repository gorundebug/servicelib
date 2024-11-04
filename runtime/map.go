/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type MapFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       MapFunction[T, R]
}

func (f *MapFunctionContext[T, R]) call(value T) R {
	f.BeforeCall()
	result := f.f.Map(f.context, value)
	f.AfterCall()
	return result
}

type MapStream[T, R any] struct {
	ConsumedStream[R]
	source TypedStream[T]
	f      MapFunctionContext[T, R]
}

func MakeMapStream[T, R any](name string, stream TypedStream[T], f MapFunction[T, R]) *MapStream[T, R] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	mapStream := &MapStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			ServiceStream: ServiceStream[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
		source: stream,
		f: MapFunctionContext[T, R]{
			f: f,
		},
	}
	mapStream.f.context = mapStream
	stream.SetConsumer(mapStream)
	runtime.registerStream(mapStream)
	return mapStream
}

func (s *MapStream[T, R]) Consume(value T) {
	v := s.f.call(value)
	if s.caller != nil {
		s.caller.Consume(v)
	}
}
