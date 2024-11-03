/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type FilterFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       FilterFunction[T]
}

func (f *FilterFunctionContext[T]) call(value T) bool {
	f.BeforeCall()
	result := f.f.Filter(f.context, value)
	f.AfterCall()
	return result
}

type FilterStream[T any] struct {
	ConsumedStream[T]
	source TypedStream[T]
	f      FilterFunctionContext[T]
}

func MakeFilterStream[T any](name string, stream TypedStream[T], f FilterFunction[T]) *FilterStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	filterStream := &FilterStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		f: FilterFunctionContext[T]{
			f: f,
		},
	}
	filterStream.f.context = filterStream
	stream.SetConsumer(filterStream)
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
