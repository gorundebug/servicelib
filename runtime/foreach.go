/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type ForEachFunction[T any] interface {
	ForEach(Stream, T)
}

type ForEachFunctionContext[T any] struct {
	StreamFunction[T]
	context TypedStream[T]
	f       ForEachFunction[T]
}

func (f *ForEachFunctionContext[T]) call(value T) {
	f.BeforeCall()
	f.f.ForEach(f.context, value)
	f.AfterCall()
}

type ForEachStream[T any] struct {
	ConsumedStream[T]
	source TypedStream[T]
	f      ForEachFunctionContext[T]
}

func MakeForEachStream[T any](name string, stream TypedStream[T], f ForEachFunction[T]) *ForEachStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetAppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.GetLog().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	forEachStream := &ForEachStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		f: ForEachFunctionContext[T]{
			f: f,
		},
	}
	forEachStream.f.context = forEachStream
	stream.SetConsumer(forEachStream)
	runtime.registerStream(forEachStream)
	return forEachStream
}

func (s *ForEachStream[T]) Consume(value T) {
	s.f.call(value)
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
