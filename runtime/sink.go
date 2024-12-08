/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type SinkErrorFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       SinkErrorFunction[T, R]
}

func (f *SinkErrorFunctionContext[T, R]) call(value T, err error, out Collect[R]) {
	f.BeforeCall()
	f.f.SinkError(f.context, value, err, out)
	f.AfterCall()
}

type SinkStream[T, R any] struct {
	ConsumedStream[R]
	source       TypedStream[T]
	sinkConsumer SinkConsumer[T]
	f            SinkErrorFunctionContext[T, R]
}

func MakeSinkStream[T, R any](name string, stream TypedStream[T], f SinkErrorFunction[T, R]) *SinkStream[T, R] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	sinkStream := &SinkStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			ServiceStream: ServiceStream[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
		source: stream,
		f: SinkErrorFunctionContext[T, R]{
			f: f,
		},
	}
	stream.SetConsumer(sinkStream)
	runtime.registerStream(sinkStream)
	return sinkStream
}

func (s *SinkStream[T, R]) Consume(value T) {
	if err := s.sinkConsumer.Consume(value); err != nil {
		s.f.call(value, err, makeCollector[R](s.caller))
	}
}

func (s *SinkStream[T, R]) SetSinkConsumer(sinkConsumer SinkConsumer[T]) {
	s.sinkConsumer = sinkConsumer
}

func (s *SinkStream[T, R]) GetEndpointId() int {
	return *s.GetConfig().IdEndpoint
}
