/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type SinkFunctionContext[T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       SinkFunction[T, R]
}

func (f *SinkFunctionContext[T, R]) call(value T, err error, out Collect[R]) {
	f.BeforeCall()
	f.f.Sink(f.context, value, err, out)
	f.AfterCall()
}

type SinkStream[T, R any] struct {
	ConsumedStream[R]
	source       TypedStream[T]
	sinkConsumer SinkConsumer[T]
	f            SinkFunctionContext[T, R]
}

func MakeSinkStream[T, R any](name string, stream TypedStream[T], f SinkFunction[T, R]) *SinkStream[T, R] {
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
		f: SinkFunctionContext[T, R]{
			f: f,
		},
	}
	stream.SetConsumer(sinkStream)
	runtime.registerStream(sinkStream)
	return sinkStream
}

func (s *SinkStream[T, R]) Consume(value T) {
	s.sinkConsumer.Consume(value)
}

func (s *SinkStream[T, R]) Done(value T, err error) {
	s.f.call(value, err, makeCollector[R](s.caller))
}

func (s *SinkStream[T, R]) SetSinkConsumer(sinkConsumer SinkConsumer[T]) {
	s.sinkConsumer = sinkConsumer
	sinkConsumer.SetSinkCallback(s)
}

func (s *SinkStream[T, R]) GetEndpointId() int {
	return *s.GetConfig().IdEndpoint
}
