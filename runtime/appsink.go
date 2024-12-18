/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type AppSinkStream[T any] struct {
	ServiceStream[T]
	consumer ConsumerFunc[T]
	source   TypedStream[T]
}

func MakeAppSinkStream[T any](name string, stream TypedStream[T], consumer ConsumerFunc[T]) *AppSinkStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	appSink := &AppSinkStream[T]{
		ServiceStream: ServiceStream[T]{
			environment: env,
			id:          streamConfig.Id,
		},
		consumer: consumer,
		source:   stream,
	}
	stream.SetConsumer(appSink)
	runtime.registerStream(appSink)
	return appSink
}

func (s *AppSinkStream[T]) Consume(value T) {
	_ = s.consumer(value)
}

func (s *AppSinkStream[T]) GetConsumers() []Stream {
	return []Stream{}
}
