/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type InputStream[T any] struct {
	ConsumedStream[T]
}

func MakeInputStream[T any](name string, env ServiceExecutionEnvironment) *InputStream[T] {
	runtime := env.GetRuntime()
	cfg := env.GetAppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.GetLog().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	inputStream := &InputStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[T](runtime),
		},
	}
	runtime.registerStream(inputStream)
	return inputStream
}

func (s *InputStream[T]) GetEndpointId() int {
	return *s.GetConfig().IdEndpoint
}

func (s *InputStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
