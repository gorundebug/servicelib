/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type AppInputStream[T any] struct {
	ConsumedStream[T]
}

func MakeAppInputStream[T any](name string, env ServiceExecutionEnvironment) *AppInputStream[T] {
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	appInputStream := &AppInputStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[T](runtime),
		},
	}
	runtime.registerStream(appInputStream)
	return appInputStream
}

func (s *AppInputStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
