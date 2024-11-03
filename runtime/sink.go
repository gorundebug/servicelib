/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/serde"
)

type SinkStream[T any] struct {
	ServiceStream[T]
	source   TypedStream[T]
	consumer Consumer[T]
	serde    serde.StreamSerde[T]
}

func MakeSinkStream[T any](name string, stream TypedStream[T]) *SinkStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	sinkStream := &SinkStream[T]{
		ServiceStream: ServiceStream[T]{
			environment: env,
			id:          streamConfig.Id,
		},
		serde:  stream.GetSerde(),
		source: stream,
	}
	stream.SetConsumer(sinkStream)
	runtime.registerStream(sinkStream)
	return sinkStream
}

func (s *SinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *SinkStream[T]) SetConsumer(consumer Consumer[T]) {
	s.consumer = consumer
}

func (s *SinkStream[T]) GetConsumers() []Stream {
	return []Stream{}
}

func (s *SinkStream[T]) GetEndpointId() int {
	return *s.GetConfig().IdEndpoint
}
