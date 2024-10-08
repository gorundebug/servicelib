/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/serde"
	log "github.com/sirupsen/logrus"
)

type SinkStream[T any] struct {
	StreamBase[T]
	source   TypedStream[T]
	consumer Consumer[T]
	serde    serde.StreamSerde[T]
}

func MakeSinkStream[T any](name string, stream TypedStream[T]) *SinkStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	sinkStream := &SinkStream[T]{
		StreamBase: StreamBase[T]{
			environment: env,
			config:      streamConfig,
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
	return *s.config.IdEndpoint
}
