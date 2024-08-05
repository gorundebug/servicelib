/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
)

type SinkStream[T any] struct {
	Stream[T]
	consumer Consumer[T]
}

func MakeSinkStream[T any](name string, stream TypedStream[T]) *SinkStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	sinkStream := SinkStream[T]{
		Stream: Stream[T]{
			runtime: runtime,
			config:  *streamConfig,
		},
	}
	stream.setConsumer(&sinkStream)
	runtime.registerStream(&sinkStream)
	return &sinkStream
}

func (s *SinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *SinkStream[T]) SetConsumer(consumer Consumer[T]) {
	s.consumer = consumer
}

func (s *SinkStream[T]) getConsumers() []StreamBase {
	return []StreamBase{}
}

func (s *SinkStream[T]) GetEndpointId() int {
	return s.config.Properties["idendpoint"].(int)
}
