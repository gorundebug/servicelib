/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
)

type SinkStream[T any] struct {
	*StreamBase[T]
	source   TypedStream[T]
	consumer Consumer[T]
	serde    serde.StreamSerde[T]
}

func MakeSinkStream[T any](name string, stream TypedStream[T]) *SinkStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	sinkStream := &SinkStream[T]{
		StreamBase: &StreamBase[T]{
			runtime: runtime,
			config:  streamConfig,
		},
		serde:  stream.GetSerde(),
		source: stream,
	}
	stream.setConsumer(sinkStream)
	runtime.registerStream(sinkStream)
	return sinkStream
}

func (s *SinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *SinkStream[T]) SetConsumer(consumer Consumer[T]) {
	s.consumer = consumer
}

func (s *SinkStream[T]) getConsumers() []Stream {
	return []Stream{}
}

func (s *SinkStream[T]) GetEndpointId() int {
	return s.config.Properties["idendpoint"].(int)
}
