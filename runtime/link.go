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

type LinkStream[T any] struct {
	ConsumedStream[T]
	consumer TypedStreamConsumer[T]
}

func MakeLinkStream[T any](name string, runtime StreamExecutionRuntime) *LinkStream[T] {
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	linkStream := &LinkStream[T]{
		ConsumedStream: ConsumedStream[T]{
			Stream: Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	runtime.registerStream(linkStream)
	return linkStream
}

func (s *LinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *LinkStream[T]) SetConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Panicf("consumer already assigned to the link stream %d", s.Stream.config.Id)
	}
	s.setConsumer(consumer)
}
