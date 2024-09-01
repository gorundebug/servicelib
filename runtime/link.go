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
	*ConsumedStream[T]
	source TypedConsumedStream[T]
}

func MakeLinkStream[T any](name string, runtime StreamExecutionRuntime) *LinkStream[T] {
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	linkStream := &LinkStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			StreamBase: &StreamBase[T]{
				runtime: runtime,
				config:  streamConfig,
			},
		},
	}
	runtime.registerStream(linkStream)
	return linkStream
}

func (s *LinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *LinkStream[T]) SetSource(stream TypedConsumedStream[T]) {
	s.serde = stream.GetSerde()
	s.source = stream
	stream.SetConsumer(s)
}
