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

type ExternalLinkConsumer struct {
}

type ExternalLinkStream[T any] struct {
	Stream[T]
	consumer Consumer[T]
}

func ExternalLink[T any](name string, stream TypedStream[T], consumer Consumer[T]) {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	externalLinkStream := ExternalLinkStream[T]{
		Stream: Stream[T]{
			runtime: runtime,
			config:  *streamConfig,
		},
		consumer: consumer,
	}
	stream.setConsumer(&externalLinkStream)
	runtime.registerStream(&externalLinkStream)
}

func (s *ExternalLinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *ExternalLinkStream[T]) getConsumers() []StreamBase {
	return []StreamBase{}
}
