/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	log "github.com/sirupsen/logrus"
)

type AppLinkStream[T any] struct {
	Stream[T]
	consumer Consumer[T]
}

func AppLink[T any](name string, stream TypedStream[T], consumer Consumer[T]) *AppLinkStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	appLink := AppLinkStream[T]{
		Stream: Stream[T]{
			runtime: runtime,
			config:  *streamConfig,
		},
		consumer: consumer,
	}
	stream.setConsumer(&appLink)
	runtime.registerStream(&appLink)
	return &appLink
}

func (s *AppLinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *AppLinkStream[T]) getConsumers() []StreamBase {
	return []StreamBase{}
}
