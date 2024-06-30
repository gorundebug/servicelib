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
	Stream[T]
	consumer StreamConsumer[T]
}

func Link[T any](name string, runtime StreamExecutionRuntime) {
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	linkStream := LinkStream[T]{
		Stream: Stream[T]{
			runtime: runtime,
			config:  *streamConfig,
		},
	}
	runtime.registerStream(&linkStream)
}

func (s *LinkStream[T]) Consume(value T) {
	s.consumer.Consume(value)
}

func (s *LinkStream[T]) AssignConsumer(consumer StreamConsumer[T]) {
	s.consumer = consumer
}

func (s *LinkStream[T]) getConsumers() []StreamBase {
	return []StreamBase{s.consumer}
}
