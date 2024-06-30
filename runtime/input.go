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

type InputStream[T any] struct {
	ConsumedStream[T]
}

func Input[T any](name string, runtime StreamExecutionRuntime) *InputStream[T] {
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	inputStream := InputStream[T]{
		ConsumedStream: ConsumedStream[T]{
			Stream: Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	runtime.registerStream(&inputStream)
	return &inputStream
}

func (s *InputStream[T]) GetEndpointId() int {
	return s.config.Properties["idendpoint"].(int)
}
