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

type DelayStream[T any] struct {
	*ConsumedStream[T]
	source TypedStream[T]
}

func MakeDelayStream[T any](name string, stream TypedStream[T]) *DelayStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	delayStream := &DelayStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
	}
	stream.setConsumer(delayStream)
	runtime.registerStream(delayStream)
	return delayStream
}

func (s *DelayStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
