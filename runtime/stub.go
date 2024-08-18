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

type InStubStream[T any] struct {
	*ConsumedStream[T]
}

type InStubKVStream[T any] struct {
	*ConsumedStream[T]
}

func MakeInStubStream[T any](name string, runtime StreamExecutionRuntime) *InStubStream[T] {
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	inStubStream := &InStubStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	runtime.registerStream(inStubStream)
	return inStubStream
}

func MakeInStubKVStream[T any](name string, runtime StreamExecutionRuntime) *InStubKVStream[T] {
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	inStubStream := &InStubKVStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	runtime.registerStream(inStubStream)
	return inStubStream
}

func (s *InStubStream[T]) Consume(value T) {
}

func (s *InStubStream[T]) ConsumeBinary(data []byte) {
}

func (s *InStubKVStream[T]) Consume(value T) {
}

func (s *InStubKVStream[T]) ConsumeBinary(key []byte, value []byte) {
}

type OutStubStream[T any] struct {
	*ConsumedStream[T]
}

func MakeOutStubStream[T any](name string, stream TypedStream[T]) *OutStubStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	outStubStream := &OutStubStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			Stream: &Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	stream.setConsumer(outStubStream)
	runtime.registerStream(outStubStream)
	return outStubStream
}

func (s *OutStubStream[T]) Consume(T) {}
