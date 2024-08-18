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

type KeyByFunction[T any, K comparable, V any] interface {
	KeyBy(T) KeyValue[K, V]
}

type KeyByFunctionContext[T any, K comparable, V any] struct {
	StreamFunction[KeyValue[K, V]]
	context TypedStream[KeyValue[K, V]]
	f       KeyByFunction[T, K, V]
}

func (f *KeyByFunctionContext[T, K, V]) call(value T) KeyValue[K, V] {
	f.BeforeCall()
	result := f.f.KeyBy(value)
	f.AfterCall()
	return result
}

type KeyByStream[T any, K comparable, V any] struct {
	*ConsumedStream[KeyValue[K, V]]
	f KeyByFunctionContext[T, K, V]
}

func MakeKeyByStream[T any, K comparable, V any](name string, stream TypedStream[T], f KeyByFunction[T, K, V]) *KeyByStream[T, K, V] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
	}
	keyByStream := &KeyByStream[T, K, V]{
		ConsumedStream: &ConsumedStream[KeyValue[K, V]]{
			Stream: &Stream[KeyValue[K, V]]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: KeyByFunctionContext[T, K, V]{
			f: f,
		},
	}
	keyByStream.f.context = keyByStream
	stream.setConsumer(keyByStream)
	runtime.registerStream(keyByStream)
	return keyByStream
}

func (s *KeyByStream[T, K, V]) setConsumer(consumer TypedStreamConsumer[KeyValue[K, V]]) {
	s.consumer = consumer
	s.caller = makeCaller[KeyValue[K, V]](s.runtime, s, makeKeyValueSerde[K, V](s.runtime))
}

func (s *KeyByStream[T, K, V]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(s.f.call(value))
	}
}
