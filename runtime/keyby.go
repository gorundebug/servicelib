/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/serde"
)

type KeyByFunctionContext[T any, K comparable, V any] struct {
	StreamFunction[datastruct.KeyValue[K, V]]
	context TypedStream[datastruct.KeyValue[K, V]]
	f       KeyByFunction[T, K, V]
}

func (f *KeyByFunctionContext[T, K, V]) call(value T) datastruct.KeyValue[K, V] {
	f.BeforeCall()
	result := f.f.KeyBy(f.context, value)
	f.AfterCall()
	return result
}

type KeyByStream[T any, K comparable, V any] struct {
	ConsumedStream[datastruct.KeyValue[K, V]]
	serdeIn serde.StreamSerde[T]
	source  TypedStream[T]
	f       KeyByFunctionContext[T, K, V]
}

func MakeKeyByStream[T any, K comparable, V any](name string, stream TypedStream[T], f KeyByFunction[T, K, V]) *KeyByStream[T, K, V] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	keyByStream := &KeyByStream[T, K, V]{
		ConsumedStream: ConsumedStream[datastruct.KeyValue[K, V]]{
			ServiceStream: ServiceStream[datastruct.KeyValue[K, V]]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeKeyValueSerde[K, V](runtime),
		},
		serdeIn: stream.GetSerde(),
		source:  stream,
		f: KeyByFunctionContext[T, K, V]{
			f: f,
		},
	}
	keyByStream.f.context = keyByStream
	stream.SetConsumer(keyByStream)
	runtime.registerStream(keyByStream)
	return keyByStream
}

func (s *KeyByStream[T, K, V]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(s.f.call(value))
	}
}
