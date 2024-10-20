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
	log "github.com/sirupsen/logrus"
)

type InStubStream[T any] struct {
	ConsumedStream[T]
}

func MakeInStubStream[T any](name string, env ServiceExecutionEnvironment) *InStubStream[T] {
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	ser := MakeSerde[T](runtime)
	if ser.ValueSerializer().IsStubSerde() {
		log.Fatalf("Serializer for the type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[T]().Name(), name)
	}
	inStubStream := &InStubStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				config:      streamConfig,
			},
			serde: ser,
		},
	}
	runtime.registerStream(inStubStream)
	return inStubStream
}

func (s *InStubStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

type InStubKVStream[T any] struct {
	ConsumedStream[T]
	serdeKV serde.StreamKeyValueSerde[T]
}

func MakeInStubKVStream[K comparable, V any](name string, env ServiceExecutionEnvironment) *InStubKVStream[datastruct.KeyValue[K, V]] {
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	serdeKV := MakeKeyValueSerde[K, V](runtime)
	if serdeKV.KeySerializer().IsStubSerde() {
		log.Fatalf("Serializer for the key type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[K]().Name(), name)
	}
	if serdeKV.ValueSerializer().IsStubSerde() {
		log.Fatalf("Serializer for the value type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[V]().Name(), name)
	}

	inStubStream := &InStubKVStream[datastruct.KeyValue[K, V]]{
		ConsumedStream: ConsumedStream[datastruct.KeyValue[K, V]]{
			StreamBase: StreamBase[datastruct.KeyValue[K, V]]{
				environment: env,
				config:      streamConfig,
			},
			serde: serdeKV,
		},
		serdeKV: serdeKV,
	}
	runtime.registerStream(inStubStream)
	return inStubStream
}

func (s *InStubKVStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *InStubStream[T]) ConsumeBinary(data []byte) {
	t, err := s.serde.Deserialize(data)
	if err != nil {
		log.Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

func (s *InStubKVStream[T]) ConsumeBinary(key []byte, value []byte) {
	t, err := s.serdeKV.DeserializeKeyValue(key, value)
	if err != nil {
		log.Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

type OutStubStream[T any] struct {
	ConsumedStream[T]
	consumer ConsumerFunc[T]
	source   TypedStream[T]
}

func (s *OutStubStream[T]) Consume(value T) {
	err := s.consumer(value)
	if err != nil {
		log.Errorln(err)
	}
}

type OutStubBinaryStream[T any] struct {
	ConsumedStream[T]
	source   TypedStream[T]
	consumer BinaryConsumerFunc
}

func (s *OutStubBinaryStream[T]) Consume(value T) {
	data, err := s.serde.Serialize(value)
	if err != nil {
		log.Fatalln(err)
	}
	err = s.consumer(data)
	if err != nil {
		log.Errorln(err)
	}
}

type OutStubBinaryKVStream[T any] struct {
	ConsumedStream[T]
	source   TypedStream[T]
	serdeKV  serde.StreamKeyValueSerde[T]
	consumer BinaryKVConsumerFunc
}

func (s *OutStubBinaryKVStream[T]) Consume(value T) {
	key, err := s.serdeKV.SerializeKey(value)
	if err != nil {
		log.Fatalln(err)
	}
	val, err := s.serdeKV.SerializeValue(value)
	if err != nil {
		log.Fatalln(err)
	}
	err = s.consumer(key, val)
	if err != nil {
		log.Errorln(err)
	}
}

func MakeOutStubStream[T any](name string, stream TypedStream[T], consumer ConsumerFunc[T]) *OutStubStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}

	ser := MakeSerde[T](runtime)

	outStubStream := &OutStubStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				config:      streamConfig,
			},
			serde: ser,
		},
		source:   stream,
		consumer: consumer,
	}
	stream.SetConsumer(outStubStream)
	runtime.registerStream(outStubStream)
	return outStubStream
}

func MakeOutStubBinaryStream[T any](name string, stream TypedStream[T], consumer BinaryConsumerFunc) *OutStubBinaryStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}

	ser := MakeSerde[T](runtime)
	if ser.ValueSerializer().IsStubSerde() {
		log.Fatalf("Serializer for the type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[T]().Name(), name)
	}

	outStubBinaryStream := &OutStubBinaryStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				config:      streamConfig,
			},
			serde: ser,
		},
		source:   stream,
		consumer: consumer,
	}
	stream.SetConsumer(outStubBinaryStream)
	runtime.registerStream(outStubBinaryStream)
	return outStubBinaryStream
}

func MakeOutStubBinaryKVStream[T any](name string, stream TypedStream[T], consumer BinaryKVConsumerFunc) *OutStubBinaryKVStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	serdeKV := MakeSerde[T](runtime).(serde.StreamKeyValueSerde[T])
	if serdeKV.KeySerializer().IsStubSerde() {
		log.Fatalf("Serializer for the key type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[T]().Name(), name)
	}
	if serdeKV.ValueSerializer().IsStubSerde() {
		log.Fatalf("Serializer for the value type '%s' in the stream '%s' can't be a stub serializer", serde.GetSerdeType[T]().Name(), name)
	}
	outStubBinaryKVStream := &OutStubBinaryKVStream[T]{
		ConsumedStream: ConsumedStream[T]{
			StreamBase: StreamBase[T]{
				environment: env,
				config:      streamConfig,
			},
			serde: serdeKV,
		},
		serdeKV:  serdeKV,
		source:   stream,
		consumer: consumer,
	}
	stream.SetConsumer(outStubBinaryKVStream)
	runtime.registerStream(outStubBinaryKVStream)
	return outStubBinaryKVStream
}
