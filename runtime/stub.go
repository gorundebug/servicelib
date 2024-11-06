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

type InStubStream[T any] struct {
	ConsumedStream[T]
}

func MakeInStubStream[T any](name string, env ServiceExecutionEnvironment) *InStubStream[T] {
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	ser := MakeSerde[T](runtime)
	if ser.ValueSerializer().IsStub() {
		env.Log().Fatalf("Serializer for the type %q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[T]().Name(), name)
	}
	inStubStream := &InStubStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
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

func MakeInStubKVStream[K comparable, V any](name string,
	env ServiceExecutionEnvironment) *InStubKVStream[datastruct.KeyValue[K, V]] {
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	serdeKV := MakeKeyValueSerde[K, V](runtime)
	if serdeKV.KeySerializer().IsStub() {
		env.Log().Fatalf("Serializer for the key type '%q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[K]().Name(), name)
	}
	if serdeKV.ValueSerializer().IsStub() {
		env.Log().Fatalf("Serializer for the value type %q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[V]().Name(), name)
	}

	inStubStream := &InStubKVStream[datastruct.KeyValue[K, V]]{
		ConsumedStream: ConsumedStream[datastruct.KeyValue[K, V]]{
			ServiceStream: ServiceStream[datastruct.KeyValue[K, V]]{
				environment: env,
				id:          streamConfig.Id,
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
		s.environment.Log().Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

func (s *InStubKVStream[T]) ConsumeBinary(key []byte, value []byte) {
	t, err := s.serdeKV.DeserializeKeyValue(key, value)
	if err != nil {
		s.environment.Log().Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

type OutStubStream[T any] struct {
	ServiceStream[T]
	consumer ConsumerFunc[T]
	source   TypedStream[T]
}

func (s *OutStubStream[T]) Consume(value T) {
	err := s.consumer(value)
	if err != nil {
		s.environment.Log().Errorln(err)
	}
}

func (s *OutStubStream[T]) GetConsumers() []Stream {
	return []Stream{}
}

type OutStubBinaryStream[T any] struct {
	ServiceStream[T]
	source   TypedStream[T]
	serde    serde.StreamSerde[T]
	consumer BinaryConsumerFunc
}

func (s *OutStubBinaryStream[T]) Consume(value T) {
	data, err := s.serde.Serialize(value)
	if err != nil {
		s.environment.Log().Fatalln(err)
	}
	err = s.consumer(data)
	if err != nil {
		s.environment.Log().Errorln(err)
	}
}

func (s *OutStubBinaryStream[T]) GetConsumers() []Stream {
	return []Stream{}
}

type OutStubBinaryKVStream[T any] struct {
	ServiceStream[T]
	source   TypedStream[T]
	serdeKV  serde.StreamKeyValueSerde[T]
	consumer BinaryKVConsumerFunc
}

func (s *OutStubBinaryKVStream[T]) Consume(value T) {
	key, err := s.serdeKV.SerializeKey(value)
	if err != nil {
		s.environment.Log().Fatalln(err)
	}
	val, err := s.serdeKV.SerializeValue(value)
	if err != nil {
		s.environment.Log().Fatalln(err)
	}
	err = s.consumer(key, val)
	if err != nil {
		s.environment.Log().Errorln(err)
	}
}

func (s *OutStubBinaryKVStream[T]) GetConsumers() []Stream {
	return []Stream{}
}

func MakeOutStubStream[T any](name string, stream TypedStream[T],
	consumer ConsumerFunc[T]) *OutStubStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}

	outStubStream := &OutStubStream[T]{
		ServiceStream: ServiceStream[T]{
			environment: env,
			id:          streamConfig.Id,
		},
		source:   stream,
		consumer: consumer,
	}
	stream.SetConsumer(outStubStream)
	runtime.registerStream(outStubStream)
	return outStubStream
}

func MakeOutStubBinaryStream[T any](name string, stream TypedStream[T],
	consumer BinaryConsumerFunc) *OutStubBinaryStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}

	ser := stream.GetSerde()
	if ser.ValueSerializer().IsStub() {
		env.Log().Fatalf("Serializer for the type %q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[T]().Name(), name)
	}

	outStubBinaryStream := &OutStubBinaryStream[T]{
		ServiceStream: ServiceStream[T]{
			environment: env,
			id:          streamConfig.Id,
		},
		source:   stream,
		serde:    ser,
		consumer: consumer,
	}
	stream.SetConsumer(outStubBinaryStream)
	runtime.registerStream(outStubBinaryStream)
	return outStubBinaryStream
}

func MakeOutStubBinaryKVStream[T any](name string, stream TypedStream[T],
	consumer BinaryKVConsumerFunc) *OutStubBinaryKVStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%q does not exists", name)
		return nil
	}
	serdeKV := stream.GetSerde().(serde.StreamKeyValueSerde[T])
	if serdeKV.KeySerializer().IsStub() {
		env.Log().Fatalf("Serializer for the key type %q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[T]().Name(), name)
	}
	if serdeKV.ValueSerializer().IsStub() {
		env.Log().Fatalf("Serializer for the value type %q in the stream %q can't be a stub serializer",
			serde.GetSerdeType[T]().Name(), name)
	}
	outStubBinaryKVStream := &OutStubBinaryKVStream[T]{
		ServiceStream: ServiceStream[T]{
			environment: env,
			id:          streamConfig.Id,
		},
		serdeKV:  serdeKV,
		source:   stream,
		consumer: consumer,
	}
	stream.SetConsumer(outStubBinaryKVStream)
	runtime.registerStream(outStubBinaryKVStream)
	return outStubBinaryKVStream
}
