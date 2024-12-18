/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"fmt"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/serde"
	"strconv"
)

type SplitLink[T any] struct {
	splitStream *SplitStream[T]
	index       int
	caller      Caller[T]
	consumer    TypedStreamConsumer[T]
}

func (s *SplitLink[T]) GetId() int {
	return s.splitStream.GetId()
}

func (s *SplitLink[T]) GetName() string {
	return s.splitStream.GetName() + "SplitLink" + strconv.Itoa(s.index)
}

func (s *SplitLink[T]) GetEnvironment() ServiceExecutionEnvironment {
	return s.splitStream.GetEnvironment()
}

func (s *SplitLink[T]) GetConfig() *config.StreamConfig {
	return s.splitStream.GetConfig()
}

func (s *SplitLink[T]) SetConsumer(consumer TypedStreamConsumer[T]) {
	s.consumer = consumer
	s.caller = makeCaller[T](s)
}

func (s *SplitLink[T]) Validate() error {
	return nil
}

func (s *SplitLink[T]) GetTransformationName() string {
	return s.splitStream.GetTransformationName()
}

func (s *SplitLink[T]) GetTypeName() string {
	return s.splitStream.GetTypeName()
}

func (s *SplitLink[T]) GetSerde() serde.StreamSerde[T] {
	return s.splitStream.GetSerde()
}

func (s *SplitLink[T]) GetConsumers() []Stream {
	return s.splitStream.GetConsumers()
}

func (s *SplitLink[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *SplitLink[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}

func splitLink[T any](index int, splitStream *SplitStream[T]) *SplitLink[T] {
	link := SplitLink[T]{
		splitStream: splitStream,
		index:       index,
	}
	return &link
}

type SplitStream[T any] struct {
	ConsumedStream[T]
	links  []*SplitLink[T]
	source TypedStream[T]
}

type InputSplitStream[T any] struct {
	*SplitStream[T]
}

type InputKVSplitStream[T any] struct {
	*SplitStream[T]
	serdeKV serde.StreamKeyValueSerde[T]
}

func (s *InputSplitStream[T]) ConsumeBinary(data []byte) {
	t, err := s.serde.Deserialize(data)
	if err != nil {
		s.environment.Log().Errorln(err)
	} else if s.caller != nil {
		s.caller.Consume(t)
	}
}

func (s *InputKVSplitStream[T]) ConsumeBinary(key []byte, value []byte) {
	t, err := s.serdeKV.DeserializeKeyValue(key, value)
	if err != nil {
		s.environment.Log().Errorln(err)
	} else if s.caller != nil {
		s.caller.Consume(t)
	}
}

func MakeSplitStream[T any](name string, stream TypedStream[T]) *SplitStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	splitStream := &SplitStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: stream.GetSerde(),
		},
		source: stream,
		links:  make([]*SplitLink[T], 0, 2),
	}
	runtime.registerStream(splitStream)
	stream.SetConsumer(splitStream)
	return splitStream
}

func MakeInputSplitStream[T any](name string, env ServiceExecutionEnvironment) *InputSplitStream[T] {
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

	inputSplitStream := &InputSplitStream[T]{
		SplitStream: &SplitStream[T]{
			ConsumedStream: ConsumedStream[T]{
				ServiceStream: ServiceStream[T]{
					environment: env,
					id:          streamConfig.Id,
				},
				serde: ser,
			},
			links: make([]*SplitLink[T], 0, 1),
		},
	}
	runtime.registerStream(inputSplitStream)
	return inputSplitStream
}

func MakeInputKVSplitStream[K comparable, V any](name string,
	env ServiceExecutionEnvironment) *InputKVSplitStream[datastruct.KeyValue[K, V]] {
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
	inputKVSplitStream := &InputKVSplitStream[datastruct.KeyValue[K, V]]{
		SplitStream: &SplitStream[datastruct.KeyValue[K, V]]{
			ConsumedStream: ConsumedStream[datastruct.KeyValue[K, V]]{
				ServiceStream: ServiceStream[datastruct.KeyValue[K, V]]{
					environment: env,
					id:          streamConfig.Id,
				},
				serde: serdeKV,
			},
			links: make([]*SplitLink[datastruct.KeyValue[K, V]], 0),
		},
		serdeKV: serdeKV,
	}
	runtime.registerStream(inputKVSplitStream)
	return inputKVSplitStream
}

func (s *SplitStream[T]) AddStream() TypedConsumedStream[T] {
	index := len(s.links)
	link := splitLink[T](index, s)
	s.links = append(s.links, link)
	return link
}

func (s *SplitStream[T]) Consume(value T) {
	for _, link := range s.links {
		link.Consume(value)
	}
}

func (s *SplitStream[T]) GetConsumers() []Stream {
	var consumers = make([]Stream, len(s.links))
	for i := 0; i < len(s.links); i++ {
		consumers[i] = s.links[i].GetConsumer()
	}
	return consumers
}

func (s *SplitStream[T]) Validate() error {
	for i := 0; i < len(s.links); i++ {
		if s.links[i].GetConsumer() == nil {
			return fmt.Errorf("link with index %d for the SplitStream %q does not have consumer",
				i, s.GetName())
		}
	}
	return nil
}
