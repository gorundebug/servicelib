/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	log "github.com/sirupsen/logrus"
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

func (s *SplitLink[T]) GetRuntime() StreamExecutionRuntime {
	return s.splitStream.GetRuntime()
}

func (s *SplitLink[T]) GetConfig() *config.StreamConfig {
	return s.splitStream.GetConfig()
}

func (s *SplitLink[T]) SetConsumer(consumer TypedStreamConsumer[T]) {
	s.consumer = consumer
	s.caller = makeCaller[T](s.splitStream.runtime, s)
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

func (s *SplitLink[T]) getConsumers() []Stream {
	return s.splitStream.getConsumers()
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
	*ConsumedStream[T]
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
		log.Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

func (s *InputKVSplitStream[T]) ConsumeBinary(key []byte, value []byte) {
	t, err := s.serdeKV.DeserializeKeyValue(key, value)
	if err != nil {
		log.Errorln(err)
	} else {
		s.caller.Consume(t)
	}
}

func MakeSplitStream[T any](name string, stream TypedStream[T]) *SplitStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	splitStream := &SplitStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			StreamBase: &StreamBase[T]{
				runtime: runtime,
				config:  streamConfig,
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

func MakeInputSplitStream[T any](name string, runtime StreamExecutionRuntime) *InputSplitStream[T] {
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	inputSplitStream := &InputSplitStream[T]{
		SplitStream: &SplitStream[T]{
			ConsumedStream: &ConsumedStream[T]{
				StreamBase: &StreamBase[T]{
					runtime: runtime,
					config:  streamConfig,
				},
				serde: MakeSerde[T](runtime),
			},
			links: make([]*SplitLink[T], 0, 1),
		},
	}
	runtime.registerStream(inputSplitStream)
	return inputSplitStream
}

func MakeInputKVSplitStream[T any](name string, runtime StreamExecutionRuntime) *InputKVSplitStream[T] {
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	serdeKV := MakeSerde[T](runtime).(serde.StreamKeyValueSerde[T])
	inputKVSplitStream := &InputKVSplitStream[T]{
		SplitStream: &SplitStream[T]{
			ConsumedStream: &ConsumedStream[T]{
				StreamBase: &StreamBase[T]{
					runtime: runtime,
					config:  streamConfig,
				},
				serde: serdeKV,
			},
			links: make([]*SplitLink[T], 0),
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
	return s.links[index]
}

func (s *SplitStream[T]) Consume(value T) {
	for i := 0; i < len(s.links); i++ {
		s.links[i].Consume(value)
	}
}

func (s *SplitStream[T]) getConsumers() []Stream {
	var consumers = make([]Stream, len(s.links))
	for i := 0; i < len(s.links); i++ {
		consumers[i] = s.links[i].GetConsumer()
	}
	return consumers
}
