/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
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

func (s *SplitLink[T]) GetConfig() *StreamConfig {
	return s.splitStream.GetConfig()
}

func (s *SplitLink[T]) setConsumer(consumer TypedStreamConsumer[T]) {
	s.consumer = consumer
	s.caller = makeCaller[T](s.splitStream.runtime, s, makeSerde[T](s.splitStream.runtime))
}

func (s *SplitLink[T]) GetTransformationName() string {
	return s.splitStream.GetTransformationName()
}

func (s *SplitLink[T]) GetTypeName() string {
	return s.splitStream.GetTypeName()
}

func (s *SplitLink[T]) getConsumers() []StreamBase {
	return []StreamBase{s.splitStream}
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
	links []*SplitLink[T]
}

func MakeSplitStream[T any](name string, stream TypedStream[T], count int) *SplitStream[T] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}

	splitStream := &SplitStream[T]{
		ConsumedStream: ConsumedStream[T]{
			Stream: Stream[T]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		links: make([]*SplitLink[T], count),
	}
	for i := 0; i < count; i++ {
		splitStream.links[i] = splitLink[T](i, splitStream)
	}
	stream.setConsumer(splitStream)
	runtime.registerStream(splitStream)
	return splitStream
}

func (s *SplitStream[T]) Get(index int) TypedStream[T] {
	return s.links[index]
}

func (s *SplitStream[T]) Consume(value T) {
	for i := 0; i < len(s.links); i++ {
		if s.links[i] != nil {
			s.links[i].Consume(value)
		}
	}
}
