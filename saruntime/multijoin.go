/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	log "github.com/sirupsen/logrus"
)

type MultiJoinFunction[K comparable, T, R any] interface {
	MultiJoin(K, [][]interface{}, Collect[R])
}

type MultiJoinFunctionContext[K comparable, T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       MultiJoinFunction[K, T, R]
}

func (f *MultiJoinFunctionContext[K, T, R]) call(key K, values [][]interface{}, out Collect[R]) {
	f.BeforeCall()
	f.f.MultiJoin(key, values, out)
	f.AfterCall()
}

type multiJoinLinkStream interface {
	serializeValue(value interface{}) ([]byte, error)
	deserializeValue([]byte) (interface{}, error)
}

type MultiJoinLinkStream[K comparable, T1, T2, R any] struct {
	multiJoinStream *MultiJoinStream[K, T1, R]
	index           int
	serdeValue      StreamSerde[T2]
}

func MultiJoinLink[K comparable, T1, T2, R any](
	multiJoinStream *MultiJoinStream[K, T1, R],
	stream TypedStream[KeyValue[K, T2]]) {

	runtime := multiJoinStream.runtime
	link := MultiJoinLinkStream[K, T1, T2, R]{
		multiJoinStream: multiJoinStream,
		index:           len(multiJoinStream.links),
		serdeValue:      makeSerde[T2](runtime),
	}
	multiJoinStream.links = append(multiJoinStream.links, &link)

	stream.setConsumer(&link)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) serializeValue(value interface{}) ([]byte, error) {
	v := value.(T2)
	return s.serdeValue.Serialize(v)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) deserializeValue(data []byte) (interface{}, error) {
	return s.serdeValue.Deserialize(data)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) Consume(value KeyValue[K, T2]) {
	s.multiJoinStream.ConsumeRight(s.index, KeyValue[K, interface{}]{Key: value.Key, Value: value.Value})
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetId() int {
	return s.multiJoinStream.GetId()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetName() string {
	return s.multiJoinStream.GetName()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetRuntime() StreamExecutionRuntime {
	return s.multiJoinStream.GetRuntime()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetConfig() *StreamConfig {
	return s.multiJoinStream.GetConfig()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) getConsumers() []StreamBase {
	return []StreamBase{s.multiJoinStream}
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTransformationName() string {
	return s.multiJoinStream.GetTransformationName()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTypeName() string {
	return s.multiJoinStream.GetTypeName()
}

type MultiJoinStream[K comparable, T, R any] struct {
	ConsumedStream[R]
	f        MultiJoinFunctionContext[K, T, R]
	links    []multiJoinLinkStream
	serdeKey StreamSerde[K]
}

func MultiJoin[K comparable, T, R any](
	name string, leftStream TypedStream[KeyValue[K, T]],
	f MultiJoinFunction[K, T, R]) *MultiJoinStream[K, T, R] {

	runtime := leftStream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	multiJoinStream := MultiJoinStream[K, T, R]{
		ConsumedStream: ConsumedStream[R]{
			Stream: Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
		f: MultiJoinFunctionContext[K, T, R]{
			f: f,
		},
		serdeKey: makeSerde[K](runtime),
	}
	multiJoinStream.f.context = &multiJoinStream
	leftStream.setConsumer(&multiJoinStream)
	runtime.registerStream(&multiJoinStream)

	return &multiJoinStream
}

func (s *MultiJoinStream[K, T, R]) Consume(value KeyValue[K, T]) {
}

func (s *MultiJoinStream[K, T, R]) ConsumeRight(index int, value KeyValue[K, interface{}]) {
}

func (s *MultiJoinStream[K, T, R]) consume(value R) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
