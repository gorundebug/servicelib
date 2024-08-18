/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"reflect"
)

type StreamBase interface {
	GetName() string
	GetTransformationName() string
	GetTypeName() string
	GetId() int
	GetConfig() *StreamConfig
	GetRuntime() StreamExecutionRuntime
	getConsumers() []StreamBase
}

type TypedStream[T any] interface {
	StreamBase
	GetConsumer() TypedStreamConsumer[T]
	setConsumer(TypedStreamConsumer[T])
}

type TypedConsumedStream[T any] interface {
	TypedStream[T]
	Consumer[T]
}

type TypedTransformConsumedStream[T any, R any] interface {
	TypedStream[R]
	Consumer[T]
}

type TypedJoinConsumedStream[K comparable, T1, T2, R any] interface {
	TypedTransformConsumedStream[KeyValue[K, T1], R]
	ConsumeRight(KeyValue[K, T2])
}

type TypedMultiJoinConsumedStream[K comparable, T, R any] interface {
	TypedTransformConsumedStream[KeyValue[K, T], R]
	ConsumeRight(int, KeyValue[K, interface{}])
}

type TypedLinkStream[T any] interface {
	TypedStream[T]
	Consumer[T]
	SetConsumer(TypedConsumedStream[T])
}

type TypedSplitStream[T any] interface {
	TypedConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedBinarySplitStream[T any] interface {
	TypedBinaryConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedBinaryKVSplitStream[T any] interface {
	TypedBinaryKVConsumedStream[T]
	AddStream() TypedConsumedStream[T]
}

type TypedInputStream[T any] interface {
	TypedStream[T]
	Consumer[T]
	GetEndpointId() int
}

type TypedSinkStream[T any] interface {
	TypedStreamConsumer[T]
	GetEndpointId() int
	SetConsumer(Consumer[T])
}

type Consumer[T any] interface {
	Consume(T)
}

type BinaryConsumer interface {
	ConsumeBinary([]byte)
}

type BinaryKVConsumer interface {
	ConsumeBinary([]byte, []byte)
}

type TypedBinaryConsumedStream[T any] interface {
	TypedConsumedStream[T]
	BinaryConsumer
}

type TypedBinaryKVConsumedStream[T any] interface {
	TypedConsumedStream[T]
	BinaryKVConsumer
}

type TypedStreamConsumer[T any] interface {
	StreamBase
	Consumer[T]
}

type ConsumerFunc[T any] func(T)

func (f ConsumerFunc[T]) Consume(value T) {
	f(value)
}

type ExternalConsumer[T any] interface {
	Consumer[T]
}

type Stream[T any] struct {
	runtime StreamExecutionRuntime
	config  StreamConfig
}

func (s *Stream[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *Stream[T]) GetName() string {
	return s.config.Name
}

func (s *Stream[T]) GetId() int {
	return s.config.Id
}

func (s *Stream[T]) GetConfig() *StreamConfig {
	return &s.config
}

func (s *Stream[T]) GetRuntime() StreamExecutionRuntime {
	return s.runtime
}

func (s *Stream[T]) GetTransformationName() string {
	return s.GetConfig().GetTransformationName()
}

type ConsumedStream[T any] struct {
	*Stream[T]
	caller   Caller[T]
	consumer TypedStreamConsumer[T]
}

func (s *ConsumedStream[T]) getConsumers() []StreamBase {
	if s.consumer != nil {
		return []StreamBase{s.consumer}
	}
	return []StreamBase{}
}

func (s *ConsumedStream[T]) setConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Fatalf("consumer already assigned to the link stream %d", s.Stream.config.Id)
	}
	s.consumer = consumer
	s.caller = makeCaller[T](s.runtime, s, makeSerde[T](s.runtime))
}

func (s *ConsumedStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}
