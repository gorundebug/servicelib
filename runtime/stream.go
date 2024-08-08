/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import "reflect"

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
	GetConsumer() StreamConsumer[T]
	setConsumer(StreamConsumer[T])
}

type TypedSplitStream[T any] interface {
	Get(index int) TypedStream[T]
}

type TypedInputStream[T any] interface {
	TypedStream[T]
	Consumer[T]
	GetEndpointId() int
}

type TypedSinkStream[T any] interface {
	StreamConsumer[T]
	GetEndpointId() int
	SetConsumer(Consumer[T])
}

type Consumer[T any] interface {
	Consume(T)
}

type ConsumerFunc[T any] func(T)

func (f ConsumerFunc[T]) Consume(value T) {
	f(value)
}

type StreamConsumer[T any] interface {
	StreamBase
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
	Stream[T]
	caller   Caller[T]
	consumer StreamConsumer[T]
}

func (s *ConsumedStream[T]) getConsumers() []StreamBase {
	return []StreamBase{s.consumer}
}

func (s *ConsumedStream[T]) setConsumer(consumer StreamConsumer[T]) {
	s.consumer = consumer
	s.caller = makeCaller[T](s.runtime, s, makeSerde[T](s.runtime))
}

func (s *ConsumedStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *ConsumedStream[T]) GetConsumer() StreamConsumer[T] {
	return s.consumer
}
