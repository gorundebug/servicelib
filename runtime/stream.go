/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime/config"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
	"reflect"
)

type Stream[T any] struct {
	runtime StreamExecutionRuntime
	config  config.StreamConfig
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

func (s *Stream[T]) GetConfig() *config.StreamConfig {
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
	serde    serde.StreamSerde[T]
	consumer TypedStreamConsumer[T]
}

func (s *ConsumedStream[T]) getConsumers() []StreamBase {
	if s.consumer != nil {
		return []StreamBase{s.consumer}
	}
	return []StreamBase{}
}

func (s *ConsumedStream[T]) GetSerde() serde.StreamSerde[T] {
	return s.serde
}

func (s *ConsumedStream[T]) setConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Fatalf("consumer already assigned to the stream %d", s.Stream.config.Id)
	}
	s.consumer = consumer
	s.caller = makeCaller[T](s.runtime, s)
}

func (s *ConsumedStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}
