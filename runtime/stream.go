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

type StreamBase[T any] struct {
	runtime StreamExecutionRuntime
	config  *config.StreamConfig
}

func (s *StreamBase[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *StreamBase[T]) GetName() string {
	return s.config.Name
}

func (s *StreamBase[T]) GetId() int {
	return s.config.Id
}

func (s *StreamBase[T]) GetConfig() *config.StreamConfig {
	return s.config
}

func (s *StreamBase[T]) GetRuntime() StreamExecutionRuntime {
	return s.runtime
}

func (s *StreamBase[T]) GetTransformationName() string {
	return s.GetConfig().GetTransformationName()
}

type ConsumedStream[T any] struct {
	*StreamBase[T]
	caller   Caller[T]
	serde    serde.StreamSerde[T]
	consumer TypedStreamConsumer[T]
}

func (s *ConsumedStream[T]) getConsumers() []Stream {
	if s.consumer != nil {
		return []Stream{s.consumer}
	}
	return []Stream{}
}

func (s *ConsumedStream[T]) GetSerde() serde.StreamSerde[T] {
	return s.serde
}

func (s *ConsumedStream[T]) setConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Fatalf("consumer already assigned to the stream %d", s.StreamBase.config.Id)
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
