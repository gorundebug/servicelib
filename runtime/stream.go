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
	"reflect"
)

type StreamBase[T any] struct {
	environment ServiceExecutionEnvironment
	id          int
}

func (s *StreamBase[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *StreamBase[T]) GetName() string {
	return s.GetConfig().Name
}

func (s *StreamBase[T]) GetId() int {
	return s.id
}

func (s *StreamBase[T]) GetConfig() *config.StreamConfig {
	return s.environment.GetAppConfig().GetStreamConfigById(s.id)
}

func (s *StreamBase[T]) GetEnvironment() ServiceExecutionEnvironment {
	return s.environment
}

func (s *StreamBase[T]) GetTransformationName() string {
	return s.GetConfig().GetTransformationName()
}

type ConsumedStream[T any] struct {
	StreamBase[T]
	caller   Caller[T]
	serde    serde.StreamSerde[T]
	consumer TypedStreamConsumer[T]
}

func (s *ConsumedStream[T]) GetConsumers() []Stream {
	if s.consumer != nil {
		return []Stream{s.consumer}
	}
	return []Stream{}
}

func (s *ConsumedStream[T]) GetSerde() serde.StreamSerde[T] {
	return s.serde
}

func (s *ConsumedStream[T]) SetConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Fatalf("consumer already assigned to the stream %s", s.StreamBase.GetConfig().Name)
	}
	s.consumer = consumer
	s.caller = makeCaller[T](s.environment, s)
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}
