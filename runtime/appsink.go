/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
)

type AppSinkStream[T any] struct {
	*StreamBase[T]
	consumer ConsumerFunc[T]
	serde    serde.StreamSerde[T]
	source   TypedStream[T]
}

func MakeAppSinkStream[T any](name string, stream TypedStream[T], consumer ConsumerFunc[T]) *AppSinkStream[T] {
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	appSink := &AppSinkStream[T]{
		StreamBase: &StreamBase[T]{
			runtime: runtime,
			config:  *streamConfig,
		},
		consumer: consumer,
		serde:    stream.GetSerde(),
		source:   stream,
	}
	stream.setConsumer(appSink)
	runtime.registerStream(appSink)
	return appSink
}

func (s *AppSinkStream[T]) Consume(value T) {
	_ = s.consumer(value)
}

func (s *AppSinkStream[T]) getConsumers() []Stream {
	return []Stream{}
}
