/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
    log "github.com/sirupsen/logrus"
)

type AppSinkStream[T any] struct {
    *Stream[T]
    consumer ConsumerFunc[T]
    serde    StreamSerde[T]
    source   TypedStream[T]
}

func MakeAppSinkStream[T any](name string, stream TypedStream[T], consumer ConsumerFunc[T]) *AppSinkStream[T] {
    runtime := stream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    appSink := &AppSinkStream[T]{
        Stream: &Stream[T]{
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

func (s *AppSinkStream[T]) getConsumers() []StreamBase {
    return []StreamBase{}
}
