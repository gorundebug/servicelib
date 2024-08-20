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

type InStubStream[T any] struct {
    *ConsumedStream[T]
}

type InStubKVStream[T any] struct {
    *ConsumedStream[T]
}

func MakeInStubStream[T any](name string, runtime StreamExecutionRuntime) *InStubStream[T] {
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    inStubStream := &InStubStream[T]{
        ConsumedStream: &ConsumedStream[T]{
            Stream: &Stream[T]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[T](runtime),
        },
    }
    runtime.registerStream(inStubStream)
    return inStubStream
}

func MakeInStubKVStream[T any](name string, runtime StreamExecutionRuntime) *InStubKVStream[T] {
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    inStubStream := &InStubKVStream[T]{
        ConsumedStream: &ConsumedStream[T]{
            Stream: &Stream[T]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[T](runtime),
        },
    }
    runtime.registerStream(inStubStream)
    return inStubStream
}

func (s *InStubStream[T]) Consume(value T) {
}

func (s *InStubStream[T]) ConsumeBinary(data []byte) {
}

func (s *InStubKVStream[T]) Consume(value T) {
}

func (s *InStubKVStream[T]) ConsumeBinary(key []byte, value []byte) {
}

type OutStubStream[T any] struct {
    *ConsumedStream[T]
    consumer ConsumerFunc[T]
    source   TypedStream[T]
}

func (s *OutStubStream[T]) Consume(value T) {
    err := s.consumer(value)
    if err != nil {
        log.Errorln(err)
    }
}

type OutStubBinaryStream[T any] struct {
    *ConsumedStream[T]
    source   TypedStream[T]
    consumer BinaryConsumerFunc
}

func (s *OutStubBinaryStream[T]) Consume(T) {
}

type OutStubBinaryKVStream[T any] struct {
    *ConsumedStream[T]
    source   TypedStream[T]
    serdeKV  StreamKeyValueSerde[T]
    consumer BinaryKVConsumerFunc
}

func (s *OutStubBinaryKVStream[T]) Consume(value T) {
    ser := s.serde.(StreamKeyValueSerde[T])
    key, err := ser.SerializeKey(value)
    if err != nil {
        log.Fatalln(err)
    }
    val, err := ser.SerializeValue(value)
    if err != nil {
        log.Fatalln(err)
    }
    err = s.consumer(key, val)
    if err != nil {
        log.Errorln(err)
    }
}

func MakeOutStubStream[T any](name string, stream TypedStream[T], consumer ConsumerFunc[T]) *OutStubStream[T] {
    runtime := stream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    outStubStream := &OutStubStream[T]{
        ConsumedStream: &ConsumedStream[T]{
            Stream: &Stream[T]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[T](runtime),
        },
        source:   stream,
        consumer: consumer,
    }
    stream.setConsumer(outStubStream)
    runtime.registerStream(outStubStream)
    return outStubStream
}

func MakeOutStubBinaryStream[T any](name string, stream TypedStream[T], consumer BinaryConsumerFunc) *OutStubBinaryStream[T] {
    runtime := stream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    outStubBinaryStream := &OutStubBinaryStream[T]{
        ConsumedStream: &ConsumedStream[T]{
            Stream: &Stream[T]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[T](runtime),
        },
        source:   stream,
        consumer: consumer,
    }
    stream.setConsumer(outStubBinaryStream)
    runtime.registerStream(outStubBinaryStream)
    return outStubBinaryStream
}

func MakeOutStubBinaryKVStream[T any](name string, stream TypedStream[T], consumer BinaryKVConsumerFunc) *OutStubBinaryKVStream[T] {
    runtime := stream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    serdeKV := makeSerde[T](runtime).(StreamKeyValueSerde[T])
    outStubBinaryKVStream := &OutStubBinaryKVStream[T]{
        ConsumedStream: &ConsumedStream[T]{
            Stream: &Stream[T]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: serdeKV,
        },
        serdeKV:  serdeKV,
        source:   stream,
        consumer: consumer,
    }
    stream.setConsumer(outStubBinaryKVStream)
    runtime.registerStream(outStubBinaryKVStream)
    return outStubBinaryKVStream
}
