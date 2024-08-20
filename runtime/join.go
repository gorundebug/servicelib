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

type JoinFunction[K comparable, T1, T2, R any] interface {
    Join(K, []T1, []T2, Collect[R])
}

type JoinFunctionContext[K comparable, T1, T2, R any] struct {
    StreamFunction[R]
    context TypedStream[R]
    f       JoinFunction[K, T1, T2, R]
}

func (f *JoinFunctionContext[K, T1, T2, R]) call(key K, leftValue []T1, rightValue []T2, out Collect[R]) {
    f.BeforeCall()
    f.f.Join(key, leftValue, rightValue, out)
    f.AfterCall()
}

type JoinLink[K comparable, T1, T2, R any] struct {
    joinStream *JoinStream[K, T1, T2, R]
    serde      StreamSerde[KeyValue[K, T2]]
    source     TypedStream[KeyValue[K, T2]]
}

func joinLink[K comparable, T1, T2, R any](joinStream *JoinStream[K, T1, T2, R], stream TypedStream[KeyValue[K, T2]]) *JoinLink[K, T1, T2, R] {
    joinLink := &JoinLink[K, T1, T2, R]{
        joinStream: joinStream,
        source:     stream,
        serde:      stream.GetSerde(),
    }
    return joinLink
}

func (s *JoinLink[K, T1, T2, R]) Consume(value KeyValue[K, T2]) {
    s.joinStream.ConsumeRight(value)
}

func (s *JoinLink[K, T1, T2, R]) GetId() int {
    return s.joinStream.GetId()
}

func (s *JoinLink[K, T1, T2, R]) GetName() string {
    return s.joinStream.GetName()
}

func (s *JoinLink[K, T1, T2, R]) GetRuntime() StreamExecutionRuntime {
    return s.joinStream.GetRuntime()
}

func (s *JoinLink[K, T1, T2, R]) GetConfig() *StreamConfig {
    return s.joinStream.GetConfig()
}

func (s *JoinLink[K, T1, T2, R]) getConsumers() []StreamBase {
    return s.joinStream.getConsumers()
}

func (s *JoinLink[K, T1, T2, R]) GetTransformationName() string {
    return s.joinStream.GetTransformationName()
}

func (s *JoinLink[K, T1, T2, R]) GetTypeName() string {
    return s.joinStream.GetTypeName()
}

type JoinStream[K comparable, T1, T2, R any] struct {
    *ConsumedStream[R]
    f       JoinFunctionContext[K, T1, T2, R]
    serdeIn StreamSerde[KeyValue[K, T1]]
    source  TypedStream[KeyValue[K, T1]]
}

func (s *JoinStream[K, T1, T2, R]) ConsumeRight(value KeyValue[K, T2]) {
}

func (s *JoinStream[K, T1, T2, R]) Consume(value KeyValue[K, T1]) {
}

func (s *JoinStream[K, T1, T2, R]) consume(value R) {
    if s.caller != nil {
        s.caller.Consume(value)
    }
}

func MakeJoinStream[K comparable, T1, T2, R any](name string, stream TypedStream[KeyValue[K, T1]],
    streamRight TypedStream[KeyValue[K, T2]],
    f JoinFunction[K, T1, T2, R]) *JoinStream[K, T1, T2, R] {

    runtime := stream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }

    joinStream := &JoinStream[K, T1, T2, R]{
        ConsumedStream: &ConsumedStream[R]{
            Stream: &Stream[R]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[R](runtime),
        },
        f: JoinFunctionContext[K, T1, T2, R]{
            f: f,
        },
        serdeIn: stream.GetSerde(),
        source:  stream,
    }
    joinStream.f.context = joinStream
    stream.setConsumer(joinStream)
    runtime.registerStream(joinStream)

    link := joinLink[K, T1, T2, R](joinStream, streamRight)
    streamRight.setConsumer(link)
    return joinStream
}
