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
    serdeIn         StreamKeyValueSerde[KeyValue[K, T2]]
    serdeValue      Serde[T2]
    source          TypedStream[KeyValue[K, T2]]
}

func MakeMultiJoinLink[K comparable, T1, T2, R any](
    multiJoin TypedStream[R],
    stream TypedStream[KeyValue[K, T2]]) {

    multiJoinStream := multiJoin.(*MultiJoinStream[K, T1, R])

    link := &MultiJoinLinkStream[K, T1, T2, R]{
        multiJoinStream: multiJoinStream,
        index:           len(multiJoinStream.links),
        source:          stream,
        serdeIn:         stream.GetSerde().(StreamKeyValueSerde[KeyValue[K, T2]]),
        serdeValue:      stream.GetSerde().(StreamKeyValueSerde[KeyValue[K, T2]]).GetValueSerializer().(Serde[T2]),
    }
    multiJoinStream.links = append(multiJoinStream.links, link)

    stream.setConsumer(link)
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
    return s.multiJoinStream.getConsumers()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTransformationName() string {
    return s.multiJoinStream.GetTransformationName()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTypeName() string {
    return s.multiJoinStream.GetTypeName()
}

type MultiJoinStream[K comparable, T, R any] struct {
    *ConsumedStream[R]
    f       MultiJoinFunctionContext[K, T, R]
    links   []multiJoinLinkStream
    serdeIn StreamSerde[KeyValue[K, T]]
    source  TypedStream[KeyValue[K, T]]
}

func MakeMultiJoinStream[K comparable, T, R any](
    name string, leftStream TypedStream[KeyValue[K, T]],
    f MultiJoinFunction[K, T, R]) *MultiJoinStream[K, T, R] {

    runtime := leftStream.GetRuntime()
    config := runtime.GetConfig()
    streamConfig := config.GetStreamConfigByName(name)
    if streamConfig == nil {
        log.Fatalf("Config for the stream with name=%s does not exists", name)
    }
    multiJoinStream := &MultiJoinStream[K, T, R]{
        ConsumedStream: &ConsumedStream[R]{
            Stream: &Stream[R]{
                runtime: runtime,
                config:  *streamConfig,
            },
            serde: makeSerde[R](runtime),
        },
        serdeIn: leftStream.GetSerde(),
        source:  leftStream,
        f: MultiJoinFunctionContext[K, T, R]{
            f: f,
        },
    }
    multiJoinStream.f.context = multiJoinStream
    leftStream.setConsumer(multiJoinStream)
    runtime.registerStream(multiJoinStream)

    return multiJoinStream
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
