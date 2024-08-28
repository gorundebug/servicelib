/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/api"
	"gitlab.com/gorundebug/servicelib/runtime/config"
	"gitlab.com/gorundebug/servicelib/runtime/datastruct"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
	"gitlab.com/gorundebug/servicelib/runtime/store"
	"time"
)

type JoinFunction[K comparable, T1, T2, R any] interface {
	Join(Stream, K, []T1, []T2, Collect[R]) bool
}

type JoinFunctionContext[K comparable, T1, T2, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       JoinFunction[K, T1, T2, R]
}

func (f *JoinFunctionContext[K, T1, T2, R]) call(key K, leftValue []T1, rightValue []T2, out Collect[R]) bool {
	f.BeforeCall()
	result := f.f.Join(f.context, key, leftValue, rightValue, out)
	f.AfterCall()
	return result
}

type JoinLink[K comparable, T1, T2, R any] struct {
	joinStream *JoinStream[K, T1, T2, R]
	serde      serde.StreamSerde[datastruct.KeyValue[K, T2]]
	source     TypedStream[datastruct.KeyValue[K, T2]]
}

func joinLink[K comparable, T1, T2, R any](joinStream *JoinStream[K, T1, T2, R], stream TypedStream[datastruct.KeyValue[K, T2]]) *JoinLink[K, T1, T2, R] {
	joinLink := &JoinLink[K, T1, T2, R]{
		joinStream: joinStream,
		source:     stream,
		serde:      stream.GetSerde(),
	}
	return joinLink
}

func (s *JoinLink[K, T1, T2, R]) Consume(value datastruct.KeyValue[K, T2]) {
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

func (s *JoinLink[K, T1, T2, R]) GetConfig() *config.StreamConfig {
	return s.joinStream.GetConfig()
}

func (s *JoinLink[K, T1, T2, R]) getConsumers() []Stream {
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
	f           JoinFunctionContext[K, T1, T2, R]
	serdeIn     serde.StreamSerde[datastruct.KeyValue[K, T1]]
	source      TypedStream[datastruct.KeyValue[K, T1]]
	joinStorage store.JoinStorage[K]
}

func (s *JoinStream[K, T1, T2, R]) consume(key K, index int, value interface{}) {
	s.joinStorage.JoinValue(key, index, value, func(values [][]interface{}) bool {
		var leftValues []T1
		var rightValues []T2
		if len(values) > 0 {
			leftValues = make([]T1, len(values[0]))
			for idx, v := range values[0] {
				leftValues[idx] = v.(T1)
			}
		}
		if len(values) > 1 {
			rightValues = make([]T2, len(values[1]))
			for idx, v := range values[1] {
				rightValues[idx] = v.(T2)
			}
		}
		return s.f.call(key, leftValues, rightValues, s)
	})
}

func (s *JoinStream[K, T1, T2, R]) ConsumeRight(value datastruct.KeyValue[K, T2]) {
	s.consume(value.Key, 1, value.Value)
}

func (s *JoinStream[K, T1, T2, R]) Consume(value datastruct.KeyValue[K, T1]) {
	s.consume(value.Key, 0, value.Value)
}

func (s *JoinStream[K, T1, T2, R]) Out(value R) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func MakeJoinStream[K comparable, T1, T2, R any](name string, stream TypedStream[datastruct.KeyValue[K, T1]],
	streamRight TypedStream[datastruct.KeyValue[K, T2]],
	f JoinFunction[K, T1, T2, R]) *JoinStream[K, T1, T2, R] {

	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	joinStream := &JoinStream[K, T1, T2, R]{
		ConsumedStream: &ConsumedStream[R]{
			StreamBase: &StreamBase[R]{
				runtime: runtime,
				config:  streamConfig,
			},
			serde: MakeSerde[R](runtime),
		},
		f: JoinFunctionContext[K, T1, T2, R]{
			f: f,
		},
		serdeIn: stream.GetSerde(),
		source:  stream,
	}
	if streamConfig.JoinStorage == nil {
		log.Fatalf("Join storage type is undefined for the stream '%s", name)
		return nil
	}
	ttl := time.Duration(0)
	if streamConfig.TTL != nil {
		ttl = time.Duration(*streamConfig.TTL) * time.Millisecond
	}
	switch *streamConfig.JoinStorage {
	case api.HashMap:
		joinStream.joinStorage = store.MakeHashMapJoinStorage[K](ttl)
	default:
		log.Fatalf("Join storage type %d is not supported for the stream '%s", *streamConfig.JoinStorage, name)
		return nil
	}

	joinStream.f.context = joinStream
	stream.SetConsumer(joinStream)
	runtime.registerStream(joinStream)

	link := joinLink[K, T1, T2, R](joinStream, streamRight)
	streamRight.SetConsumer(link)
	return joinStream
}
