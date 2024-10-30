/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
	"time"
)

type MultiJoinFunction[K comparable, T, R any] interface {
	MultiJoin(Stream, K, [][]interface{}, Collect[R]) bool
}

type MultiJoinFunctionContext[K comparable, T, R any] struct {
	StreamFunction[R]
	context TypedStream[R]
	f       MultiJoinFunction[K, T, R]
}

func (f *MultiJoinFunctionContext[K, T, R]) call(key K, values [][]interface{}, out Collect[R]) bool {
	f.BeforeCall()
	result := f.f.MultiJoin(f.context, key, values, out)
	f.AfterCall()
	return result
}

type multiJoinLinkStream interface {
	serializeValue(value interface{}) ([]byte, error)
	deserializeValue([]byte) (interface{}, error)
}

type MultiJoinLinkStream[K comparable, T1, T2, R any] struct {
	multiJoinStream *MultiJoinStream[K, T1, R]
	index           int
	serdeIn         serde.StreamKeyValueSerde[datastruct.KeyValue[K, T2]]
	serdeValue      serde.Serde[T2]
	source          TypedStream[datastruct.KeyValue[K, T2]]
}

func MakeMultiJoinLink[K comparable, T1, T2, R any](
	multiJoin TypedStream[R],
	stream TypedStream[datastruct.KeyValue[K, T2]]) {

	multiJoinStream := multiJoin.(*MultiJoinStream[K, T1, R])

	link := &MultiJoinLinkStream[K, T1, T2, R]{
		multiJoinStream: multiJoinStream,
		index:           len(multiJoinStream.links),
		source:          stream,
		serdeIn:         stream.GetSerde().(serde.StreamKeyValueSerde[datastruct.KeyValue[K, T2]]),
		serdeValue:      stream.GetSerde().(serde.StreamKeyValueSerde[datastruct.KeyValue[K, T2]]).ValueSerializer().(serde.Serde[T2]),
	}
	stream.SetConsumer(link)
	multiJoinStream.links = append(multiJoinStream.links, link)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) serializeValue(value interface{}) ([]byte, error) {
	v := value.(T2)
	return s.serdeValue.Serialize(v, nil)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) deserializeValue(data []byte) (interface{}, error) {
	return s.serdeValue.Deserialize(data)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) Consume(value datastruct.KeyValue[K, T2]) {
	s.multiJoinStream.ConsumeRight(s.index, datastruct.KeyValue[K, interface{}]{Key: value.Key, Value: value.Value})
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetId() int {
	return s.multiJoinStream.GetId()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetName() string {
	return s.multiJoinStream.GetName()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetEnvironment() ServiceExecutionEnvironment {
	return s.multiJoinStream.GetEnvironment()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetConfig() *config.StreamConfig {
	return s.multiJoinStream.GetConfig()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetConsumers() []Stream {
	return s.multiJoinStream.GetConsumers()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTransformationName() string {
	return s.multiJoinStream.GetTransformationName()
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) GetTypeName() string {
	return s.multiJoinStream.GetTypeName()
}

type MultiJoinStream[K comparable, T, R any] struct {
	ConsumedStream[R]
	f           MultiJoinFunctionContext[K, T, R]
	links       []multiJoinLinkStream
	serdeIn     serde.StreamSerde[datastruct.KeyValue[K, T]]
	source      TypedStream[datastruct.KeyValue[K, T]]
	joinStorage store.JoinStorage[K]
}

func MakeMultiJoinStream[K comparable, T, R any](
	name string, leftStream TypedStream[datastruct.KeyValue[K, T]],
	f MultiJoinFunction[K, T, R]) *MultiJoinStream[K, T, R] {

	env := leftStream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	if streamConfig.JoinStorage == nil {
		env.Log().Fatalf("Join storage type is undefined for the stream '%s", name)
		return nil
	}
	multiJoinStream := &MultiJoinStream[K, T, R]{
		ConsumedStream: ConsumedStream[R]{
			ServiceStream: ServiceStream[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
		serdeIn: leftStream.GetSerde(),
		source:  leftStream,
		f: MultiJoinFunctionContext[K, T, R]{
			f: f,
		},
	}
	multiJoinStream.joinStorage = store.MakeJoinStorage[K](env, multiJoinStream)
	runtime.registerStorage(multiJoinStream.joinStorage)
	multiJoinStream.f.context = multiJoinStream
	leftStream.SetConsumer(multiJoinStream)
	runtime.registerStream(multiJoinStream)

	return multiJoinStream
}

func (s *MultiJoinStream[K, T, R]) consume(key K, index int, value interface{}) {
	s.joinStorage.JoinValue(key, index, value, func(values [][]interface{}) bool {
		if len(values) > 0 && len(values[0]) > 0 {
			return s.f.call(key, values, s)
		}
		return false
	})
}

func (s *MultiJoinStream[K, T, R]) Consume(value datastruct.KeyValue[K, T]) {
	s.consume(value.Key, 0, value.Value)
}

func (s *MultiJoinStream[K, T, R]) ConsumeRight(index int, value datastruct.KeyValue[K, interface{}]) {
	s.consume(value.Key, index+1, value.Value)
}

func (s *MultiJoinStream[K, T, R]) Out(value R) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}

func (s *MultiJoinStream[K, T, R]) GetJoinStorageType() api.JoinStorageType {
	return *s.GetConfig().JoinStorage
}

func (s *MultiJoinStream[K, T, R]) GetTTL() time.Duration {
	ttl := time.Duration(0)
	if s.GetConfig().Ttl != nil {
		ttl = time.Duration(*s.GetConfig().Ttl) * time.Millisecond
	}
	return ttl
}

func (s *MultiJoinStream[K, T, R]) GetRenewTTL() bool {
	renewTTL := false
	if s.GetConfig().RenewTTL != nil {
		renewTTL = *s.GetConfig().RenewTTL
	}
	return renewTTL
}
