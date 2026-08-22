/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
)

var _ runtime.TypedMultiJoinConsumedStream[int, any, any] = (*MultiJoinStream[int, any, any])(nil)

type MultiJoinFunctionContext[K comparable, T, R any] struct {
	runtime.StreamFunction[R]
	stream runtime.TypedStream[R]
	f      MultiJoinFunction[K, T, R]
}

func (f *MultiJoinFunctionContext[K, T, R]) call(ctx context.Context, key K, values [][]interface{}, out runtime.Collect[R]) bool {
	f.BeforeCall()
	defer f.AfterCall()
	result := f.f.MultiJoin(ctx, f.stream, key, values, out)
	return result
}

type multiJoinLinkStream interface {
	runtime.Stream
	serializeValue(value interface{}) ([]byte, error)
	deserializeValue([]byte) (interface{}, error)
}

type MultiJoinLinkStream[K comparable, T1, T2, R any] struct {
	streamLink
	multiJoinStream *MultiJoinStream[K, T1, R]
	index           int
	serdeValue      serde.Serde[T2]
	source          runtime.TypedStream[datastruct.KeyValue[K, T2]]
}

func MakeMultiJoinLink[K comparable, T1, T2, R any](
	multiJoinStream runtime.TypedMultiJoinConsumedStream[K, T1, R],
	rightStream runtime.TypedStream[datastruct.KeyValue[K, T2]]) error {

	multiJoin, ok := multiJoinStream.(*MultiJoinStream[K, T1, R])
	if !ok {
		return fmt.Errorf("multiJoinStream is not a *MultiJoinStream[K, T1, R]")
	}
	kvSerde, ok := rightStream.GetSerde().(serde.StreamKeyValueSerde[datastruct.KeyValue[K, T2]])
	if !ok {
		return fmt.Errorf("rightStream serde is not a StreamKeyValueSerde")
	}
	valueSerializer, ok := kvSerde.ValueSerializer().(serde.Serde[T2])
	if !ok {
		return fmt.Errorf("rightStream value serializer is not a Serde[T2]")
	}
	link := &MultiJoinLinkStream[K, T1, T2, R]{
		streamLink:      streamLink{stream: multiJoin},
		multiJoinStream: multiJoin,
		index:           0,
		source:          rightStream,
		serdeValue:      valueSerializer,
	}
	if err := rightStream.SetConsumer(link); err != nil {
		return err
	}
	link.index = multiJoin.addLink(link)
	return nil
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) serializeValue(value interface{}) ([]byte, error) {
	v := value.(T2)
	return s.serdeValue.Serialize(v, nil)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) deserializeValue(data []byte) (interface{}, error) {
	return s.serdeValue.Deserialize(data)
}

func (s *MultiJoinLinkStream[K, T1, T2, R]) Consume(ctx context.Context, value datastruct.KeyValue[K, T2]) {
	s.multiJoinStream.ConsumeRight(ctx, s.index, datastruct.KeyValue[K, interface{}]{Key: value.Key, Value: value.Value})
}

type multiJoinStorageConfig struct {
	stream runtime.Stream
}

func (jsc *multiJoinStorageConfig) GetTTL() time.Duration {
	ttl := time.Duration(0)
	cfg := jsc.stream.GetConfig().(*config.MultiJoinStreamConfig)
	if cfg.Ttl != 0 {
		ttl = time.Duration(cfg.Ttl) * time.Millisecond
	}
	return ttl
}

func (jsc *multiJoinStorageConfig) GetRenewTTL() bool {
	renewTTL := false
	cfg := jsc.stream.GetConfig().(*config.MultiJoinStreamConfig)
	if cfg.RenewTTL {
		renewTTL = cfg.RenewTTL
	}
	return renewTTL
}

func (jsc *multiJoinStorageConfig) GetName() string {
	return jsc.stream.GetName()
}

type MultiJoinStream[K comparable, T, R any] struct {
	runtime.ConsumedStream[R]
	f           MultiJoinFunctionContext[K, T, R]
	links       []multiJoinLinkStream
	source      runtime.TypedStream[datastruct.KeyValue[K, T]]
	joinStorage store.JoinStorage[K]
}

func MakeMultiJoinStream[K comparable, T, R any](
	streamConfig *config.MultiJoinStreamConfig, leftStream runtime.TypedStream[datastruct.KeyValue[K, T]],
	f MultiJoinFunction[K, T, R]) (runtime.TypedMultiJoinConsumedStream[K, T, R], error) {

	env := leftStream.GetRuntimeEnvironment()
	multiJoinStream := &MultiJoinStream[K, T, R]{
		ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
		source:         leftStream,
		f: MultiJoinFunctionContext[K, T, R]{
			stream: nil,
			f:      f,
		},
	}
	stgCfg := &multiJoinStorageConfig{stream: multiJoinStream}
	stg := env.CreateKeyValueJoinStorage(streamConfig.JoinStorage, stgCfg, multiJoinStream)
	if stg != nil {
		var ok bool
		if multiJoinStream.joinStorage, ok = stg.(store.JoinStorage[K]); !ok {
			return nil, fmt.Errorf("MultiJoinStream joinStorage for stream %q is not a JoinStorage[K]", streamConfig.Name)
		}
	} else {
		var stgErr error
		multiJoinStream.joinStorage, stgErr = store.MakeJoinStorage[K](streamConfig.JoinStorage, env, stgCfg)
		if stgErr != nil {
			return nil, stgErr
		}
	}
	env.RegisterStorage(multiJoinStream.joinStorage)
	multiJoinStream.f.stream = multiJoinStream
	if err := leftStream.SetConsumer(multiJoinStream); err != nil {
		return nil, err
	}
	env.RegisterStream(multiJoinStream)

	return multiJoinStream, nil
}

func (s *MultiJoinStream[K, T, R]) consume(ctx context.Context, key K, index int, value interface{}) {
	s.joinStorage.JoinValue(ctx, key, index, value, func(values [][]interface{}) bool {
		if len(values) > 0 && len(values[0]) > 0 {
			return s.f.call(ctx, key, values, s)
		}
		return false
	})
}

func (s *MultiJoinStream[K, T, R]) addLink(link multiJoinLinkStream) int {
	index := len(s.links) + 1
	s.links = append(s.links, link)
	return index
}

func (s *MultiJoinStream[K, T, R]) FunctionImplementation() interface{} {
	return s.f.f
}

func (s *MultiJoinStream[K, T, R]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *MultiJoinStream[K, T, R]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *MultiJoinStream[K, T, R]) GetKeyType() reflect.Type {
	var k K
	return reflect.TypeOf(&k).Elem()
}

func (s *MultiJoinStream[K, T, R]) Stream() runtime.Stream {
	return s
}

func (s *MultiJoinStream[K, T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) error {
	return s.SetDownstream(consumer, s)
}

func (s *MultiJoinStream[K, T, R]) Consume(ctx context.Context, value datastruct.KeyValue[K, T]) {
	if s.TracingEnabled(ctx) {
		newCtx, span := s.StartSpan(ctx, "stream.join")
		ctx = newCtx
		defer span.End()
	}
	s.consume(ctx, value.Key, 0, value.Value)
}

func (s *MultiJoinStream[K, T, R]) ConsumeRight(ctx context.Context, index int, value datastruct.KeyValue[K, interface{}]) {
	s.consume(ctx, value.Key, index, value.Value)
}

func (s *MultiJoinStream[K, T, R]) Out(ctx context.Context, value R) {
	s.Emit(ctx, value)
}
