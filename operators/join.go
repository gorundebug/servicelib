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

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/store"
)

var _ runtime.TypedJoinConsumedStream[int, any, any, any] = (*JoinStream[int, any, any, any])(nil)

type JoinFunctionContext[K comparable, T1, T2, R any] struct {
	runtime.StreamFunction[R]
	stream runtime.TypedStream[R]
	f      JoinFunction[K, T1, T2, R]
}

func (f *JoinFunctionContext[K, T1, T2, R]) call(ctx context.Context, key K, leftValue []T1, rightValue []T2, out runtime.Collect[R]) bool {
	f.BeforeCall()
	defer f.AfterCall()
	result := f.f.Join(ctx, f.stream, key, leftValue, rightValue, out)
	return result
}

type JoinLink[K comparable, T1, T2, R any] struct {
	streamLink
	joinStream *JoinStream[K, T1, T2, R]
	source     runtime.TypedStream[datastruct.KeyValue[K, T2]]
}

func joinLink[K comparable, T1, T2, R any](joinStream *JoinStream[K, T1, T2, R], stream runtime.TypedStream[datastruct.KeyValue[K, T2]]) *JoinLink[K, T1, T2, R] {
	joinLink := &JoinLink[K, T1, T2, R]{
		streamLink: streamLink{stream: joinStream},
		joinStream: joinStream,
		source:     stream,
	}
	stream.SetConsumer(joinLink)
	return joinLink
}

func (s *JoinLink[K, T1, T2, R]) Consume(ctx context.Context, value datastruct.KeyValue[K, T2]) {
	s.joinStream.ConsumeRight(ctx, value)
}

type JoinStream[K comparable, T1, T2, R any] struct {
	runtime.ConsumedStream[R]
	f           JoinFunctionContext[K, T1, T2, R]
	source      runtime.TypedStream[datastruct.KeyValue[K, T1]]
	joinStorage store.JoinStorage[K]
	joinType    api.JoinType
	joinLink    *JoinLink[K, T1, T2, R]
}

func (s *JoinStream[K, T1, T2, R]) consume(ctx context.Context, key K, index int, value interface{}) {
	s.joinStorage.JoinValue(ctx, key, index, value, func(values [][]interface{}) bool {
		canCall := false
		switch s.joinType {
		case api.JoinTypeInner:
			canCall = len(values) > 1 && len(values[0]) != 0 && len(values[1]) != 0
		case api.JoinTypeLeft:
			canCall = len(values) > 0 && len(values[0]) != 0
		case api.JoinTypeRight:
			canCall = len(values) > 1 && len(values[1]) != 0
		case api.JoinTypeOuter:
			canCall = true
		}
		if canCall {
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
			return s.f.call(ctx, key, leftValues, rightValues, s)
		}
		return false
	})
}

func (s *JoinStream[K, T1, T2, R]) ConsumeRight(ctx context.Context, value datastruct.KeyValue[K, T2]) {
	s.consume(ctx, value.Key, 1, value.Value)
}

func (s *JoinStream[K, T1, T2, R]) FunctionImplementation() interface{} {
	return s.f.f
}

func (s *JoinStream[K, T1, T2, R]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *JoinStream[K, T1, T2, R]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *JoinStream[K, T1, T2, R]) GetKeyType() reflect.Type {
	var k K
	return reflect.TypeOf(&k).Elem()
}

func (s *JoinStream[K, T1, T2, R]) Stream() runtime.Stream {
	return s
}

func (s *JoinStream[K, T1, T2, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
	s.SetDownstream(consumer, s)
}

func (s *JoinStream[K, T1, T2, R]) Consume(ctx context.Context, value datastruct.KeyValue[K, T1]) {
	ctx, span := s.StartSpan(ctx, "stream.join")
	defer span.End()
	s.consume(ctx, value.Key, 0, value.Value)
}

func (s *JoinStream[K, T1, T2, R]) Out(ctx context.Context, value R) {
	s.Emit(ctx, value)
}

type joinStorageConfig struct {
	stream runtime.Stream
}

func (jsc *joinStorageConfig) GetTTL() time.Duration {
	ttl := time.Duration(0)
	cfg := jsc.stream.GetConfig().(*config.JoinStreamConfig)
	if cfg.Ttl != 0 {
		ttl = time.Duration(cfg.Ttl) * time.Millisecond
	}
	return ttl
}

func (jsc *joinStorageConfig) GetRenewTTL() bool {
	renewTTL := false
	cfg := jsc.stream.GetConfig().(*config.JoinStreamConfig)
	if cfg.RenewTTL {
		renewTTL = cfg.RenewTTL
	}
	return renewTTL
}

func (jsc *joinStorageConfig) GetName() string {
	return jsc.stream.GetName()
}

func MakeJoinStream[K comparable, T1, T2, R any](streamConfig *config.JoinStreamConfig, stream runtime.TypedStream[datastruct.KeyValue[K, T1]],
	streamRight runtime.TypedStream[datastruct.KeyValue[K, T2]],
	f JoinFunction[K, T1, T2, R]) (runtime.TypedJoinConsumedStream[K, T1, T2, R], error) {

	env := stream.GetRuntimeEnvironment()
	joinStream := &JoinStream[K, T1, T2, R]{

		ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
		f: JoinFunctionContext[K, T1, T2, R]{
			stream: nil,
			f:      f,
		},
		source:   stream,
		joinType: streamConfig.JoinType,
	}

	stgCfg := &joinStorageConfig{stream: joinStream}
	stg := env.CreateKeyValueJoinStorage(streamConfig.JoinStorage, stgCfg, joinStream)
	if stg != nil {
		var ok bool
		if joinStream.joinStorage, ok = stg.(store.JoinStorage[K]); !ok {
			return nil, fmt.Errorf("JoinStream joinStorage for stream %q is not a JoinStorage[K]", streamConfig.Name)
		}
	} else {
		var stgErr error
		joinStream.joinStorage, stgErr = store.MakeJoinStorage[K](streamConfig.JoinStorage, env, stgCfg)
		if stgErr != nil {
			return nil, stgErr
		}
	}
	env.RegisterStorage(joinStream.joinStorage)
	joinStream.f.stream = joinStream
	stream.SetConsumer(joinStream)
	env.RegisterStream(joinStream)

	joinStream.joinLink = joinLink[K, T1, T2, R](joinStream, streamRight)
	return joinStream, nil
}
