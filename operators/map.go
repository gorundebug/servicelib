/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"context"
	"reflect"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

var _ runtime.TypedTransformConsumedStream[any, any] = (*MapStream[any, any])(nil)

type MapFunctionContext[T, R any] struct {
	runtime.StreamFunction[R]
	stream runtime.TypedStream[R]
	f      MapFunction[T, R]
}

func (f *MapFunctionContext[T, R]) call(ctx context.Context, value T, out runtime.Collect[R]) {
	f.BeforeCall()
	defer f.AfterCall()
	f.f.Map(ctx, f.stream, value, out)
}

type MapStream[T, R any] struct {
	runtime.ConsumedStream[R]
	source runtime.TypedStream[T]
	f      MapFunctionContext[T, R]
}

func MakeMapStream[T, R any](streamConfig *config.MapStreamConfig, stream runtime.TypedStream[T], f MapFunction[T, R]) (runtime.TypedTransformConsumedStream[T, R], error) {
	env := stream.GetRuntimeEnvironment()
	mapStream := &MapStream[T, R]{
		ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
		source:         stream,
		f: MapFunctionContext[T, R]{
			stream: nil,
			f:      f,
		},
	}
	mapStream.f.stream = mapStream
	if err := stream.SetConsumer(mapStream); err != nil {
		return nil, err
	}
	env.RegisterStream(mapStream)
	return mapStream, nil
}

func (s *MapStream[T, R]) FunctionImplementation() interface{} {
	return s.f.f
}

func (s *MapStream[T, R]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *MapStream[T, R]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *MapStream[T, R]) GetKeyType() reflect.Type {
	return nil
}

func (s *MapStream[T, R]) Stream() runtime.Stream {
	return s
}

func (s *MapStream[T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) error {
	return s.SetDownstream(consumer, s)
}

func (s *MapStream[T, R]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.map")
	defer span.End()
	s.f.call(ctx, value, s)
}

func (s *MapStream[T, R]) Out(ctx context.Context, value R) {
	s.Emit(ctx, value)
}
