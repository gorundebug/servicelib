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

var _ runtime.TypedTransformConsumedStream[any, any] = (*FlatMapStream[any, any])(nil)

type FlatMapFunctionContext[T, R any] struct {
	runtime.StreamFunction[R]
	stream runtime.TypedStream[R]
	f      FlatMapFunction[T, R]
}

func (f *FlatMapFunctionContext[T, R]) call(ctx context.Context, value T, out runtime.Collect[R]) {
	f.BeforeCall()
	defer f.AfterCall()
	f.f.FlatMap(ctx, f.stream, value, out)
}

type FlatMapStream[T, R any] struct {
	runtime.ConsumedStream[R]
	source    runtime.TypedStream[T]
	f         FlatMapFunctionContext[T, R]
	collector runtime.Collect[R]
}

func MakeFlatMapStream[T, R any](streamConfig *config.FlatMapStreamConfig, stream runtime.TypedStream[T], f FlatMapFunction[T, R]) (runtime.TypedTransformConsumedStream[T, R], error) {
	env := stream.GetRuntimeEnvironment()
	flatMapStream := &FlatMapStream[T, R]{

		ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
		source:         stream,
		f: FlatMapFunctionContext[T, R]{
			stream: nil,
			f:      f,
		},
		collector: runtime.MakeCollector[R](nil),
	}
	flatMapStream.f.stream = flatMapStream
	stream.SetConsumer(flatMapStream)
	env.RegisterStream(flatMapStream)
	return flatMapStream, nil
}

func (s *FlatMapStream[T, R]) FunctionImplementation() interface{} {
	return s.f.f
}

func (s *FlatMapStream[T, R]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *FlatMapStream[T, R]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *FlatMapStream[T, R]) GetKeyType() reflect.Type {
	return nil
}

func (s *FlatMapStream[T, R]) Stream() runtime.Stream {
	return s
}

func (s *FlatMapStream[T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
	s.SetDownstream(consumer, s)
	s.collector = s.Collector()
}

func (s *FlatMapStream[T, R]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.flatmap")
	defer span.End()
	s.f.call(ctx, value, s.collector)
}
