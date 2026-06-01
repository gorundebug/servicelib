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

var _ runtime.TypedConsumedStream[any] = (*MergeStream[any])(nil)

type MergeStream[T any] struct {
	runtime.ConsumedStream[T]
	links []*MergeLink[T]
}

type MergeLink[T any] struct {
	streamLink
	mergeStream *MergeStream[T]
	source      runtime.TypedStream[T]
	index       int
}

func mergeLink[T any](index int, mergeStream *MergeStream[T], stream runtime.TypedStream[T]) *MergeLink[T] {
	link := &MergeLink[T]{
		streamLink:  streamLink{stream: mergeStream},
		mergeStream: mergeStream,
		source:      stream,
		index:       index,
	}
	stream.SetConsumer(link)
	return link
}

func (s *MergeLink[T]) Consume(ctx context.Context, value T) {
	s.mergeStream.Consume(ctx, value)
}

func MakeMergeStream[T any](streamConfig *config.MergeStreamConfig, stream runtime.TypedStream[T], streams ...runtime.TypedStream[T]) (runtime.TypedConsumedStream[T], error) {
	env := stream.GetRuntimeEnvironment()
	ser := stream.GetSerde()

	mergeStream := &MergeStream[T]{
		ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, ser),
	}
	env.RegisterStream(mergeStream)
	mergeStream.links = make([]*MergeLink[T], len(streams)+1)
	mergeStream.links[0] = mergeLink[T](0, mergeStream, stream)
	for i, s := range streams {
		mergeStream.links[i+1] = mergeLink[T](i+1, mergeStream, s)
	}
	return mergeStream, nil
}

func (s *MergeStream[T]) FunctionImplementation() interface{} {
	return nil
}

func (s *MergeStream[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *MergeStream[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *MergeStream[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *MergeStream[T]) Stream() runtime.Stream {
	return s
}

func (s *MergeStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
	s.SetDownstream(consumer, s)
}

func (s *MergeStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.merge")
	defer span.End()
	s.Emit(ctx, value)
}
