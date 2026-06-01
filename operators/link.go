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
	"github.com/gorundebug/servicelib/runtime/serde"
)

var _ runtime.TypedLinkStream[any] = (*LinkStream[any])(nil)

type LinkStream[T any] struct {
	runtime.ConsumedStream[T]
	source runtime.TypedConsumedStream[T]
	serde  serde.StreamSerde[T]
}

func MakeLinkStream[T any](streamConfig config.StreamConfig, env runtime.RuntimeEnvironment) (runtime.TypedLinkStream[T], error) {
	linkStream := &LinkStream[T]{
		ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.GetID(), env, nil),
	}
	env.RegisterStream(linkStream)
	return linkStream, nil
}

func (s *LinkStream[T]) FunctionImplementation() interface{} {
	return nil
}

func (s *LinkStream[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *LinkStream[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *LinkStream[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *LinkStream[T]) Stream() runtime.Stream {
	return s
}

func (s *LinkStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
	s.SetDownstream(consumer, s)
}

func (s *LinkStream[T]) GetSerde() serde.StreamSerde[T] {
	return s.serde
}

func (s *LinkStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.link")
	defer span.End()
	s.Emit(ctx, value)
}

func (s *LinkStream[T]) SetSource(stream runtime.TypedConsumedStream[T]) {
	s.source = stream
	s.serde = stream.GetSerde()
	stream.SetConsumer(s)
}
