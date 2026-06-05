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
	"strconv"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
)

var _ runtime.TypedSplitStream[any] = (*SplitStream[any])(nil)

type SplitLink[T any] struct {
	streamLink
	splitStream *SplitStream[T]
	index       int
	caller      runtime.Caller[T]
	consumer    runtime.TypedStreamConsumer[T]
}

func (s *SplitLink[T]) GetName() string {
	return s.splitStream.GetName() + "SplitLink" + strconv.Itoa(s.index)
}

func (s *SplitLink[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) error {
	caller, err := runtime.MakeCaller[T](s)
	if err != nil {
		return err
	}
	s.consumer = consumer
	s.caller = caller
	return nil
}

func (s *SplitLink[T]) GetSerde() serde.StreamSerde[T] {
	return s.splitStream.GetSerde()
}

func (s *SplitLink[T]) Consume(ctx context.Context, value T) {
	if s.caller != nil {
		s.caller.Consume(ctx, value)
	}
}

func (s *SplitLink[T]) GetConsumer() runtime.TypedStreamConsumer[T] {
	return s.consumer
}

func (s *SplitLink[T]) GetConsumers() []runtime.Stream {
	if s.consumer == nil {
		return nil
	}
	return []runtime.Stream{s.consumer}
}

func (s *SplitLink[T]) FunctionImplementation() interface{} {
	return nil
}

func (s *SplitLink[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *SplitLink[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *SplitLink[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *SplitLink[T]) Stream() runtime.Stream {
	return s
}

func (s *SplitLink[T]) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
	return s.splitStream.GetRuntimeEnvironment()
}

func splitLink[T any](index int, splitStream *SplitStream[T]) *SplitLink[T] {
	return &SplitLink[T]{
		streamLink:  streamLink{stream: splitStream},
		splitStream: splitStream,
		index:       index,
	}
}

type SplitStream[T any] struct {
	runtime.ConsumedStream[T]
	links  []*SplitLink[T]
	source runtime.TypedStream[T]
}

func MakeSplitStream[T any](streamConfig *config.SplitStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedSplitStream[T], error) {
	env := stream.GetRuntimeEnvironment()
	splitStream := &SplitStream[T]{

		ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, stream.GetSerde()),
		source:         stream,
		links:          make([]*SplitLink[T], 0, 2),
	}
	env.RegisterStream(splitStream)
	if err := stream.SetConsumer(splitStream); err != nil {
		return nil, err
	}
	return splitStream, nil
}

func (s *SplitStream[T]) AddStream() runtime.TypedConsumedStream[T] {
	index := len(s.links)
	link := splitLink[T](index, s)
	s.links = append(s.links, link)
	return link
}

func (s *SplitStream[T]) FunctionImplementation() interface{} {
	return nil
}

func (s *SplitStream[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *SplitStream[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *SplitStream[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *SplitStream[T]) Stream() runtime.Stream {
	return s
}

func (s *SplitStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) error {
	return s.SetDownstream(consumer, s)
}

func (s *SplitStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.split")
	defer span.End()
	for _, link := range s.links {
		link.Consume(ctx, value)
	}
}

func (s *SplitStream[T]) GetConsumers() []runtime.Stream {
	var consumers = make([]runtime.Stream, len(s.links))
	for i := 0; i < len(s.links); i++ {
		consumers[i] = s.links[i].GetConsumer()
	}
	return consumers
}

func (s *SplitStream[T]) Build() error {
	for i := 0; i < len(s.links); i++ {
		if s.links[i].GetConsumer() == nil {
			return fmt.Errorf("link with index %d for the SplitStream %q does not have consumer",
				i, s.GetName())
		}
	}
	return nil
}
