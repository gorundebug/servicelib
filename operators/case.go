/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/environment"
	"reflect"
	"strconv"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
)

var _ runtime.TypedWhenStream[any] = (*WhenStream[any, any])(nil)
var _ runtime.TypedCaseStream[any] = (*CaseStream[any])(nil)

type WhenStream[T, R any] struct {
	streamLink
	caseStream runtime.TypedCaseStream[T]
	index      int
	caller     runtime.Caller[R]
	consumer   runtime.TypedStreamConsumer[R]
	serde      serde.StreamSerde[R]
}

func (s *WhenStream[T, R]) GetName() string {
	return s.caseStream.GetName() + "CaseLink" + strconv.Itoa(s.index)
}

func (s *WhenStream[T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) {
	s.consumer = consumer
	s.caller = runtime.MakeCaller[R](s)
}

func (s *WhenStream[T, R]) GetSerde() serde.StreamSerde[R] {
	return s.serde
}

func (s *WhenStream[T, R]) Consume(ctx context.Context, value R) {
	if s.caller != nil {
		s.caller.Consume(ctx, value)
	}
}

func (s *WhenStream[T, R]) GetWhenConsumer() runtime.Stream {
	return s.GetConsumer()
}

func (s *WhenStream[T, R]) GetConsumers() []runtime.Stream {
	if s.consumer == nil {
		return nil
	}
	return []runtime.Stream{s.consumer}
}

func (s *WhenStream[T, R]) ConsumeCase(ctx context.Context, value any) {
	s.Consume(ctx, value.(R))
}

func (s *WhenStream[T, R]) GetConsumer() runtime.TypedStreamConsumer[R] {
	return s.consumer
}

func (s *WhenStream[T, R]) SetIndex(index int) {
	s.index = index
}

func (s *WhenStream[T, R]) GetType() reflect.Type {
	return reflect.TypeOf((*R)(nil)).Elem()
}

func (s *WhenStream[T, R]) GetEnvironment() environment.ServiceEnvironment {
	return s.caseStream.GetEnvironment()
}

func (s *WhenStream[T, R]) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
	return s.caseStream.GetRuntimeEnvironment()
}

func (s *WhenStream[T, R]) FunctionImplementation() interface{} {
	return nil
}

func (s *WhenStream[T, R]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *WhenStream[T, R]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *WhenStream[T, R]) GetKeyType() reflect.Type {
	return nil
}

func (s *WhenStream[T, R]) Stream() runtime.Stream {
	return s
}

func MakeWhenStream[T, R any](caseStream runtime.TypedCaseStream[T]) (runtime.TypedWhenStream[R], error) {
	whenStream := &WhenStream[T, R]{
		streamLink: streamLink{stream: caseStream},
		caseStream: caseStream,
		serde:      runtime.MakeSerde[R](caseStream.GetRuntimeEnvironment()),
	}
	if err := caseStream.AddStream(whenStream); err != nil {
		return nil, err
	}
	return whenStream, nil
}

type CaseStream[T any] struct {
	runtime.ConsumedStream[T]
	whenStreams []runtime.WhenStream
	source      runtime.TypedStream[T]
	buildSwitch BuildSwitchFunction[T]
	whenFunc    func(T) int
}

// MakeCaseStream
// Custom buildSwitch function must guarantee that returned index matches the expected type R, otherwise panic will occur.
func MakeCaseStream[T any](streamConfig *config.CaseStreamConfig, stream runtime.TypedStream[T], buildSwitch BuildSwitchFunction[T]) (runtime.TypedCaseStream[T], error) {
	env := stream.GetRuntimeEnvironment()
	caseStream := &CaseStream[T]{

		ConsumedStream: runtime.MakeConsumedStream[T](streamConfig.ID, env, stream.GetSerde()),
		source:         stream,
		whenStreams:    nil,
		buildSwitch:    buildSwitch,
		whenFunc:       func(T) int { panic("when function not implemented") },
	}
	env.RegisterStream(caseStream)
	stream.SetConsumer(caseStream)
	return caseStream, nil
}

func (s *CaseStream[T]) FunctionImplementation() interface{} {
	return s.buildSwitch
}

func (s *CaseStream[T]) GetErrorConsumer() runtime.RuntimeStream {
	return nil
}

func (s *CaseStream[T]) GetValueType() reflect.Type {
	var t T
	return reflect.TypeOf(&t).Elem()
}

func (s *CaseStream[T]) GetKeyType() reflect.Type {
	return nil
}

func (s *CaseStream[T]) Stream() runtime.Stream {
	return s
}

func (s *CaseStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
	s.SetDownstream(consumer, s)
}

func (s *CaseStream[T]) AddStream(stream runtime.WhenStream) error {
	stream.SetIndex(len(s.whenStreams))
	s.whenStreams = append(s.whenStreams, stream)
	return nil
}

func (s *CaseStream[T]) Consume(ctx context.Context, value T) {
	ctx, span := s.StartSpan(ctx, "stream.case")
	defer span.End()
	index := s.whenFunc(value)
	s.whenStreams[index].ConsumeCase(ctx, value)
}

func (s *CaseStream[T]) GetConsumers() []runtime.Stream {
	var consumers = make([]runtime.Stream, 0, len(s.whenStreams))
	for _, stream := range s.whenStreams {
		consumers = append(consumers, stream.GetWhenConsumer())
	}
	return consumers
}

func (s *CaseStream[T]) Build() error {
	whenItems := make([]When, 0, len(s.whenStreams))
	for _, stream := range s.whenStreams {
		whenItems = append(whenItems, stream)
	}
	var err error
	s.whenFunc, err = s.buildSwitch.BuildSwitch(s, whenItems)
	return err
}
