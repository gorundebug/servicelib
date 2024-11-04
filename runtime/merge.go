/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/config"
)

type MergeStream[T any] struct {
	ConsumedStream[T]
	links []*MergeLink[T]
}

type MergeLink[T any] struct {
	mergeStream *MergeStream[T]
	source      TypedStream[T]
	index       int
}

func mergeLink[T any](index int, mergeSteam *MergeStream[T], stream TypedStream[T]) *MergeLink[T] {
	mergeLink := &MergeLink[T]{
		mergeStream: mergeSteam,
		source:      stream,
		index:       index,
	}
	stream.SetConsumer(mergeLink)
	return mergeLink
}

func (s *MergeLink[T]) Consume(value T) {
	s.mergeStream.Consume(value)
}

func (s *MergeLink[T]) GetId() int {
	return s.mergeStream.GetId()
}

func (s *MergeLink[T]) GetName() string {
	return s.mergeStream.GetName()
}

func (s *MergeLink[T]) GetEnvironment() ServiceExecutionEnvironment {
	return s.mergeStream.GetEnvironment()
}

func (s *MergeLink[T]) GetConfig() *config.StreamConfig {
	return s.mergeStream.GetConfig()
}

func (s *MergeLink[T]) GetConsumers() []Stream {
	return s.mergeStream.GetConsumers()
}

func (s *MergeLink[T]) GetTransformationName() string {
	return s.mergeStream.GetTransformationName()
}

func (s *MergeLink[T]) GetTypeName() string {
	return s.mergeStream.GetTypeName()
}

func MakeMergeStream[T any](name string, stream TypedStream[T], streams ...TypedStream[T]) *MergeStream[T] {
	env := stream.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	ser := stream.GetSerde()

	mergeStream := &MergeStream[T]{
		ConsumedStream: ConsumedStream[T]{
			ServiceStream: ServiceStream[T]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: ser,
		},
	}
	runtime.registerStream(mergeStream)
	mergeStream.links = make([]*MergeLink[T], len(streams)+1)
	mergeStream.links[0] = mergeLink[T](0, mergeStream, stream)
	for i, s := range streams {
		mergeStream.links[i+1] = mergeLink[T](i+1, mergeStream, s)
	}
	return mergeStream
}

func (s *MergeStream[T]) Consume(value T) {
	if s.caller != nil {
		s.caller.Consume(value)
	}
}
