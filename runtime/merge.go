/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	log "github.com/sirupsen/logrus"
)

type MergeStream[T any] struct {
	*ConsumedStream[T]
}

type MergeLink[T any] struct {
	mergeStream *MergeStream[T]
	source      TypedStream[T]
	index       int
}

func mergeLink[T any](index int, mergeSteam *MergeStream[T], stream TypedStream[T]) *MergeLink[T] {
	mergeLink := MergeLink[T]{
		mergeStream: mergeSteam,
		source:      stream,
		index:       index,
	}
	return &mergeLink
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

func (s *MergeLink[T]) GetRuntime() StreamExecutionRuntime {
	return s.mergeStream.GetRuntime()
}

func (s *MergeLink[T]) GetConfig() *config.StreamConfig {
	return s.mergeStream.GetConfig()
}

func (s *MergeLink[T]) getConsumers() []Stream {
	return s.mergeStream.getConsumers()
}

func (s *MergeLink[T]) GetTransformationName() string {
	return s.mergeStream.GetTransformationName()
}

func (s *MergeLink[T]) GetTypeName() string {
	return s.mergeStream.GetTypeName()
}

func MakeMergeStream[T any](name string, streams ...TypedStream[T]) *MergeStream[T] {
	runtime := streams[0].GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	var ser serde.StreamSerde[T]
	if len(streams) > 0 {
		ser = streams[0].GetSerde()
	}
	mergeStream := &MergeStream[T]{
		ConsumedStream: &ConsumedStream[T]{
			StreamBase: &StreamBase[T]{
				runtime: runtime,
				config:  streamConfig,
			},
			serde: ser,
		},
	}
	runtime.registerStream(mergeStream)
	for index, stream := range streams {
		link := mergeLink[T](index, mergeStream, stream)
		stream.SetConsumer(link)
	}
	return mergeStream
}
