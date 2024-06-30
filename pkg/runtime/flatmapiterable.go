/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"reflect"
)

type FlatMapIterableStream[T []any | string, R any] struct {
	ConsumedStream[R]
}

func FlatMapIterable[T []any | string, R any](name string, stream TypedStream[T]) *FlatMapIterableStream[T, R] {
	runtime := stream.GetRuntime()
	config := runtime.GetConfig()
	streamConfig := config.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Panicf("Config for the stream with name=%s does not exists", name)
	}
	flatMapStreamIterable := FlatMapIterableStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			Stream: Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
		},
	}
	stream.setConsumer(&flatMapStreamIterable)
	runtime.registerStream(&flatMapStreamIterable)
	return &flatMapStreamIterable
}

func isRuneType(value interface{}) bool {
	switch value.(type) {
	case rune:
		return true
	}
	return false
}

func (s *FlatMapIterableStream[T, R]) Consume(value T) {
	var r R
	if s.caller != nil {
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.String && isRuneType(r) {
			var intf interface{}
			intf = value
			str := intf.(string)
			for _, v := range str {
				s.caller.Consume(reflect.ValueOf(v).Interface().(R))
			}
		} else {
			l := val.Len()
			for i := 0; i < l; i++ {
				s.caller.Consume(val.Index(i).Interface().(R))
			}
		}
	}
}
