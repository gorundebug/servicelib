/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
	"reflect"
)

type FlatMapIterableStream[T, R any] struct {
	*ConsumedStream[R]
	serdeIn serde.StreamSerde[T]
	source  TypedStream[T]
}

func MakeFlatMapIterableStream[T, R any](name string, stream TypedStream[T]) *FlatMapIterableStream[T, R] {
	tpT := reflect.TypeOf((*T)(nil)).Elem()
	tpR := reflect.TypeOf((*R)(nil)).Elem()
	if tpT.Kind() != reflect.Array && tpT.Kind() != reflect.Slice {
		log.Fatalf("Type %s is not an array or slice", tpR.Name())
	}
	tpE := tpT.Elem()
	if tpE != tpR {
		log.Fatalf("Element type %s does not equals to type %s", tpE.Name(), tpR.Name())
	}
	runtime := stream.GetRuntime()
	cfg := runtime.GetConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		log.Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	flatMapStreamIterable := &FlatMapIterableStream[T, R]{
		ConsumedStream: &ConsumedStream[R]{
			Stream: &Stream[R]{
				runtime: runtime,
				config:  *streamConfig,
			},
			serde: MakeSerde[R](runtime),
		},
		serdeIn: stream.GetSerde(),
	}
	stream.setConsumer(flatMapStreamIterable)
	runtime.registerStream(flatMapStreamIterable)
	return flatMapStreamIterable
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
