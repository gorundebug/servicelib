/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"reflect"
)

type FlatMapIterableStream[T, R any] struct {
	ConsumedStream[R]
	source TypedStream[T] //nolint:unused
}

func MakeFlatMapIterableStream[T, R any](name string, stream TypedStream[T]) *FlatMapIterableStream[T, R] {
	env := stream.GetEnvironment()
	tpT := reflect.TypeOf((*T)(nil)).Elem()
	tpR := reflect.TypeOf((*R)(nil)).Elem()
	if tpT.Kind() != reflect.Array && tpT.Kind() != reflect.Slice && tpT.Kind() != reflect.String {
		env.Log().Fatalf("Type %s is not an array or slice", tpR.Name())
	}
	if tpT.Kind() != reflect.String {
		tpE := tpT.Elem()
		if tpE != tpR {
			env.Log().Fatalf("Element type %s does not equals to type %s", tpE.Name(), tpR.Name())
		}
	} else if tpR.Kind() != reflect.Int32 && tpR.Kind() != reflect.Uint8 {
		env.Log().Fatalf("Element type %s is not rune or byte", tpR.Name())
	}
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	streamConfig := cfg.GetStreamConfigByName(name)
	if streamConfig == nil {
		env.Log().Fatalf("Config for the stream with name=%s does not exists", name)
		return nil
	}
	flatMapStreamIterable := &FlatMapIterableStream[T, R]{
		ConsumedStream: ConsumedStream[R]{
			ServiceStream: ServiceStream[R]{
				environment: env,
				id:          streamConfig.Id,
			},
			serde: MakeSerde[R](runtime),
		},
	}
	stream.SetConsumer(flatMapStreamIterable)
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
			var intf interface{} = value
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
