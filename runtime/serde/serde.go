/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package serde

import (
	"github.com/gorundebug/servicelib/runtime/datastruct"
	log "github.com/sirupsen/logrus"
	"reflect"
)

type Serializer interface {
	SerializeObj(interface{}, []byte) ([]byte, error)
	DeserializeObj([]byte) (interface{}, error)
	IsStub() bool
}

type Serde[T any] interface {
	Serializer
	Serialize(T, []byte) ([]byte, error)
	Deserialize([]byte) (T, error)
}

type StreamSerializer interface {
	IsKeyValue() bool
}

type StreamSerde[T any] interface {
	StreamSerializer
	ValueSerializer() Serializer
	Serialize(T) ([]byte, error)
	Deserialize([]byte) (T, error)
}

type StreamKeyValueSerde[T any] interface {
	StreamSerde[T]
	SerializeKey(T) ([]byte, error)
	SerializeValue(T) ([]byte, error)
	KeySerializer() Serializer
	DeserializeKeyValue([]byte, []byte) (T, error)
}

func MakeTypedArraySerde[T any](valueSerde Serializer) *ArraySerde[T] {
	var t T
	v := reflect.ValueOf(t)
	if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
		log.Fatalf("expected array or slice, got %d", v.Kind())
	}
	return &ArraySerde[T]{arraySerde: arraySerde{valueSerde: valueSerde, arrayType: GetSerdeType[T]()}}
}

func MakeStreamSerde[T any](serde Serde[T]) StreamSerde[T] {
	return &streamSerde[T]{serde: serde}
}

func MakeStreamKeyValueSerde[K comparable, V any](serdeKey Serde[K], serdeValue Serde[V]) StreamKeyValueSerde[datastruct.KeyValue[K, V]] {
	return &streamKeyValueSerde[K, V]{
		serdeKey:   serdeKey,
		serdeValue: serdeValue,
	}
}

func GetSerdeType[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

func GetSerdeTypeWithoutPtr[T any]() reflect.Type {
	tp := reflect.TypeOf((*T)(nil)).Elem()
	for {
		if tp.Kind() == reflect.Ptr {
			tp = tp.Elem()
		} else {
			break
		}
	}
	return tp
}

func GetSerdeTypeName[T any]() string {
	name := ""
	tp := reflect.TypeOf((*T)(nil)).Elem()
	for {
		if tp.Kind() == reflect.Ptr {
			tp = tp.Elem()
			name = "*" + name
		} else {
			name = name + tp.Name()
			break
		}
	}
	return name
}

func MakeStubSerde[T any]() *StubSerde[T] {
	return &StubSerde[T]{}
}

func MakeArraySerde(arrayType reflect.Type, valueSerde Serializer) Serializer {
	return &arraySerde{
		arrayType:  arrayType,
		valueSerde: valueSerde,
	}
}

func MakeTypedMapSerde[T any](keyArraySerde Serializer, valueArraySerde Serializer) Serde[T] {
	var t T
	v := reflect.ValueOf(t)
	if v.Kind() != reflect.Map {
		log.Fatalf("expected map, got %d", v.Kind())
	}
	return &MapSerde[T]{
		mapSerde: mapSerde{
			keyArraySerde:   keyArraySerde,
			valueArraySerde: valueArraySerde,
			mapType:         GetSerdeType[T](),
		},
	}
}

func MakeMapSerde(mapType reflect.Type,
	keyArraySerde Serializer,
	valueArraySerde Serializer) Serializer {
	return &mapSerde{
		mapType:         mapType,
		keyArraySerde:   keyArraySerde,
		valueArraySerde: valueArraySerde,
	}
}

func MakeDefaultSerde(valueType reflect.Type) (Serializer, error) {
	return makeDefaultSerde(valueType)
}
