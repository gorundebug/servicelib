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
	SerializeObj(interface{}) ([]byte, error)
	DeserializeObj([]byte) (interface{}, error)
}

type Serde[T any] interface {
	Serializer
	Serialize(T) ([]byte, error)
	Deserialize([]byte) (T, error)
}

type StreamSerializer interface {
	IsKeyValue() bool
}

type StreamSerde[T any] interface {
	StreamSerializer
	Serialize(T) ([]byte, error)
	Deserialize([]byte) (T, error)
}

type StreamKeyValueSerde[T any] interface {
	StreamSerde[T]
	SerializeKey(T) ([]byte, error)
	SerializeValue(T) ([]byte, error)
	KeySerializer() Serializer
	ValueSerializer() Serializer
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

func MakeStubSerde[T any]() *StubSerde[T] {
	return &StubSerde[T]{}
}

func MakeArraySerde(arrayType reflect.Type, valueSerde Serializer) Serializer {
	return &arraySerde{
		arrayType:  arrayType,
		valueSerde: valueSerde,
	}
}

func MakeTypedMapSerde[T any](keySerde Serializer, valueSerde Serializer) Serde[T] {
	var t T
	v := reflect.ValueOf(t)
	if v.Kind() != reflect.Map {
		log.Fatalf("expected map, got %d", v.Kind())
	}
	return &MapSerde[T]{mapSerde: mapSerde{keySerde: keySerde, valueSerde: valueSerde, mapType: GetSerdeType[T]()}}
}

func MakeMapSerde(mapType reflect.Type, keySerde Serializer, valueSerde Serializer) Serializer {
	return &mapSerde{
		mapType:    mapType,
		keySerde:   keySerde,
		valueSerde: valueSerde,
	}
}

func MakeDefaultSerde(valueType reflect.Type) (Serializer, error) {
	return makeDefaultSerde(valueType)
}
