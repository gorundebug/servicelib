/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"reflect"
)

const uintSize = bits.UintSize / 8

type Serializer interface{}

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
}

type streamSerde[T any] struct {
	serde Serde[T]
}

func (s *streamSerde[T]) Serialize(value T) ([]byte, error) {
	return s.serde.Serialize(value)
}

func (s *streamSerde[T]) Deserialize(data []byte) (T, error) {
	return s.serde.Deserialize(data)
}

func (s *streamSerde[T]) IsKeyValue() bool {
	return false
}

type streamKeyValueSerde[K comparable, V any] struct {
	serdeKey   Serde[K]
	serdeValue Serde[V]
}

func (s *streamKeyValueSerde[K, V]) SerializeKey(kv KeyValue[K, V]) ([]byte, error) {
	return s.serdeKey.Serialize(kv.Key)
}

func (s *streamKeyValueSerde[K, V]) SerializeValue(kv KeyValue[K, V]) ([]byte, error) {
	return s.serdeValue.Serialize(kv.Value)
}

func (s *streamKeyValueSerde[K, V]) Serialize(kv KeyValue[K, V]) ([]byte, error) {
	if keyBytes, err := s.serdeKey.Serialize(kv.Key); err == nil {
		lBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(lBytes, uint32(len(keyBytes)))
		if valueBytes, err := s.serdeValue.Serialize(kv.Value); err == nil {
			return append(append(lBytes, keyBytes...), valueBytes...), nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (s *streamKeyValueSerde[K, V]) Deserialize(data []byte) (KeyValue[K, V], error) {
	var kv KeyValue[K, V]
	keyLen := binary.LittleEndian.Uint32(data)
	var err error
	kv.Key, err = s.serdeKey.Deserialize(data[4 : keyLen+4])
	if err != nil {
		return kv, err
	}
	kv.Value, err = s.serdeValue.Deserialize(data[keyLen+4:])
	return kv, err
}

func (s *streamKeyValueSerde[K, V]) IsKeyValue() bool {
	return true
}

func makeStreamSerde[T any](serde Serde[T]) StreamSerde[T] {
	return &streamSerde[T]{serde: serde}
}

func makeStreamKeyValueSerde[K comparable, V any](serdeKey Serde[K], serdeValue Serde[V]) StreamKeyValueSerde[KeyValue[K, V]] {
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

func IsTypePtr[T any]() bool {
	tp := reflect.TypeOf((*T)(nil)).Elem()
	if tp.Kind() == reflect.Ptr {
		return true
	}
	return false
}

type StringSerde struct {
}

func (s *StringSerde) Serialize(value string) ([]byte, error) {
	length := len(value)
	data := make([]byte, uintSize+length)
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data, uint32(length))
	} else {
		binary.LittleEndian.PutUint64(data, uint64(length))
	}
	copy(data[:uintSize], value)
	return data, nil
}

func (s *StringSerde) Deserialize(data []byte) (string, error) {
	if len(data) < uintSize {
		return "", fmt.Errorf("deserialization error StringSerde.Deserialize")
	}
	var length int
	if uintSize == 4 {
		length = int(binary.LittleEndian.Uint32(data))
	} else {
		length = int(binary.LittleEndian.Uint64(data))
	}
	if len(data) < uintSize+length {
		return "", fmt.Errorf("deserialization error StringSerde.Deserialize")
	}
	return string(data[uintSize : uintSize+length]), nil
}

type UIntSerde struct {
}

func (s *UIntSerde) Serialize(value uint) ([]byte, error) {
	data := make([]byte, uintSize)
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data, uint32(value))
	} else {
		binary.LittleEndian.PutUint64(data, uint64(value))
	}
	return data, nil
}

func (s *UIntSerde) Deserialize(data []byte) (uint, error) {
	if len(data) < uintSize {
		return 0, fmt.Errorf("deserialization error UIntSerde.Deserialize")
	}
	if uintSize == 4 {
		return uint(binary.LittleEndian.Uint32(data)), nil
	} else {
		return uint(binary.LittleEndian.Uint64(data)), nil
	}
}

type UInt8Serde struct {
}

func (s *UInt8Serde) Serialize(value uint8) ([]byte, error) {
	return []byte{value}, nil
}

func (s *UInt8Serde) Deserialize(data []byte) (uint8, error) {
	if len(data) < 1 {
		return 0, fmt.Errorf("serialization error UInt8Serde.Deserialize")
	}
	return data[0], nil
}

type UInt16Serde struct {
}

func (s *UInt16Serde) Serialize(value uint16) ([]byte, error) {
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, value)
	return data, nil
}

func (s *UInt16Serde) Deserialize(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("deserialization error UInt16Serde.Deserialize")
	}
	return binary.LittleEndian.Uint16(data), nil
}

type UInt32Serde struct {
}

func (s *UInt32Serde) Serialize(value uint32) ([]byte, error) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, value)
	return data, nil
}

func (s *UInt32Serde) Deserialize(data []byte) (uint32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("deserialization error UInt32Serde.Deserialize")
	}
	return binary.LittleEndian.Uint32(data), nil
}

type UInt64Serde struct {
}

func (s *UInt64Serde) Serialize(value uint64) ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return data, nil
}

func (s *UInt64Serde) Deserialize(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("deserialization error UInt64Serde.Deserialize")
	}
	return binary.LittleEndian.Uint64(data), nil
}

type IntSerde struct {
}

func (s *IntSerde) Serialize(value int) ([]byte, error) {
	data := make([]byte, uintSize)
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data, uint32(value))
	} else {
		binary.LittleEndian.PutUint64(data, uint64(value))
	}
	return data, nil
}

func (s *IntSerde) Deserialize(data []byte) (int, error) {
	if len(data) < uintSize {
		return 0, fmt.Errorf("deserialization error IntSerde.Deserialize")
	}
	if uintSize == 4 {
		return int(binary.LittleEndian.Uint32(data)), nil
	} else {
		return int(binary.LittleEndian.Uint64(data)), nil
	}
}

type Int8Serde struct {
}

func (s *Int8Serde) Serialize(value int8) ([]byte, error) {
	return []byte{byte(value)}, nil
}

func (s *Int8Serde) Deserialize(data []byte) (int8, error) {
	if len(data) < 1 {
		return 0, fmt.Errorf("deserialization error UInt8Serde.Deserialize")
	}
	return int8(data[0]), nil
}

type Int16Serde struct {
}

func (s *Int16Serde) Serialize(value int16) ([]byte, error) {
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, uint16(value))
	return data, nil
}

func (s *Int16Serde) Deserialize(data []byte) (int16, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("deserialization error Int16Serde.Deserialize")
	}
	return int16(binary.LittleEndian.Uint16(data)), nil
}

type Int32Serde struct {
}

func (s *Int32Serde) Serialize(value int32) ([]byte, error) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(value))
	return data, nil
}

func (s *Int32Serde) Deserialize(data []byte) (int32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("deserialization error Int32Serde.Deserialize")
	}
	return int32(binary.LittleEndian.Uint32(data)), nil
}

type Int64Serde struct {
}

func (s *Int64Serde) Serialize(value int64) ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(value))
	return data, nil
}

func (s *Int64Serde) Deserialize(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("deserialization error Int64Serde.Deserialize")
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

type BoolSerde struct {
}

func (s *BoolSerde) Serialize(value bool) ([]byte, error) {
	if value {
		return []byte{1}, nil
	} else {
		return []byte{0}, nil
	}
}

func (s *BoolSerde) Deserialize(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, fmt.Errorf("serialization error BoolSerde.Deserialize")
	}
	return data[0] != 0, nil
}

type RuneSerde struct {
}

func (s *RuneSerde) Serialize(value rune) ([]byte, error) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(value))
	return data, nil
}

func (s *RuneSerde) Deserialize(data []byte) (rune, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("deserialization error RuneSerde.Deserialize")
	}
	return int32(binary.LittleEndian.Uint32(data)), nil
}

type Float32Serde struct {
}

func (s *Float32Serde) Serialize(value float32) ([]byte, error) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(value))
	return data, nil
}

func (s *Float32Serde) Deserialize(data []byte) (float32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("deserialization error Float32Serde.Deserialize")
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(data)), nil
}

type Float64Serde struct {
}

func (s *Float64Serde) Serialize(value float64) ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(value))
	return data, nil
}

func (s *Float64Serde) Deserialize(data []byte) (float64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("deserialization error Float64Serde.Deserialize")
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil
}

func makeDefaultSerde(valueType reflect.Type) (Serializer, error) {
	switch valueType {
	case GetSerdeType[int]():
		{
			var serde Serde[int] = &IntSerde{}
			return serde, nil
		}
	case GetSerdeType[int8]():
		{
			var serde Serde[int8] = &Int8Serde{}
			return serde, nil
		}
	case GetSerdeType[int16]():
		{
			var serde Serde[int16] = &Int16Serde{}
			return serde, nil
		}
	case GetSerdeType[int32]():
		{
			var serde Serde[int32] = &Int32Serde{}
			return serde, nil
		}
	case GetSerdeType[int64]():
		{
			var serde Serde[int64] = &Int64Serde{}
			return serde, nil
		}
	case GetSerdeType[uint]():
		{
			var serde Serde[uint] = &UIntSerde{}
			return serde, nil
		}
	case GetSerdeType[uint8]():
		{
			var serde Serde[uint8] = &UInt8Serde{}
			return serde, nil
		}
	case GetSerdeType[uint16]():
		{
			var serde Serde[uint16] = &UInt16Serde{}
			return serde, nil
		}
	case GetSerdeType[uint32]():
		{
			var serde Serde[uint32] = &UInt32Serde{}
			return serde, nil
		}
	case GetSerdeType[uint64]():
		{
			var serde Serde[uint64] = &UInt64Serde{}
			return serde, nil
		}
	case GetSerdeType[string]():
		{
			var serde Serde[string] = &StringSerde{}
			return serde, nil
		}
	case GetSerdeType[bool]():
		{
			var serde Serde[bool] = &BoolSerde{}
			return serde, nil
		}
	case GetSerdeType[rune]():
		{
			var serde Serde[rune] = &RuneSerde{}
			return serde, nil
		}
	case GetSerdeType[float32]():
		{
			var serde Serde[float32] = &Float32Serde{}
			return serde, nil
		}
	case GetSerdeType[float64]():
		{
			var serde Serde[float64] = &Float64Serde{}
			return serde, nil
		}
	}
	return nil, fmt.Errorf("makeDefaultSerde unsupported type: %v", valueType)
}

type SerdeStub[T any] struct {
}

func (s *SerdeStub[T]) Serialize(T) ([]byte, error) {
	return []byte{}, nil
}

func (s *SerdeStub[T]) Deserialize([]byte) (T, error) {
	var t T
	return t, nil
}

func MakeStubSerde[T any]() *SerdeStub[T] {
	return &SerdeStub[T]{}
}
