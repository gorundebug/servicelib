/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package serde

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	log "github.com/sirupsen/logrus"
	"math"
	"math/bits"
	"reflect"
)

const uintSize = bits.UintSize / 8
const maxSizeLength = uintSize

func fixedSizeTypeDataSize(data any) int {
	switch data.(type) {
	case bool, int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int64, uint64, float64:
		return 8
	}
	return 8
}

func setSize(data []byte, size int) int {
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data, uint32(size))
	} else {
		binary.LittleEndian.PutUint64(data, uint64(size))
	}
	return uintSize
}

func getSize(data []byte) (int, int) {
	if len(data) < uintSize {
		return 0, 0
	}
	if uintSize == 4 {
		return int(binary.LittleEndian.Uint32(data)), 4
	} else {
		return int(binary.LittleEndian.Uint64(data)), 8
	}
}

type streamSerde[T any] struct {
	serde Serde[T]
}

func (s *streamSerde[T]) ValueSerializer() Serializer {
	return s.serde
}

func (s *streamSerde[T]) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("value is not %s", GetSerdeType[T]().Name())
	}
	return s.Serialize(v)
}

func (s *streamSerde[T]) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *streamSerde[T]) Serialize(value T) ([]byte, error) {
	return s.serde.Serialize(value, nil)
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

func (s *streamKeyValueSerde[K, V]) KeySerializer() Serializer {
	return s.serdeKey
}

func (s *streamKeyValueSerde[K, V]) ValueSerializer() Serializer {
	return s.serdeValue
}

func (s *streamKeyValueSerde[K, V]) SerializeKey(kv datastruct.KeyValue[K, V]) ([]byte, error) {
	return s.serdeKey.Serialize(kv.Key, nil)
}

func (s *streamKeyValueSerde[K, V]) SerializeValue(kv datastruct.KeyValue[K, V]) ([]byte, error) {
	return s.serdeValue.Serialize(kv.Value, nil)
}

func (s *streamKeyValueSerde[K, V]) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(datastruct.KeyValue[K, V])
	if !ok {
		return nil, fmt.Errorf("value is not %s", GetSerdeType[datastruct.KeyValue[K, V]]().Name())
	}
	return s.Serialize(v)
}

func (s *streamKeyValueSerde[K, V]) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *streamKeyValueSerde[K, V]) DeserializeKeyValue(key []byte, value []byte) (datastruct.KeyValue[K, V], error) {
	k, err := s.serdeKey.Deserialize(key)
	if err != nil {
		return datastruct.KeyValue[K, V]{}, err
	}
	v, err := s.serdeValue.Deserialize(value)
	if err != nil {
		return datastruct.KeyValue[K, V]{}, err
	}
	return datastruct.KeyValue[K, V]{Key: k, Value: v}, nil
}

func (s *streamKeyValueSerde[K, V]) Serialize(kv datastruct.KeyValue[K, V]) ([]byte, error) {
	data := make([]byte, maxSizeLength)
	if keyData, err := s.serdeKey.Serialize(kv.Key, data); err == nil {
		keyBytesLength := len(keyData) - maxSizeLength
		n := setSize(keyData[:maxSizeLength], keyBytesLength)
		if n != maxSizeLength {
			copy(keyData[n:n+keyBytesLength], keyData[maxSizeLength:maxSizeLength+keyBytesLength])
		}
		buf := bytes.NewBuffer(keyData[:n+keyBytesLength])
		length := buf.Len()
		buf.Grow(length + maxSizeLength)
		if valueBytes, err := s.serdeValue.Serialize(kv.Value, buf.Bytes()[:length+maxSizeLength]); err == nil {
			valueBytesLength := len(valueBytes) - length - maxSizeLength
			n = setSize(valueBytes[length:length+maxSizeLength], valueBytesLength)
			if n != maxSizeLength {
				copy(valueBytes[length+n:length+n+valueBytesLength], valueBytes[length+maxSizeLength:length+maxSizeLength+valueBytesLength])
			}
			return valueBytes[:length+n+valueBytesLength], nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (s *streamKeyValueSerde[K, V]) Deserialize(data []byte) (datastruct.KeyValue[K, V], error) {
	var kv datastruct.KeyValue[K, V]

	var keyLen, n int
	var err error

	if keyLen, n = getSize(data); n == 0 {
		return kv, fmt.Errorf("deserialize key len error streamKeyValueSerde")
	}
	data = data[n:]
	if len(data) < keyLen {
		return kv, fmt.Errorf("deserialize key error streamKeyValueSerde")
	}

	kv.Key, err = s.serdeKey.Deserialize(data[:keyLen])
	if err != nil {
		return kv, err
	}
	data = data[keyLen:]

	var valueLen int
	if valueLen, n = getSize(data); n == 0 {
		return kv, fmt.Errorf("deserialize value len error streamKeyValueSerde")
	}
	data = data[n:]
	if len(data) < valueLen {
		return kv, fmt.Errorf("deserialize value error streamKeyValueSerde")
	}
	kv.Value, err = s.serdeValue.Deserialize(data[:valueLen])
	return kv, err
}

func (s *streamKeyValueSerde[K, V]) IsKeyValue() bool {
	return true
}

func IsTypePtr[T any]() bool {
	tp := reflect.TypeOf((*T)(nil)).Elem()
	return tp.Kind() == reflect.Ptr
}

type BytesSerde struct {
}

func (s *BytesSerde) IsStubSerde() bool {
	return false
}

func (s *BytesSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("value is not []byte")
	}
	return s.Serialize(v, b)
}

func (s *BytesSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *BytesSerde) Serialize(value []byte, b []byte) ([]byte, error) {
	length := len(value)
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + maxSizeLength + length)
	data := buf.Bytes()[:bufLen+maxSizeLength+length]
	bufLen += setSize(data[bufLen:bufLen+maxSizeLength], length)
	copy(data[bufLen:bufLen+length], value)
	return data[:bufLen+length], nil
}

func (s *BytesSerde) Deserialize(data []byte) ([]byte, error) {
	length, n := getSize(data)
	if n == 0 {
		return nil, fmt.Errorf("deserialization error BytesSerde.Deserialize (invalid data length)")
	}
	if len(data) < n+length {
		return nil, fmt.Errorf("deserialization error BytesSerde.Deserialize (invalid data)")
	}
	return data[:n+length], nil
}

type BaseType interface {
	int32 | int64 | int16 | uint32 | uint64 | uint16 | float32 | float64 | bool
}

type FixedSizeTypeArraySerde[T BaseType] struct {
}

func (s *FixedSizeTypeArraySerde[T]) IsStubSerde() bool {
	return false
}

func (s *FixedSizeTypeArraySerde[T]) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.([]T)
	if !ok {
		return nil, fmt.Errorf("value is not []T")
	}
	return s.Serialize(v, b)
}

func (s *FixedSizeTypeArraySerde[T]) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *FixedSizeTypeArraySerde[T]) Serialize(value []T, b []byte) ([]byte, error) {
	var v T

	typeSize := fixedSizeTypeDataSize(v)
	length := len(value)
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + length*typeSize + maxSizeLength)
	data := buf.Bytes()[:bufLen+maxSizeLength+length*typeSize]
	bufLen += setSize(data[bufLen:bufLen+maxSizeLength], length)
	if size, err := binary.Encode(data[bufLen:bufLen+length*typeSize], binary.LittleEndian, value); err != nil {
		return nil, err
	} else {
		bufLen += size
	}
	return data[:bufLen], nil
}

func (s *FixedSizeTypeArraySerde[T]) Deserialize(data []byte) ([]T, error) {
	var v T
	length, n := getSize(data)
	if n == 0 {
		return nil, fmt.Errorf("deserialization error []T.Deserialize (invalid size)")
	}
	if len(data) < fixedSizeTypeDataSize(v)*length+n {
		return nil, fmt.Errorf("deserialization error []T.Deserialize (invalid data)")
	}
	values := make([]T, length)
	if _, err := binary.Decode(data[n:], binary.LittleEndian, values); err != nil {
		return nil, err
	}
	return values, nil
}

type IntArraySerde struct {
}

func (s *IntArraySerde) IsStubSerde() bool {
	return false
}

func (s *IntArraySerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.([]int)
	if !ok {
		return nil, fmt.Errorf("value is not []int")
	}
	return s.Serialize(v, b)
}

func (s *IntArraySerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *IntArraySerde) Serialize(value []int, b []byte) ([]byte, error) {
	length := len(value)
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + length*uintSize + maxSizeLength)
	data := buf.Bytes()[:bufLen+maxSizeLength+length*uintSize]
	bufLen += setSize(data[bufLen:bufLen+maxSizeLength], length)
	if uintSize == 4 {
		for _, v := range value {
			binary.LittleEndian.PutUint32(data[bufLen:bufLen+uintSize], uint32(v))
			bufLen += uintSize
		}
	} else {
		for _, v := range value {
			binary.LittleEndian.PutUint64(data[bufLen:bufLen+uintSize], uint64(v))
			bufLen += uintSize
		}
	}
	return data[:bufLen], nil
}

func (s *IntArraySerde) Deserialize(data []byte) ([]int, error) {
	length, n := getSize(data)
	if n == 0 {
		return nil, fmt.Errorf("IntArraySerde deserialization error (invalid length data)")
	}
	if len(data) < uintSize*length+n {
		return nil, fmt.Errorf("IntArraySerde deserialization error (invalid data)")
	}
	values := make([]int, length)
	if uintSize == 4 {
		for i := 0; i < length; i++ {
			values[i] = int(binary.LittleEndian.Uint32(data[n:]))
			n += 4
		}
	} else {
		for i := 0; i < length; i++ {
			values[i] = int(binary.LittleEndian.Uint64(data[n:]))
			n += 8
		}
	}
	return values, nil
}

type UIntArraySerde struct {
}

func (s *UIntArraySerde) IsStubSerde() bool {
	return false
}

func (s *UIntArraySerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.([]uint)
	if !ok {
		return nil, fmt.Errorf("value is not []int")
	}
	return s.Serialize(v, b)
}

func (s *UIntArraySerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UIntArraySerde) Serialize(value []uint, b []byte) ([]byte, error) {
	length := len(value)
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + length*uintSize + maxSizeLength)
	data := buf.Bytes()[:bufLen+maxSizeLength+length*uintSize]
	bufLen += setSize(data[bufLen:bufLen+maxSizeLength], length)
	if uintSize == 4 {
		for _, v := range value {
			binary.LittleEndian.PutUint32(data[bufLen:bufLen+uintSize], uint32(v))
			bufLen += uintSize
		}
	} else {
		for _, v := range value {
			binary.LittleEndian.PutUint64(data[bufLen:bufLen+uintSize], uint64(v))
			bufLen += uintSize
		}
	}
	return data[:bufLen], nil
}

func (s *UIntArraySerde) Deserialize(data []byte) ([]uint, error) {
	length, n := getSize(data)
	if n == 0 {
		return nil, fmt.Errorf("IntArraySerde deserialization error (invalid length data)")
	}
	if len(data) < uintSize*length+n {
		return nil, fmt.Errorf("IntArraySerde deserialization error (invalid data)")
	}
	values := make([]uint, length)
	if uintSize == 4 {
		for i := 0; i < length; i++ {
			values[i] = uint(binary.LittleEndian.Uint32(data[n:]))
			n += 4
		}
	} else {
		for i := 0; i < length; i++ {
			values[i] = uint(binary.LittleEndian.Uint64(data[n:]))
			n += 8
		}
	}
	return values, nil
}

type StringSerde struct {
}

func (s *StringSerde) IsStubSerde() bool {
	return false
}

func (s *StringSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("value is not string")
	}
	return s.Serialize(v, b)
}

func (s *StringSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *StringSerde) Serialize(value string, b []byte) ([]byte, error) {
	length := len(value)
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + maxSizeLength + length)
	data := buf.Bytes()[:bufLen+maxSizeLength+length]
	bufLen += setSize(data[bufLen:bufLen+maxSizeLength], length)
	copy(data[bufLen:bufLen+length], value)
	return data[:bufLen+length], nil
}

func (s *StringSerde) Deserialize(data []byte) (string, error) {
	length, n := getSize(data)
	if n == 0 {
		return "", fmt.Errorf("StringSerde deserialization error (invalid length data)")
	}
	if len(data) < n+length {
		return "", fmt.Errorf("deserialization error StringSerde.Deserialize")
	}
	return string(data[n : n+length]), nil
}

type UIntSerde struct {
}

func (s *UIntSerde) IsStubSerde() bool {
	return false
}

func (s *UIntSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(uint)
	if !ok {
		return nil, fmt.Errorf("value is not uint")
	}
	return s.Serialize(v, b)
}

func (s *UIntSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UIntSerde) Serialize(value uint, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + uintSize)
	data := buf.Bytes()[:bufLen+uintSize]
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data[bufLen:bufLen+uintSize], uint32(value))
	} else {
		binary.LittleEndian.PutUint64(data[bufLen:bufLen+uintSize], uint64(value))
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

func (s *UInt8Serde) IsStubSerde() bool {
	return false
}

func (s *UInt8Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(uint8)
	if !ok {
		return nil, fmt.Errorf("value is not uint8")
	}
	return s.Serialize(v, b)
}

func (s *UInt8Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UInt8Serde) Serialize(value uint8, b []byte) ([]byte, error) {
	return append(b, value), nil
}

func (s *UInt8Serde) Deserialize(data []byte) (uint8, error) {
	if len(data) < 1 {
		return 0, fmt.Errorf("serialization error UInt8Serde.Deserialize")
	}
	return data[0], nil
}

type UInt16Serde struct {
}

func (s *UInt16Serde) IsStubSerde() bool {
	return false
}

func (s *UInt16Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(uint16)
	if !ok {
		return nil, fmt.Errorf("value is not uint16")
	}
	return s.Serialize(v, b)
}

func (s *UInt16Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UInt16Serde) Serialize(value uint16, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 2)
	data := buf.Bytes()[:bufLen+2]
	binary.LittleEndian.PutUint16(data[bufLen:bufLen+2], value)
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

func (s *UInt32Serde) IsStubSerde() bool {
	return false
}

func (s *UInt32Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(uint32)
	if !ok {
		return nil, fmt.Errorf("value is not uint32")
	}
	return s.Serialize(v, b)
}

func (s *UInt32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UInt32Serde) Serialize(value uint32, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 4)
	data := buf.Bytes()[:bufLen+4]
	binary.LittleEndian.PutUint32(data[bufLen:bufLen+4], value)
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

func (s *UInt64Serde) IsStubSerde() bool {
	return false
}

func (s *UInt64Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(uint64)
	if !ok {
		return nil, fmt.Errorf("value is not uint64")
	}
	return s.Serialize(v, b)
}

func (s *UInt64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *UInt64Serde) Serialize(value uint64, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 8)
	data := buf.Bytes()[:bufLen+8]
	binary.LittleEndian.PutUint64(data[bufLen:bufLen+8], value)
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

func (s *IntSerde) IsStubSerde() bool {
	return false
}

func (s *IntSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(int)
	if !ok {
		return nil, fmt.Errorf("value is not int")
	}
	return s.Serialize(v, b)
}

func (s *IntSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *IntSerde) Serialize(value int, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + uintSize)
	data := buf.Bytes()[:bufLen+uintSize]
	if uintSize == 4 {
		binary.LittleEndian.PutUint32(data[bufLen:bufLen+uintSize], uint32(value))
	} else {
		binary.LittleEndian.PutUint64(data[bufLen:bufLen+uintSize], uint64(value))
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

func (s *Int8Serde) IsStubSerde() bool {
	return false
}

func (s *Int8Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(int8)
	if !ok {
		return nil, fmt.Errorf("value is not int8")
	}
	return s.Serialize(v, b)
}

func (s *Int8Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Int8Serde) Serialize(value int8, b []byte) ([]byte, error) {
	return append(b, byte(value)), nil
}

func (s *Int8Serde) Deserialize(data []byte) (int8, error) {
	if len(data) < 1 {
		return 0, fmt.Errorf("deserialization error UInt8Serde.Deserialize")
	}
	return int8(data[0]), nil
}

type Int16Serde struct {
}

func (s *Int16Serde) IsStubSerde() bool {
	return false
}

func (s *Int16Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(int16)
	if !ok {
		return nil, fmt.Errorf("value is not uint16")
	}
	return s.Serialize(v, b)
}

func (s *Int16Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Int16Serde) Serialize(value int16, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 2)
	data := buf.Bytes()[:bufLen+2]
	binary.LittleEndian.PutUint16(data[bufLen:bufLen+2], uint16(value))
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

func (s *Int32Serde) IsStubSerde() bool {
	return false
}

func (s *Int32Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(int32)
	if !ok {
		return nil, fmt.Errorf("value is not int32")
	}
	return s.Serialize(v, b)
}

func (s *Int32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Int32Serde) Serialize(value int32, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 4)
	data := buf.Bytes()[:bufLen+4]
	binary.LittleEndian.PutUint32(data[bufLen:bufLen+4], uint32(value))
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

func (s *Int64Serde) IsStubSerde() bool {
	return false
}

func (s *Int64Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("value is not int64")
	}
	return s.Serialize(v, b)
}

func (s *Int64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Int64Serde) Serialize(value int64, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 8)
	data := buf.Bytes()[:bufLen+8]
	binary.LittleEndian.PutUint64(data[bufLen:bufLen+8], uint64(value))
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

func (s *BoolSerde) IsStubSerde() bool {
	return false
}

func (s *BoolSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("value is not bool")
	}
	return s.Serialize(v, b)
}

func (s *BoolSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *BoolSerde) Serialize(value bool, b []byte) ([]byte, error) {
	var v byte
	if value {
		v = 1
	} else {
		v = 0
	}
	return append(b, v), nil
}

func (s *BoolSerde) Deserialize(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, fmt.Errorf("serialization error BoolSerde.Deserialize")
	}
	return data[0] != 0, nil
}

type RuneSerde struct {
}

func (s *RuneSerde) IsStubSerde() bool {
	return false
}

func (s *RuneSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(rune)
	if !ok {
		return nil, fmt.Errorf("value is not rune")
	}
	return s.Serialize(v, b)
}

func (s *RuneSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *RuneSerde) Serialize(value rune, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 4)
	data := buf.Bytes()[:bufLen+4]
	binary.LittleEndian.PutUint32(data[bufLen:bufLen+4], uint32(value))
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

func (s *Float32Serde) IsStubSerde() bool {
	return false
}

func (s *Float32Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(float32)
	if !ok {
		return nil, fmt.Errorf("value is not float32")
	}
	return s.Serialize(v, b)
}

func (s *Float32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Float32Serde) Serialize(value float32, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 4)
	data := buf.Bytes()[:bufLen+4]
	binary.LittleEndian.PutUint32(data[bufLen:bufLen+4], math.Float32bits(value))
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

func (s *Float64Serde) IsStubSerde() bool {
	return false
}

func (s *Float64Serde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("value is not float64")
	}
	return s.Serialize(v, b)
}

func (s *Float64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *Float64Serde) Serialize(value float64, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	bufLen := buf.Len()
	buf.Grow(bufLen + 8)
	data := buf.Bytes()[:bufLen+8]
	binary.LittleEndian.PutUint64(data[bufLen:bufLen+8], math.Float64bits(value))
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
			var ser Serde[int] = &IntSerde{}
			return ser, nil
		}
	case GetSerdeType[int8]():
		{
			var ser Serde[int8] = &Int8Serde{}
			return ser, nil
		}
	case GetSerdeType[int16]():
		{
			var ser Serde[int16] = &Int16Serde{}
			return ser, nil
		}
	case GetSerdeType[int32]():
		{
			var ser Serde[int32] = &Int32Serde{}
			return ser, nil
		}
	case GetSerdeType[int64]():
		{
			var ser Serde[int64] = &Int64Serde{}
			return ser, nil
		}
	case GetSerdeType[uint]():
		{
			var ser Serde[uint] = &UIntSerde{}
			return ser, nil
		}
	case GetSerdeType[uint8]():
		{
			var ser Serde[uint8] = &UInt8Serde{}
			return ser, nil
		}
	case GetSerdeType[uint16]():
		{
			var ser Serde[uint16] = &UInt16Serde{}
			return ser, nil
		}
	case GetSerdeType[uint32]():
		{
			var ser Serde[uint32] = &UInt32Serde{}
			return ser, nil
		}
	case GetSerdeType[uint64]():
		{
			var ser Serde[uint64] = &UInt64Serde{}
			return ser, nil
		}
	case GetSerdeType[[]byte]():
		{
			var ser Serde[[]byte] = &BytesSerde{}
			return ser, nil
		}
	case GetSerdeType[[]int]():
		{
			var ser Serde[[]int] = &IntArraySerde{}
			return ser, nil
		}
	case GetSerdeType[[]bool]():
		{
			var ser Serde[[]bool] = &FixedSizeTypeArraySerde[bool]{}
			return ser, nil
		}
	case GetSerdeType[[]int32]():
		{
			var ser Serde[[]int32] = &FixedSizeTypeArraySerde[int32]{}
			return ser, nil
		}
	case GetSerdeType[[]int64]():
		{
			var ser Serde[[]int64] = &FixedSizeTypeArraySerde[int64]{}
			return ser, nil
		}
	case GetSerdeType[[]int16]():
		{
			var ser Serde[[]int16] = &FixedSizeTypeArraySerde[int16]{}
			return ser, nil
		}
	case GetSerdeType[[]uint]():
		{
			var ser Serde[[]uint] = &UIntArraySerde{}
			return ser, nil
		}
	case GetSerdeType[[]uint32]():
		{
			var ser Serde[[]uint32] = &FixedSizeTypeArraySerde[uint32]{}
			return ser, nil
		}
	case GetSerdeType[[]uint64]():
		{
			var ser Serde[[]uint64] = &FixedSizeTypeArraySerde[uint64]{}
			return ser, nil
		}
	case GetSerdeType[[]uint16]():
		{
			var ser Serde[[]uint16] = &FixedSizeTypeArraySerde[uint16]{}
			return ser, nil
		}
	case GetSerdeType[[]float32]():
		{
			var ser Serde[[]float32] = &FixedSizeTypeArraySerde[float32]{}
			return ser, nil
		}
	case GetSerdeType[[]float64]():
		{
			var ser Serde[[]float64] = &FixedSizeTypeArraySerde[float64]{}
			return ser, nil
		}
	case GetSerdeType[string]():
		{
			var ser Serde[string] = &StringSerde{}
			return ser, nil
		}
	case GetSerdeType[bool]():
		{
			var ser Serde[bool] = &BoolSerde{}
			return ser, nil
		}
	case GetSerdeType[rune]():
		{
			var ser Serde[rune] = &RuneSerde{}
			return ser, nil
		}
	case GetSerdeType[float32]():
		{
			var ser Serde[float32] = &Float32Serde{}
			return ser, nil
		}
	case GetSerdeType[float64]():
		{
			var ser Serde[float64] = &Float64Serde{}
			return ser, nil
		}
	}
	return nil, fmt.Errorf("makeDefaultSerde unsupported type: %v", valueType)
}

type StubSerde[T any] struct {
}

func (s *StubSerde[T]) IsStubSerde() bool {
	return true
}

func (s *StubSerde[T]) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("value is not %s", GetSerdeType[T]().Name())
	}
	return s.Serialize(v, b)
}

func (s *StubSerde[T]) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *StubSerde[T]) Serialize(T, []byte) ([]byte, error) {

	log.Fatalf("serde for type '%s' is not implemented", GetSerdeTypeName[T]())
	return []byte{}, nil
}

func (s *StubSerde[T]) Deserialize([]byte) (T, error) {
	log.Fatalf("serde for type '%s' is not implemented", GetSerdeTypeName[T]())
	var t T
	return t, nil
}

type arraySerde struct {
	arrayType  reflect.Type
	valueSerde Serializer
}

func (s *arraySerde) IsStubSerde() bool {
	return false
}

func (s *arraySerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	valueType := reflect.TypeOf(value)
	if !valueType.AssignableTo(s.arrayType) {
		return nil, fmt.Errorf("value is not %s", s.arrayType.Name())
	}
	v := reflect.ValueOf(value)
	buf := bytes.NewBuffer(b)
	length := buf.Len()
	buf.Grow(length + maxSizeLength)
	data := buf.Bytes()[:length+maxSizeLength]
	n := setSize(data[length:length+maxSizeLength], v.Len())
	data = data[:length+n]
	for i := 0; i < v.Len(); i++ {
		element := v.Index(i)
		buf = bytes.NewBuffer(data)
		length = buf.Len()
		buf.Grow(length + maxSizeLength)
		elementBytes, err := s.valueSerde.SerializeObj(element.Interface(), buf.Bytes()[:length+maxSizeLength])
		if err != nil {
			return nil, err
		}
		elementBytesLength := len(elementBytes) - length - maxSizeLength
		n = setSize(elementBytes[length:length+maxSizeLength], elementBytesLength)
		if n != maxSizeLength {
			copy(elementBytes[length+n:length+n+elementBytesLength],
				elementBytes[length+maxSizeLength:length+maxSizeLength+elementBytesLength])
		}
		data = elementBytes[:length+n+elementBytesLength]
	}
	return data, nil
}

func (s *arraySerde) DeserializeObj(data []byte) (interface{}, error) {
	v := reflect.MakeSlice(s.arrayType, 0, 0)

	var count int
	var n int
	if count, n = getSize(data); n == 0 {
		return nil, fmt.Errorf("DeserializeObj arraySerde error (invalid count data)")
	}
	data = data[n:]
	for i := 0; i < count; i++ {
		var length int
		if length, n = getSize(data); n == 0 {
			return nil, fmt.Errorf("DeserializeObj arraySerde error (invalid element length data)")
		}
		data = data[n:]
		if len(data) < length {
			return v, fmt.Errorf("DeserializeObj arraySerde error (invalid element data)")
		}
		var element interface{}
		var err error
		element, err = s.valueSerde.DeserializeObj(data[:length])
		if err != nil {
			return v, err
		}
		data = data[length:]
		v = reflect.Append(v, reflect.ValueOf(element))
	}
	return v.Interface(), nil
}

type ArraySerde[T any] struct {
	arraySerde
}

func (s *ArraySerde[T]) Serialize(value T, b []byte) ([]byte, error) {
	return s.SerializeObj(reflect.ValueOf(value).Interface(), b)
}

func (s *ArraySerde[T]) Deserialize(data []byte) (T, error) {
	var t T
	v, err := s.DeserializeObj(data)
	if err != nil {
		return t, err
	}
	return v.(T), nil
}

type mapSerde struct {
	mapType         reflect.Type
	keyArraySerde   Serializer
	valueArraySerde Serializer
}

func (s *mapSerde) IsStubSerde() bool {
	return false
}

func (s *mapSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v := reflect.ValueOf(value)

	keys := v.MapKeys()
	keyArray := reflect.MakeSlice(reflect.SliceOf(v.Type().Key()), v.Len(), v.Len())
	valueArray := reflect.MakeSlice(reflect.SliceOf(v.Type().Elem()), v.Len(), v.Len())

	for i, key := range keys {
		keyArray.Index(i).Set(key)
		valueArray.Index(i).Set(v.MapIndex(key))
	}

	buf := bytes.NewBuffer(b)
	length := buf.Len()
	buf.Grow(length + maxSizeLength)
	keyBytes, err := s.keyArraySerde.SerializeObj(keyArray.Interface(), buf.Bytes()[:length+maxSizeLength])
	if err != nil {
		return nil, err
	}
	keyBytesLength := len(keyBytes) - length - maxSizeLength
	n := setSize(keyBytes[length:length+maxSizeLength], keyBytesLength)
	if n != maxSizeLength {
		copy(keyBytes[length+n:length+n+keyBytesLength],
			keyBytes[length+maxSizeLength:length+maxSizeLength+keyBytesLength])
	}
	buf = bytes.NewBuffer(keyBytes[:length+n+keyBytesLength])
	length = buf.Len()
	buf.Grow(length + maxSizeLength)
	valueBytes, err := s.valueArraySerde.SerializeObj(valueArray.Interface(),
		buf.Bytes()[:length+maxSizeLength])
	if err != nil {
		return nil, err
	}
	valueBytesLength := len(valueBytes) - length - maxSizeLength
	n = setSize(valueBytes[length:length+maxSizeLength], valueBytesLength)
	if n != maxSizeLength {
		copy(valueBytes[length+n:length+n+valueBytesLength],
			valueBytes[length+maxSizeLength:length+maxSizeLength+valueBytesLength])
	}
	return valueBytes[:length+n+valueBytesLength], nil
}

func (s *mapSerde) DeserializeObj(data []byte) (interface{}, error) {
	v := reflect.MakeMap(s.mapType)

	var keysLength int
	var n int
	if keysLength, n = getSize(data); n == 0 {
		return nil, fmt.Errorf("mapSerde DeserializeObj error (invalid keys length data)")
	}
	data = data[n:]
	keys, err := s.keyArraySerde.DeserializeObj(data[:keysLength])
	if err != nil {
		return nil, err
	}
	data = data[keysLength:]

	var valuesLength int
	if valuesLength, n = getSize(data); n == 0 {
		return nil, fmt.Errorf("mapSerde DeserializeObj error (invalid values length data)")
	}
	data = data[n:]
	values, err := s.valueArraySerde.DeserializeObj(data[:valuesLength])
	if err != nil {
		return nil, err
	}

	keySlice := reflect.ValueOf(keys)
	valueSlice := reflect.ValueOf(values)
	for i := 0; i < keySlice.Len(); i++ {
		key := keySlice.Index(i)
		value := valueSlice.Index(i)
		v.SetMapIndex(key, value)
	}
	return v.Interface(), nil
}

type MapSerde[T any] struct {
	mapSerde
	keySerde   Serializer
	valueSerde Serializer
}

func (s *MapSerde[T]) Serialize(value T, b []byte) ([]byte, error) {
	return s.SerializeObj(reflect.ValueOf(value).Interface(), b)
}

func (s *MapSerde[T]) Deserialize(data []byte) (T, error) {
	var t T
	v, err := s.DeserializeObj(data)
	if err != nil {
		return t, err
	}
	return v.(T), nil
}
