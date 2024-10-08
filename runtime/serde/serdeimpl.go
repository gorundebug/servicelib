/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package serde

import (
	"encoding/binary"
	"fmt"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"math"
	"math/bits"
	"reflect"
)

const uintSize = bits.UintSize / 8

type streamSerde[T any] struct {
	serde Serde[T]
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

func (s *streamKeyValueSerde[K, V]) KeySerializer() Serializer {
	return s.serdeKey
}

func (s *streamKeyValueSerde[K, V]) ValueSerializer() Serializer {
	return s.serdeValue
}

func (s *streamKeyValueSerde[K, V]) SerializeKey(kv datastruct.KeyValue[K, V]) ([]byte, error) {
	return s.serdeKey.Serialize(kv.Key)
}

func (s *streamKeyValueSerde[K, V]) SerializeValue(kv datastruct.KeyValue[K, V]) ([]byte, error) {
	return s.serdeValue.Serialize(kv.Value)
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
	if keyBytes, err := s.serdeKey.Serialize(kv.Key); err == nil {
		keyLenBytes := make([]byte, uintSize)
		if uintSize == 4 {
			binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(keyBytes)))
		} else {
			binary.LittleEndian.PutUint64(keyLenBytes, uint64(len(keyBytes)))
		}
		if valueBytes, err := s.serdeValue.Serialize(kv.Value); err == nil {
			valueLenBytes := make([]byte, uintSize)
			if uintSize == 4 {
				binary.LittleEndian.PutUint32(valueLenBytes, uint32(len(valueBytes)))
			} else {
				binary.LittleEndian.PutUint64(valueLenBytes, uint64(len(valueBytes)))
			}
			result := append(keyLenBytes, keyBytes...)
			result = append(result, valueLenBytes...)
			result = append(result, valueBytes...)
			return result, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (s *streamKeyValueSerde[K, V]) Deserialize(data []byte) (datastruct.KeyValue[K, V], error) {
	var kv datastruct.KeyValue[K, V]
	var keyLen int
	if len(data) < uintSize {
		return kv, fmt.Errorf("deserialize key len error streamKeyValueSerde")
	}
	if uintSize == 4 {
		keyLen = int(binary.LittleEndian.Uint32(data))
	} else {
		keyLen = int(binary.LittleEndian.Uint64(data))
	}
	data = data[uintSize:]
	if len(data) < keyLen {
		return kv, fmt.Errorf("deserialize key error streamKeyValueSerde")
	}
	var err error
	kv.Key, err = s.serdeKey.Deserialize(data[:keyLen])
	if err != nil {
		return kv, err
	}
	data = data[keyLen:]
	var valueLen int
	if len(data) < uintSize {
		return kv, fmt.Errorf("deserialize value len error streamKeyValueSerde")
	}
	if uintSize == 4 {
		valueLen = int(binary.LittleEndian.Uint32(data))
	} else {
		valueLen = int(binary.LittleEndian.Uint64(data))
	}
	data = data[uintSize:]
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

type StringSerde struct {
}

func (s *StringSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("value is not string")
	}
	return s.Serialize(v)
}

func (s *StringSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *UIntSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(uint)
	if !ok {
		return nil, fmt.Errorf("value is not uint")
	}
	return s.Serialize(v)
}

func (s *UIntSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *UInt8Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(uint8)
	if !ok {
		return nil, fmt.Errorf("value is not uint8")
	}
	return s.Serialize(v)
}

func (s *UInt8Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *UInt16Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(uint16)
	if !ok {
		return nil, fmt.Errorf("value is not uint16")
	}
	return s.Serialize(v)
}

func (s *UInt16Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *UInt32Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(uint32)
	if !ok {
		return nil, fmt.Errorf("value is not uint32")
	}
	return s.Serialize(v)
}

func (s *UInt32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *UInt64Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(uint64)
	if !ok {
		return nil, fmt.Errorf("value is not uint64")
	}
	return s.Serialize(v)
}

func (s *UInt64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *IntSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(int)
	if !ok {
		return nil, fmt.Errorf("value is not int")
	}
	return s.Serialize(v)
}

func (s *IntSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Int8Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(int8)
	if !ok {
		return nil, fmt.Errorf("value is not int8")
	}
	return s.Serialize(v)
}

func (s *Int8Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Int16Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(int16)
	if !ok {
		return nil, fmt.Errorf("value is not uint16")
	}
	return s.Serialize(v)
}

func (s *Int16Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Int32Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(int32)
	if !ok {
		return nil, fmt.Errorf("value is not int32")
	}
	return s.Serialize(v)
}

func (s *Int32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Int64Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("value is not int64")
	}
	return s.Serialize(v)
}

func (s *Int64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *BoolSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("value is not bool")
	}
	return s.Serialize(v)
}

func (s *BoolSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *RuneSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(rune)
	if !ok {
		return nil, fmt.Errorf("value is not rune")
	}
	return s.Serialize(v)
}

func (s *RuneSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Float32Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(float32)
	if !ok {
		return nil, fmt.Errorf("value is not float32")
	}
	return s.Serialize(v)
}

func (s *Float32Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

func (s *Float64Serde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("value is not float64")
	}
	return s.Serialize(v)
}

func (s *Float64Serde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
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

type SerdeStub[T any] struct {
}

func (s *SerdeStub[T]) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("value is not %s", GetSerdeType[T]().Name())
	}
	return s.Serialize(v)
}

func (s *SerdeStub[T]) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *SerdeStub[T]) Serialize(T) ([]byte, error) {
	return []byte{}, nil
}

func (s *SerdeStub[T]) Deserialize([]byte) (T, error) {
	var t T
	return t, nil
}

type arraySerde struct {
	arrayType  reflect.Type
	valueSerde Serializer
}

func (s *arraySerde) SerializeObj(value interface{}) ([]byte, error) {
	valueType := reflect.TypeOf(value)
	if !valueType.AssignableTo(s.arrayType) {
		return nil, fmt.Errorf("value is not %s", s.arrayType.Name())
	}
	v := reflect.ValueOf(value)
	var result []byte
	var countBytes []byte
	if uintSize == 4 {
		count := int32(v.Len())
		countBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(countBytes, uint32(count))
	} else {
		count := int64(v.Len())
		countBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, uint64(count))
	}
	result = append(result, countBytes...)
	for i := 0; i < v.Len(); i++ {
		element := v.Index(i)
		elementBytes, err := s.valueSerde.SerializeObj(element.Interface())
		if err != nil {
			return nil, err
		}
		var lengthBytes []byte
		if uintSize == 4 {
			length := int32(len(elementBytes))
			lengthBytes = make([]byte, 4)
			binary.LittleEndian.PutUint32(lengthBytes, uint32(length))
		} else {
			length := int64(len(elementBytes))
			lengthBytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(lengthBytes, uint64(length))
		}
		result = append(result, lengthBytes...)
		result = append(result, elementBytes...)
	}
	return result, nil
}

func (s *arraySerde) DeserializeObj(data []byte) (interface{}, error) {
	v := reflect.MakeSlice(s.arrayType, 0, 0)
	if len(data) < uintSize {
		return v, fmt.Errorf("DeserializeObj arraySerde error (invalid count data)")
	}
	var count int
	if uintSize == 4 {
		count = int(binary.LittleEndian.Uint32(data))
	} else {
		count = int(binary.LittleEndian.Uint64(data))
	}
	data = data[uintSize:]
	for i := 0; i < count; i++ {
		if len(data) < uintSize {
			return v, fmt.Errorf("DeserializeObj arraySerde error (invalid element length data)")
		}
		var length int
		if uintSize == 4 {
			length = int(binary.LittleEndian.Uint32(data))
		} else {
			length = int(binary.LittleEndian.Uint64(data))
		}
		data = data[uintSize:]
		if len(data) < length {
			return v, fmt.Errorf("DeserializeObj arraySerde error (invalid element data)")
		}
		element, err := s.valueSerde.DeserializeObj(data[:length])
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

func (s *ArraySerde[T]) Serialize(value T) ([]byte, error) {
	return s.SerializeObj(reflect.ValueOf(value).Interface())
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
	mapType    reflect.Type
	keySerde   Serializer
	valueSerde Serializer
}

func (s *mapSerde) SerializeObj(value interface{}) ([]byte, error) {
	v := reflect.ValueOf(value)
	var result []byte
	var countBytes []byte
	if uintSize == 4 {
		count := int32(v.Len())
		countBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(countBytes, uint32(count))
	} else {
		count := int64(v.Len())
		countBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, uint64(count))
	}
	result = append(result, countBytes...)
	for _, key := range v.MapKeys() {
		keyBytes, err := s.keySerde.SerializeObj(key.Interface())
		if err != nil {
			return nil, err
		}
		var keyLengthBytes []byte
		if uintSize == 4 {
			keyLength := int32(len(keyBytes))
			keyLengthBytes = make([]byte, 4)
			binary.LittleEndian.PutUint32(keyLengthBytes, uint32(keyLength))
		} else {
			keyLength := int64(len(keyBytes))
			keyLengthBytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(keyLengthBytes, uint64(keyLength))
		}
		result = append(result, keyLengthBytes...)
		result = append(result, keyBytes...)

		valueBytes, err := s.valueSerde.SerializeObj(v.MapIndex(key).Interface())
		if err != nil {
			return nil, err
		}
		var valueLengthBytes []byte
		if uintSize == 4 {
			valueLength := int32(len(valueBytes))
			valueLengthBytes = make([]byte, 4)
			binary.LittleEndian.PutUint32(valueLengthBytes, uint32(valueLength))
		} else {
			valueLength := int64(len(valueBytes))
			valueLengthBytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(valueLengthBytes, uint64(valueLength))
		}
		result = append(result, valueLengthBytes...)
		result = append(result, valueBytes...)
	}
	return result, nil
}

func (s *mapSerde) DeserializeObj(data []byte) (interface{}, error) {
	v := reflect.MakeMap(s.mapType)
	if len(data) < uintSize {
		return v, fmt.Errorf("mapSerde DeserializeObj error (invalid count data)")
	}
	var count int
	if uintSize == 4 {
		count = int(binary.LittleEndian.Uint32(data))
	} else {
		count = int(binary.LittleEndian.Uint64(data))
	}
	data = data[uintSize:]
	for i := 0; i < count; i++ {
		if len(data) < uintSize {
			return v, fmt.Errorf("mapSerde DeserializeObj error (invalid key length data)")
		}

		var keyLength int
		if uintSize == 4 {
			keyLength = int(binary.LittleEndian.Uint32(data))
		} else {
			keyLength = int(binary.LittleEndian.Uint64(data))
		}
		data = data[uintSize:]

		if len(data) < keyLength {
			return v, fmt.Errorf("mapSerde DeserializeObj error (invalid key data)")
		}
		key, err := s.keySerde.DeserializeObj(data[:keyLength])
		if err != nil {
			return v, err
		}
		data = data[keyLength:]

		if len(data) < uintSize {
			return v, fmt.Errorf("mapSerde DeserializeObj error (invalid value length data)")
		}
		var valueLength int
		if uintSize == 4 {
			valueLength = int(binary.LittleEndian.Uint32(data))
		} else {
			valueLength = int(binary.LittleEndian.Uint64(data))
		}
		data = data[uintSize:]

		if len(data) < valueLength {
			return v, fmt.Errorf("mapSerde DeserializeObj error (invalid value data)")
		}
		value, err := s.valueSerde.DeserializeObj(data[:valueLength])
		if err != nil {
			return v, err
		}
		data = data[valueLength:]
		v.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
	}
	return v.Interface(), nil
}

type MapSerde[T any] struct {
	mapSerde
	keySerde   Serializer
	valueSerde Serializer
}

func (s *MapSerde[T]) Serialize(value T) ([]byte, error) {
	return s.SerializeObj(reflect.ValueOf(value).Interface())
}

func (s *MapSerde[T]) Deserialize(data []byte) (T, error) {
	var t T
	v, err := s.DeserializeObj(data)
	if err != nil {
		return t, err
	}
	return v.(T), nil
}
