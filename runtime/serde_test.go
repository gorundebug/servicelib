/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/stretchr/testify/assert"
	"reflect"
	"slices"
	"testing"
)

type MockServiceConfig struct {
	config.ServiceAppConfig `mapstructure:",squash"`
}

type MockService struct {
	ServiceApp
	serviceConfig *MockServiceConfig //nolint:unused
}

type MockServiceLoader struct {
}

func (s *MockServiceLoader) Stop() {
}

func (s *MockService) GetSerde(valueType reflect.Type) (serde.Serializer, error) {
	return nil, nil
}

func (s *MockService) StreamsInit(ctx context.Context) {
}

func (s *MockService) SetConfig(config config.Config) {}

func mockService(environment string) *MockService {

	cfg := MockServiceConfig{
		ServiceAppConfig: config.ServiceAppConfig{
			Services: []config.ServiceConfig{
				{
					Service: api.Service{
						Name:           "MockService",
						MonitoringHost: "127.0.0.1",
						MonitoringPort: 9000,
						Environment:    environment,
						DelayExecutors: 1,
					},
				},
			},
		},
	}
	cfg.InitRuntimeConfig()
	service := MockService{}
	if err := service.serviceInit("MockService", &service, nil, &MockServiceLoader{}, &cfg); err != nil {
		panic(err)
	}
	return &service
}

var testMockService = mockService("TestEnvironment")

func BenchmarkRange(b *testing.B) {
	arraySize := 100000
	a := make([]int, arraySize)
	for i := range a {
		a[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		for _, v := range a {
			sum += v
		}
	}
}

type ImmutableSlice[T any] struct {
	data []T
}

func NewImmutableSlice[T any](data []T) ImmutableSlice[T] {
	return ImmutableSlice[T]{data: data}
}

func (s ImmutableSlice[T]) Len() int {
	return len(s.data)
}

func (s ImmutableSlice[T]) At(i int) T {
	return s.data[i]
}

func BenchmarkRangeWithWrapper(b *testing.B) {
	arraySize := 100000
	a := make([]int, arraySize)
	for i := range a {
		a[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aWrapper := NewImmutableSlice(a)
		sum := 0
		l := aWrapper.Len()
		for j := 0; j < l; j++ {
			sum += aWrapper.At(j)
		}
	}
}

func BenchmarkRangeWithCopy(b *testing.B) {
	arraySize := 100000
	a := make([]int, arraySize)
	for i := range a {
		a[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aCopy := make([]int, len(a))
		copy(aCopy, a)
		sum := 0
		for _, v := range aCopy {
			sum += v
		}
	}
}

func BenchmarkSeq(b *testing.B) {
	arraySize := 100000
	a := make([]int, arraySize)
	for i := range a {
		a[i] = i
	}
	it := slices.Values(a)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		for v := range it {
			sum += v
		}
	}
}

func BenchmarkSeq2(b *testing.B) {
	arraySize := 100000
	a := make([]int, arraySize)
	for i := range a {
		a[i] = i
	}
	it := slices.All(a)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		for _, v := range it {
			sum += v
		}
	}
}

func TestIsKeyValueType(t *testing.T) {

	assert.Equal(t, true, IsKeyValueType[datastruct.KeyValue[int, int]]())
	assert.Equal(t, false, IsKeyValueType[int]())
}

func TestArraySerde(t *testing.T) {
	arraySer := MakeSerde[[]int](testMockService)
	arr := []int{1, 2, 3}
	data, err := arraySer.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestArrayArraySerde(t *testing.T) {
	arraySer := MakeSerde[[][]int32](testMockService)
	arr := [][]int32{{1, 2, 3}, {1, 2, 3}}
	data, err := arraySer.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestIntPtrSerde(t *testing.T) {
	t.Skip()
	mapSer := MakeSerde[*int](testMockService)
	v := 1
	data, err := mapSer.Serialize(&v)
	assert.Equal(t, err, nil, err)
	vCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, v, vCopy)
}

func TestMapSerde(t *testing.T) {
	mapSer := MakeSerde[map[int]int](testMockService)
	dict := map[int]int{1: 1, 2: 2, 3: 3}
	data, err := mapSer.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}

func TestMapMapSerde(t *testing.T) {
	mapSer := MakeSerde[map[int]map[int]int](testMockService)
	dict := map[int]map[int]int{1: {1: 1, 2: 2}, 2: {3: 3, 4: 4}}
	data, err := mapSer.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}
