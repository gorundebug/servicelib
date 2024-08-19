/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type MockServiceConfig struct {
	ServiceAppConfig `mapstructure:",squash"`
}

type MockService struct {
	ServiceApp
	serviceConfig *MockServiceConfig
}

func (s *MockService) GetSerde(valueType reflect.Type) (Serializer, error) {
	return nil, nil
}

func (s *MockService) ServiceInit(ctx context.Context, config Config) {
}

func mockService() *MockService {
	config := MockServiceConfig{
		ServiceAppConfig: ServiceAppConfig{
			Services: []ServiceConfig{
				{
					Name:           "MockService",
					MonitoringIp:   "127.0.0.1",
					MonitoringPort: 9000,
				},
			},
		},
	}
	service := MockService{}
	service.streamsInit(context.Background(), "MockService", &service, &config)
	return &service
}

func TestIsKeyValueType(t *testing.T) {
	assert.Equal(t, true, IsKeyValueType[KeyValue[int, int]]())
	assert.Equal(t, false, IsKeyValueType[int]())
}

func TestArraySerde(t *testing.T) {
	arraySerde := makeSerde[[]int](mockService())
	arr := []int{1, 2, 3}
	data, err := arraySerde.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySerde.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestArrayArraySerde(t *testing.T) {
	arraySerde := makeSerde[[][]int](mockService())
	arr := [][]int{{1, 2, 3}, {1, 2, 3}}
	data, err := arraySerde.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySerde.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestMapSerde(t *testing.T) {
	mapSerde := makeSerde[map[int]int](mockService())
	dict := map[int]int{1: 1, 2: 2, 3: 3}
	data, err := mapSerde.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSerde.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}

func TestMapMapSerde(t *testing.T) {
	mapSerde := makeSerde[map[int]map[int]int](mockService())
	dict := map[int]map[int]int{1: {1: 1, 2: 2}, 2: {3: 3, 4: 4}}
	data, err := mapSerde.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSerde.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}
