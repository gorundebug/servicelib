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
	"testing"
)

type MockServiceConfig struct {
	config.ServiceAppConfig `mapstructure:",squash"`
}

type MockService struct {
	ServiceApp
	serviceConfig *MockServiceConfig
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
						MetricsEngine:  api.Prometeus,
						Environment:    environment,
						DelayExecutors: 1,
					},
				},
			},
		},
	}
	service := MockService{}
	service.serviceInit("MockService", &service, &cfg)
	return &service
}

func TestIsKeyValueType(t *testing.T) {
	assert.Equal(t, true, IsKeyValueType[datastruct.KeyValue[int, int]]())
	assert.Equal(t, false, IsKeyValueType[int]())
}

func TestArraySerde(t *testing.T) {
	arraySer := MakeSerde[[]int](mockService("TestArraySerde"))
	arr := []int{1, 2, 3}
	data, err := arraySer.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestArrayArraySerde(t *testing.T) {
	arraySer := MakeSerde[[][]int32](mockService("TestArrayArraySerde"))
	arr := [][]int32{{1, 2, 3}, {1, 2, 3}}
	data, err := arraySer.Serialize(arr)
	assert.Equal(t, err, nil, err)
	arrCopy, err := arraySer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, arr, arrCopy)
}

func TestIntPtrSerde(t *testing.T) {
	// t.Skip()
	mapSer := MakeSerde[*int](mockService("TestMapSerde"))
	v := 1
	data, err := mapSer.Serialize(&v)
	assert.Equal(t, err, nil, err)
	vCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, v, vCopy)
}

func TestMapSerde(t *testing.T) {
	mapSer := MakeSerde[map[int]int](mockService("TestMapSerde"))
	dict := map[int]int{1: 1, 2: 2, 3: 3}
	data, err := mapSer.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}

func TestMapMapSerde(t *testing.T) {
	mapSer := MakeSerde[map[int]map[int]int](mockService("TestMapMapSerde"))
	dict := map[int]map[int]int{1: {1: 1, 2: 2}, 2: {3: 3, 4: 4}}
	data, err := mapSer.Serialize(dict)
	assert.Equal(t, err, nil, err)
	dictCopy, err := mapSer.Deserialize(data)
	assert.Equal(t, err, nil, err)
	assert.Equal(t, dict, dictCopy)
}
