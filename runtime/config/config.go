/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
    "github.com/gorundebug/servicelib/api"
)

func ValuePtr[T any](v T) *T {
    return &v
}

type Config interface {
    GetServices() []*ServiceConfig
    GetStreams() []*StreamConfig
    GetDataConnectors() []*DataConnectorConfig
    GetEndpoints() []*EndpointConfig
    GetPools() []*PoolConfig
    GetLinks() []*LinkConfig
    GetProperty(name string) interface{}
}

type StreamConfig struct {
    api.Stream `mapstructure:",squash" yaml:",inline"`
    Properties map[string]interface{} `mapstructure:",remain" yaml:",inline"`
}

func (s *StreamConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

type PoolConfig struct {
    api.Pool   `mapstructure:",squash" yaml:",inline"`
    Properties map[string]interface{} `mapstructure:",remain"`
}

func (s *PoolConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

var transformationNameMap = map[api.TransformationType]string{
    api.AppSink:         "appSink",
    api.CycleLink:       "cycleLink",
    api.Sink:            "sink",
    api.Filter:          "filter",
    api.FlatMap:         "flatMap",
    api.FlatMapIterable: "flatMapIterable",
    api.ForEach:         "forEach",
    api.Input:           "input",
    api.Join:            "join",
    api.KeyBy:           "keyBy",
    api.Map:             "map",
    api.Merge:           "merge",
    api.MultiJoin:       "multiJoin",
    api.Parallels:       "parallels",
    api.Split:           "split",
    api.Delay:           "delay",
    api.AppInput:        "appInput",
}

func (s *StreamConfig) GetTransformationName() string {
    return transformationNameMap[s.Type]
}

type ServiceConfig struct {
    api.Service `mapstructure:",squash" yaml:",inline"`
    Properties  map[string]interface{} `mapstructure:",remain" yaml:",inline"`
}

func (s *ServiceConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

type LinkConfig struct {
    api.Link   `mapstructure:",squash" yaml:",inline"`
    Properties map[string]interface{} `mapstructure:",remain" yaml:",inline"`
}

func (s *LinkConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

type DataConnectorConfig struct {
    api.DataConnector `mapstructure:",squash" yaml:",inline"`
    Properties        map[string]interface{} `mapstructure:",remain" yaml:",inline"`
}

func (s *DataConnectorConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

type EndpointConfig struct {
    api.Endpoint `mapstructure:",squash" yaml:",inline"`
    Properties   map[string]interface{} `mapstructure:",remain" yaml:",inline"`
}

func (s *EndpointConfig) GetProperty(name string) interface{} {
    return s.Properties[name]
}

type LinkId struct {
    From int
    To   int
}

type RuntimeConfig struct {
    streamsByName        map[string]*StreamConfig
    servicesByName       map[string]*ServiceConfig
    linksById            map[LinkId]*LinkConfig
    dataConnectorsByName map[string]*DataConnectorConfig
    endpointsByName      map[string]*EndpointConfig
    streamsById          map[int]*StreamConfig
    servicesById         map[int]*ServiceConfig
    dataConnectorsById   map[int]*DataConnectorConfig
    endpointsById        map[int]*EndpointConfig
    poolByName           map[string]*PoolConfig
    config               Config
}

func NewRuntimeConfig(config Config) *RuntimeConfig {
    runtimeCfg := &RuntimeConfig{
        config:               config,
        streamsByName:        make(map[string]*StreamConfig),
        streamsById:          make(map[int]*StreamConfig),
        servicesByName:       make(map[string]*ServiceConfig),
        servicesById:         make(map[int]*ServiceConfig),
        endpointsById:        make(map[int]*EndpointConfig),
        dataConnectorsById:   make(map[int]*DataConnectorConfig),
        endpointsByName:      make(map[string]*EndpointConfig),
        dataConnectorsByName: make(map[string]*DataConnectorConfig),
        linksById:            make(map[LinkId]*LinkConfig),
        poolByName:           make(map[string]*PoolConfig),
    }

    for _, v := range config.GetStreams() {
        runtimeCfg.streamsByName[v.Name] = v
        runtimeCfg.streamsById[v.Id] = v
    }
    for _, v := range config.GetServices() {
        runtimeCfg.servicesByName[v.Name] = v
        runtimeCfg.servicesById[v.Id] = v
    }
    for _, v := range config.GetEndpoints() {
        runtimeCfg.endpointsByName[v.Name] = v
        runtimeCfg.endpointsById[v.Id] = v
    }
    for _, v := range config.GetDataConnectors() {
        runtimeCfg.dataConnectorsById[v.Id] = v
        runtimeCfg.dataConnectorsByName[v.Name] = v
    }
    for _, v := range config.GetPools() {
        runtimeCfg.poolByName[v.Name] = v
    }
    for _, v := range config.GetLinks() {
        runtimeCfg.linksById[LinkId{From: v.From, To: v.To}] = v
    }
    return runtimeCfg
}

func (cfg *RuntimeConfig) GetConfig() Config {
    return cfg.config
}

func (cfg *RuntimeConfig) GetStreamConfigByName(name string) *StreamConfig {
    return cfg.streamsByName[name]
}

func (cfg *RuntimeConfig) GetDataConnectorById(id int) *DataConnectorConfig {
    return cfg.dataConnectorsById[id]
}

func (cfg *RuntimeConfig) GetEndpointConfigById(id int) *EndpointConfig {
    return cfg.endpointsById[id]
}

func (cfg *RuntimeConfig) GetServiceConfigByName(name string) *ServiceConfig {
    return cfg.servicesByName[name]
}

func (cfg *RuntimeConfig) GetServiceConfigById(id int) *ServiceConfig {
    return cfg.servicesById[id]
}

func (cfg *RuntimeConfig) GetStreamConfigById(id int) *StreamConfig {
    return cfg.streamsById[id]
}

func (cfg *RuntimeConfig) GetPoolByName(name string) *PoolConfig {
    return cfg.poolByName[name]
}

func (cfg *RuntimeConfig) GetLink(from int, to int) *LinkConfig {
    return cfg.linksById[LinkId{From: from, To: to}]
}
