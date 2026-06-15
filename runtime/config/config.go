/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
	"fmt"

	"github.com/gorundebug/servicelib/api"
)

type Config interface {
	GetServices() []*ServiceConfig
	GetStreams() []StreamConfig
	GetDataConnectors() []DataConnectorConfig
	GetEndpoints() []EndpointConfig
	GetPools() []*PoolConfig
	GetLinks() []*LinkConfig
	GetModules() []*ModuleConfig
	GetTypes() []*TypeConfig
	GetProperty(name string) interface{}
	ApplyEnvironment() error
}

type PoolConfig struct {
	ExecutorsCount int                    `yaml:"executorsCount" mapstructure:"executorsCount"`
	QueueCapacity  int                    `yaml:"queueCapacity" mapstructure:"queueCapacity"`
	Name           string                 `yaml:"name" mapstructure:"name"`
	Properties     map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *PoolConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type ModuleConfig struct {
	Name       string                 `yaml:"name" mapstructure:"name"`
	Path       string                 `yaml:"path" mapstructure:"path"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *ModuleConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

var transformationNameMap = map[api.TransformationType]string{
	api.TransformationTypeCycleLink:       "cycleLink",
	api.TransformationTypeSink:            "sink",
	api.TransformationTypeFilter:          "filter",
	api.TransformationTypeFlatMap:         "flatMap",
	api.TransformationTypeFlatMapIterable: "flatMapIterable",
	api.TransformationTypeProcess:         "process",
	api.TransformationTypeInput:           "input",
	api.TransformationTypeJoin:            "join",
	api.TransformationTypeKeyBy:           "keyBy",
	api.TransformationTypeMap:             "map",
	api.TransformationTypeMerge:           "merge",
	api.TransformationTypeMultiJoin:       "multiJoin",
	api.TransformationTypeSplit:           "split",
	api.TransformationTypeDelay:           "delay",
	api.TransformationTypeError:           "error",
	api.TransformationTypeCase:            "case",
	api.TransformationTypeWhen:            "when",
}

func GetTransformationName(t api.TransformationType) string {
	return transformationNameMap[t]
}

type ServiceConfig struct {
	Color                string                 `yaml:"color" mapstructure:"color"`
	DefaultCallSemantics *CallSemanticsGroup    `yaml:"defaultCallSemantics,omitempty" mapstructure:"defaultCallSemantics"`
	DefaultGrpcTimeout   int                    `yaml:"defaultGrpcTimeout,omitempty" mapstructure:"defaultGrpcTimeout"`
	Environment          api.Environment        `yaml:"environment" mapstructure:"environment"`
	GolangVersion        string                 `yaml:"golangVersion,omitempty" mapstructure:"golangVersion"`
	GrpcHost             string                 `yaml:"grpcHost" mapstructure:"grpcHost"`
	GrpcPort             int                    `yaml:"grpcPort" mapstructure:"grpcPort"`
	HttpHost             string                 `yaml:"httpHost" mapstructure:"httpHost"`
	HttpPort             int                    `yaml:"httpPort" mapstructure:"httpPort"`
	ID                   int                    `yaml:"id" mapstructure:"id"`
	LogLevel             api.LogLevel           `yaml:"logLevel,omitempty" mapstructure:"logLevel"`
	MetricsHandler       string                 `yaml:"metricsHandler" mapstructure:"metricsHandler"`
	ModulePath           string                 `yaml:"modulePath,omitempty" mapstructure:"modulePath"`
	Name                 string                 `yaml:"name" mapstructure:"name"`
	ShutdownTimeout      int                    `yaml:"shutdownTimeout" mapstructure:"shutdownTimeout"`
	StatusHandler        string                 `yaml:"statusHandler" mapstructure:"statusHandler"`
	Properties           map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *ServiceConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type LinkID struct {
	From int
	To   int
}

type RuntimeConfig struct {
	streamsByName        map[string]StreamConfig
	servicesByName       map[string]*ServiceConfig
	linksByID            map[LinkID]*LinkConfig
	dataConnectorsByName map[string]DataConnectorConfig
	endpointsByName      map[string]EndpointConfig
	streamsByID          map[int]StreamConfig
	servicesByID         map[int]*ServiceConfig
	dataConnectorsByID   map[int]DataConnectorConfig
	endpointsByID        map[int]EndpointConfig
	poolByName           map[string]*PoolConfig
	typesByName          map[string]*TypeConfig
	config               Config
}

func NewRuntimeConfig(config Config) (*RuntimeConfig, error) {
	runtimeCfg := &RuntimeConfig{
		config:               config,
		streamsByName:        make(map[string]StreamConfig),
		streamsByID:          make(map[int]StreamConfig),
		servicesByName:       make(map[string]*ServiceConfig),
		servicesByID:         make(map[int]*ServiceConfig),
		endpointsByID:        make(map[int]EndpointConfig),
		dataConnectorsByID:   make(map[int]DataConnectorConfig),
		endpointsByName:      make(map[string]EndpointConfig),
		dataConnectorsByName: make(map[string]DataConnectorConfig),
		linksByID:            make(map[LinkID]*LinkConfig),
		poolByName:           make(map[string]*PoolConfig),
		typesByName:          make(map[string]*TypeConfig),
	}

	for _, v := range config.GetStreams() {
		runtimeCfg.streamsByName[v.GetName()] = v
		runtimeCfg.streamsByID[v.GetID()] = v
	}
	for _, v := range config.GetServices() {
		runtimeCfg.servicesByName[v.Name] = v
		runtimeCfg.servicesByID[v.ID] = v
	}
	for _, v := range config.GetEndpoints() {
		runtimeCfg.endpointsByName[v.GetName()] = v
		runtimeCfg.endpointsByID[v.GetID()] = v
	}
	for _, v := range config.GetDataConnectors() {
		runtimeCfg.dataConnectorsByID[v.GetID()] = v
		runtimeCfg.dataConnectorsByName[v.GetName()] = v
	}
	for _, v := range config.GetPools() {
		runtimeCfg.poolByName[v.Name] = v
	}
	for _, v := range config.GetTypes() {
		runtimeCfg.typesByName[v.Name] = v
	}
	for _, v := range config.GetLinks() {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("validate link error: %w", err)
		}
		runtimeCfg.linksByID[LinkID{From: v.From, To: v.To}] = v
	}
	return runtimeCfg, nil
}

func (cfg *RuntimeConfig) GetConfig() Config {
	return cfg.config
}

func (cfg *RuntimeConfig) GetStreamConfigByName(name string) StreamConfig {
	return cfg.streamsByName[name]
}

func (cfg *RuntimeConfig) GetDataConnectorByID(id int) DataConnectorConfig {
	return cfg.dataConnectorsByID[id]
}

func (cfg *RuntimeConfig) GetEndpointConfigByID(id int) EndpointConfig {
	return cfg.endpointsByID[id]
}

func (cfg *RuntimeConfig) GetServiceConfigByName(name string) *ServiceConfig {
	return cfg.servicesByName[name]
}

func (cfg *RuntimeConfig) GetServiceConfigByID(id int) *ServiceConfig {
	return cfg.servicesByID[id]
}

func (cfg *RuntimeConfig) GetStreamConfigByID(id int) StreamConfig {
	return cfg.streamsByID[id]
}

func (cfg *RuntimeConfig) GetPoolByName(name string) *PoolConfig {
	return cfg.poolByName[name]
}

func (cfg *RuntimeConfig) GetTypeByName(name string) *TypeConfig {
	return cfg.typesByName[name]
}

func (cfg *RuntimeConfig) GetLink(from int, to int) *LinkConfig {
	return cfg.linksByID[LinkID{From: from, To: to}]
}
