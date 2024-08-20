/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"gitlab.com/gorundebug/servicelib/api"
)

type Config interface {
	GetServiceConfig() *ServiceAppConfig
}

// ConfigSettings /*
// Settings how the service will get access to config and config update options
type ConfigSettings struct {
}

type ConfigProperties interface {
	GetProperty(name string) interface{}
}

type StreamConfig struct {
	Id         int                    `yaml:"id"`
	Name       string                 `yaml:"name"`
	Type       api.TransformationType `yaml:"type"`
	IdService  int                    `yaml:"idService"`
	IdSource   int                    `yaml:"idSource"`
	IdSources  []int                  `yaml:"idSources"`
	XPos       int                    `yaml:"xPos"`
	YPos       int                    `yaml:"yPos"`
	Properties map[string]interface{} `mapstructure:",remain"`
}

func (s *StreamConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

var transformationNameMap = map[api.TransformationType]string{
	api.TransformationTypeAppSink:         "appSink",
	api.TransformationTypeCycleLink:       "cycleLink",
	api.TransformationTypeSink:            "sink",
	api.TransformationTypeFilter:          "filter",
	api.TransformationTypeFlatMap:         "flatMap",
	api.TransformationTypeFlatMapIterable: "flatMapIterable",
	api.TransformationTypeForEach:         "forEach",
	api.TransformationTypeInput:           "input",
	api.TransformationTypeJoin:            "join",
	api.TransformationTypeKeyBy:           "keyBy",
	api.TransformationTypeMap:             "map",
	api.TransformationTypeMerge:           "merge",
	api.TransformationTypeMultiJoin:       "multiJoin",
	api.TransformationTypeParallels:       "parallels",
	api.TransformationTypeSplit:           "split",
}

func (s *StreamConfig) GetTransformationName() string {
	return transformationNameMap[s.Type]
}

type ServiceConfig struct {
	Id                 int                    `yaml:"id"`
	Name               string                 `yaml:"name"`
	MonitoringPort     int                    `yaml:"monitoringPort"`
	MonitoringHost     string                 `yaml:"monitoringHost"`
	GrpcPort           int                    `yaml:"grpcPort"`
	GrpcHost           string                 `yaml:"grpcHost"`
	ShutdownTimeout    int                    `yaml:"shutdownTimeout"`
	Color              string                 `yaml:"color"`
	DefaultGrpcTimeout int                    `yaml:"defaultGrpcTimeout"`
	Properties         map[string]interface{} `mapstructure:",remain"`
}

func (s *ServiceConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type LinkConfig struct {
	From          int                    `yaml:"from"`
	To            int                    `yaml:"to"`
	CallSemantics api.CallSemantics      `yaml:"callSemantics"`
	Properties    map[string]interface{} `mapstructure:",remain"`
}

func (s *LinkConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type DataConnector struct {
	Id         int                    `yaml:"id"`
	Name       string                 `yaml:"name"`
	Type       api.DataConnectorType  `yaml:"type"`
	Properties map[string]interface{} `mapstructure:",remain"`
}

func (s *DataConnector) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type EndpointConfig struct {
	Id              int                    `yaml:"id"`
	Name            string                 `yaml:"name"`
	IdDataConnector int                    `yaml:"idDataConnector"`
	Properties      map[string]interface{} `mapstructure:",remain"`
}

func (s *EndpointConfig) GetProperty(name string) interface{} {
	return s.Properties[name]
}

func GetConfigProperty[T any](config ConfigProperties, name string) T {
	value := config.GetProperty(name)
	if value != nil {
		return value.(T)
	}
	var t T
	return t
}

type LinkId struct {
	From int
	To   int
}

type RuntimeConfig struct {
	StreamsByName        map[string]*StreamConfig
	ServicesByName       map[string]*ServiceConfig
	LinksById            map[LinkId]*LinkConfig
	DataConnectorsByName map[string]*DataConnector
	EndpointsByName      map[string]*EndpointConfig
	StreamsById          map[int]*StreamConfig
	ServicesById         map[int]*ServiceConfig
	DataConnectorsById   map[int]*DataConnector
	EndpointsById        map[int]*EndpointConfig
}

type ServiceAppConfig struct {
	Streams        []StreamConfig   `yaml:"streams"`
	Services       []ServiceConfig  `yaml:"services"`
	Links          []LinkConfig     `yaml:"links"`
	DataConnectors []DataConnector  `yaml:"dataConnectors"`
	Endpoints      []EndpointConfig `yaml:"endpoints"`
	runtimeConfig  *RuntimeConfig   `yaml:"-"`
}

func (cfg *ServiceAppConfig) initRuntimeConfig() {
	cfg.runtimeConfig = &RuntimeConfig{
		StreamsByName:        make(map[string]*StreamConfig),
		StreamsById:          make(map[int]*StreamConfig),
		ServicesByName:       make(map[string]*ServiceConfig),
		ServicesById:         make(map[int]*ServiceConfig),
		EndpointsById:        make(map[int]*EndpointConfig),
		DataConnectorsById:   make(map[int]*DataConnector),
		EndpointsByName:      make(map[string]*EndpointConfig),
		DataConnectorsByName: make(map[string]*DataConnector),
		LinksById:            make(map[LinkId]*LinkConfig),
	}
	for idx := range cfg.Streams {
		cfg.runtimeConfig.StreamsByName[cfg.Streams[idx].Name] = &cfg.Streams[idx]
		cfg.runtimeConfig.StreamsById[cfg.Streams[idx].Id] = &cfg.Streams[idx]
	}
	for idx := range cfg.Services {
		cfg.runtimeConfig.ServicesByName[cfg.Services[idx].Name] = &cfg.Services[idx]
		cfg.runtimeConfig.ServicesById[cfg.Services[idx].Id] = &cfg.Services[idx]
	}
	for idx := range cfg.Endpoints {
		cfg.runtimeConfig.EndpointsByName[cfg.Endpoints[idx].Name] = &cfg.Endpoints[idx]
		cfg.runtimeConfig.EndpointsById[cfg.Endpoints[idx].Id] = &cfg.Endpoints[idx]
	}
	for idx := range cfg.DataConnectors {
		cfg.runtimeConfig.DataConnectorsById[cfg.DataConnectors[idx].Id] = &cfg.DataConnectors[idx]
		cfg.runtimeConfig.DataConnectorsByName[cfg.DataConnectors[idx].Name] = &cfg.DataConnectors[idx]
	}
	for idx := range cfg.Links {
		cfg.runtimeConfig.LinksById[LinkId{From: cfg.Links[idx].From, To: cfg.Links[idx].To}] = &cfg.Links[idx]
	}
}

func (cfg *ServiceAppConfig) GetServiceConfig() *ServiceAppConfig {
	return cfg
}

func (cfg *ServiceAppConfig) GetConfig() Config {
	return cfg
}

func (cfg *ServiceAppConfig) GetStreamConfigByName(name string) *StreamConfig {
	return cfg.runtimeConfig.StreamsByName[name]
}

func (cfg *ServiceAppConfig) GetDataConnectorById(id int) *DataConnector {
	return cfg.runtimeConfig.DataConnectorsById[id]
}

func (cfg *ServiceAppConfig) GetEndpointConfigById(id int) *EndpointConfig {
	return cfg.runtimeConfig.EndpointsById[id]
}

func (cfg *ServiceAppConfig) GetServiceConfigByName(name string) *ServiceConfig {
	return cfg.runtimeConfig.ServicesByName[name]
}

func (cfg *ServiceAppConfig) GetServiceConfigById(id int) *ServiceConfig {
	return cfg.runtimeConfig.ServicesById[id]
}

func (cfg *ServiceAppConfig) GetStreamConfigById(id int) *StreamConfig {
	return cfg.runtimeConfig.StreamsById[id]
}

func (cfg *ServiceAppConfig) GetLink(from int, to int) *LinkConfig {
	return cfg.runtimeConfig.LinksById[LinkId{From: from, To: to}]
}
