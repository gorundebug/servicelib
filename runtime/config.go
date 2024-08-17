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
	Id              int                    `yaml:"id"`
	Name            string                 `yaml:"name"`
	MonitoringPort  int                    `yaml:"monitoringPort"`
	MonitoringIp    string                 `yaml:"monitoringIp"`
	GrpcPort        int                    `yaml:"grpcPort"`
	GrpcIp          string                 `yaml:"grpcIp"`
	ShutdownTimeout int                    `yaml:"shutdownTimeout"`
	Color           string                 `yaml:"color"`
	Properties      map[string]interface{} `mapstructure:",remain"`
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
	ID         int                    `yaml:"id"`
	Name       string                 `yaml:"name"`
	Type       api.DataConnectorType  `yaml:"type"`
	Properties map[string]interface{} `mapstructure:",remain"`
}

func (s *DataConnector) GetProperty(name string) interface{} {
	return s.Properties[name]
}

type EndpointConfig struct {
	ID              int                    `yaml:"id"`
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

type ServiceAppConfig struct {
	Streams        []StreamConfig   `yaml:"streams"`
	Services       []ServiceConfig  `yaml:"services"`
	Links          []LinkConfig     `yaml:"links"`
	DataConnectors []DataConnector  `yaml:"dataConnectors"`
	Endpoints      []EndpointConfig `yaml:"endpoints"`
}

func (cfg *ServiceAppConfig) GetServiceConfig() *ServiceAppConfig {
	return cfg
}

func (cfg *ServiceAppConfig) GetConfig() Config {
	return cfg
}

func (cfg *ServiceAppConfig) GetStreamConfigByName(name string) *StreamConfig {
	for idx := range cfg.Streams {
		stream := &cfg.Streams[idx]
		if stream.Name == name {
			return stream
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetDataConnectorById(id int) *DataConnector {
	for idx := range cfg.DataConnectors {
		dataConnector := &cfg.DataConnectors[idx]
		if dataConnector.ID == id {
			return dataConnector
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetEndpointConfigById(id int) *EndpointConfig {
	for idx := range cfg.Endpoints {
		endpoint := &cfg.Endpoints[idx]
		if endpoint.ID == id {
			return endpoint
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetServiceConfigByName(name string) *ServiceConfig {
	for idx := range cfg.Services {
		service := &cfg.Services[idx]
		if service.Name == name {
			return service
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetServiceConfigById(id int) *ServiceConfig {
	for idx := range cfg.Services {
		service := &cfg.Services[idx]
		if service.Id == id {
			return service
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetStreamConfigById(id int) *StreamConfig {
	for idx := range cfg.Streams {
		stream := &cfg.Streams[idx]
		if stream.Id == id {
			return stream
		}
	}
	return nil
}

func (cfg *ServiceAppConfig) GetCallSemantics(from int, to int) api.CallSemantics {
	for idx := range cfg.Links {
		link := &cfg.Links[idx]
		if link.From == from && link.To == to {
			return link.CallSemantics
		}
	}
	return api.FunctionCall
}
