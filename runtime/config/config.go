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
	Color                  string                     `yaml:"color" mapstructure:"color"`
	DefaultCallSemantics   *CallSemanticsGroup        `yaml:"defaultCallSemantics,omitempty" mapstructure:"defaultCallSemantics"`
	DefaultGrpcTimeout     int                        `yaml:"defaultGrpcTimeout,omitempty" mapstructure:"defaultGrpcTimeout"`
	Environment            api.Environment            `yaml:"environment" mapstructure:"environment"`
	GolangVersion          string                     `yaml:"golangVersion,omitempty" mapstructure:"golangVersion"`
	GrpcHost               string                     `yaml:"grpcHost" mapstructure:"grpcHost"`
	GrpcPort               int                        `yaml:"grpcPort" mapstructure:"grpcPort"`
	HttpHost               string                     `yaml:"httpHost" mapstructure:"httpHost"`
	HttpPort               int                        `yaml:"httpPort" mapstructure:"httpPort"`
	ID                     int                        `yaml:"id" mapstructure:"id"`
	LogLevel               api.LogLevel               `yaml:"logLevel,omitempty" mapstructure:"logLevel"`
	MetricsHandler         string                     `yaml:"metricsHandler" mapstructure:"metricsHandler"`
	StartupHandler         string                     `yaml:"startupHandler" mapstructure:"startupHandler"`
	ReadinessHandler       string                     `yaml:"readinessHandler" mapstructure:"readinessHandler"`
	LivenessHandler        string                     `yaml:"livenessHandler" mapstructure:"livenessHandler"`
	KubernetesWorkloadType api.KubernetesWorkloadType `yaml:"kubernetesWorkloadType" mapstructure:"kubernetesWorkloadType"`
	ModulePath             string                     `yaml:"modulePath,omitempty" mapstructure:"modulePath"`
	Name                   string                     `yaml:"name" mapstructure:"name"`
	ShutdownTimeout        int                        `yaml:"shutdownTimeout" mapstructure:"shutdownTimeout"`
	StatusHandler          string                     `yaml:"statusHandler" mapstructure:"statusHandler"`
	Properties             map[string]interface{}     `yaml:",inline" mapstructure:",remain"`
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
		if _, exists := runtimeCfg.streamsByName[v.GetName()]; exists {
			return nil, fmt.Errorf("duplicate stream name: %s", v.GetName())
		}
		if _, exists := runtimeCfg.streamsByID[v.GetID()]; exists {
			return nil, fmt.Errorf("duplicate stream id: %d", v.GetID())
		}
		runtimeCfg.streamsByName[v.GetName()] = v
		runtimeCfg.streamsByID[v.GetID()] = v
	}
	for _, v := range config.GetServices() {
		if _, exists := runtimeCfg.servicesByName[v.Name]; exists {
			return nil, fmt.Errorf("duplicate service name: %s", v.Name)
		}
		if _, exists := runtimeCfg.servicesByID[v.ID]; exists {
			return nil, fmt.Errorf("duplicate service id: %d", v.ID)
		}
		runtimeCfg.servicesByName[v.Name] = v
		runtimeCfg.servicesByID[v.ID] = v
	}
	for _, v := range config.GetEndpoints() {
		if _, exists := runtimeCfg.endpointsByName[v.GetName()]; exists {
			return nil, fmt.Errorf("duplicate endpoint name: %s", v.GetName())
		}
		if _, exists := runtimeCfg.endpointsByID[v.GetID()]; exists {
			return nil, fmt.Errorf("duplicate endpoint id: %d", v.GetID())
		}
		runtimeCfg.endpointsByName[v.GetName()] = v
		runtimeCfg.endpointsByID[v.GetID()] = v
	}
	for _, v := range config.GetDataConnectors() {
		if _, exists := runtimeCfg.dataConnectorsByName[v.GetName()]; exists {
			return nil, fmt.Errorf("duplicate data connector name: %s", v.GetName())
		}
		if _, exists := runtimeCfg.dataConnectorsByID[v.GetID()]; exists {
			return nil, fmt.Errorf("duplicate data connector id: %d", v.GetID())
		}
		runtimeCfg.dataConnectorsByID[v.GetID()] = v
		runtimeCfg.dataConnectorsByName[v.GetName()] = v
		if temporalConnector, ok := v.(*TemporalDataConnectorConfig); ok {
			if temporalConnector.Address == "" || temporalConnector.Namespace == "" {
				return nil, fmt.Errorf("Temporal data connector %q requires address and namespace", temporalConnector.Name)
			}
			if temporalConnector.MaxConcurrentActivities < 1 || temporalConnector.MaxConcurrentWorkflows < 1 {
				return nil, fmt.Errorf("Temporal data connector %q requires positive worker capacities", temporalConnector.Name)
			}
			if temporalConnector.WorkerStopTimeout < 0 {
				return nil, fmt.Errorf("Temporal data connector %q workerStopTimeout must not be negative", temporalConnector.Name)
			}
			if (temporalConnector.TLSCertFile == "") != (temporalConnector.TLSKeyFile == "") {
				return nil, fmt.Errorf("Temporal data connector %q requires both tlsCertFile and tlsKeyFile", temporalConnector.Name)
			}
			if (temporalConnector.TLSCAFile != "" || temporalConnector.TLSCertFile != "" || temporalConnector.TLSServerName != "") && !temporalConnector.TLSEnabled {
				return nil, fmt.Errorf("Temporal data connector %q requires tlsEnabled when TLS files or server name are configured", temporalConnector.Name)
			}
			switch temporalConnector.Implementation {
			case api.DataConnectorImplementationTemporalGo,
				api.DataConnectorImplementationTemporalPython,
				api.DataConnectorImplementationTemporalTypeScript:
			default:
				return nil, fmt.Errorf("Temporal data connector %q has unsupported implementation %q", temporalConnector.Name, temporalConnector.Implementation)
			}
		}
	}
	for _, endpoint := range config.GetEndpoints() {
		if cronEndpoint, ok := endpoint.(*CronEndpointConfig); ok {
			connector := runtimeCfg.dataConnectorsByID[cronEndpoint.IdDataConnector]
			if connector == nil || connector.GetType() != api.DataConnectorTypeCron {
				return nil, fmt.Errorf("Cron endpoint %q requires a Cron data connector id=%d", cronEndpoint.Name, cronEndpoint.IdDataConnector)
			}
			if cronEndpoint.Timezone != "UTC" {
				return nil, fmt.Errorf("Cron endpoint %q requires timezone UTC", cronEndpoint.Name)
			}
		}
		temporalEndpoint, ok := endpoint.(*TemporalEndpointConfig)
		if !ok {
			continue
		}
		connector := runtimeCfg.dataConnectorsByID[temporalEndpoint.IdDataConnector]
		if connector == nil || connector.GetType() != api.DataConnectorTypeTemporal {
			return nil, fmt.Errorf("Temporal endpoint %q requires a Temporal data connector id=%d", temporalEndpoint.Name, temporalEndpoint.IdDataConnector)
		}
		if temporalEndpoint.TaskQueue == "" {
			return nil, fmt.Errorf("Temporal endpoint %q requires taskQueue", temporalEndpoint.Name)
		}
		if temporalEndpoint.TemporalExecutionType != api.Activity && temporalEndpoint.TemporalExecutionType != api.Workflow {
			return nil, fmt.Errorf("Temporal endpoint %q requires temporalExecutionType Activity or Workflow", temporalEndpoint.Name)
		}
		if temporalEndpoint.TemporalExecutionType == api.Activity && temporalEndpoint.ActivityStartToCloseTimeout < 1 {
			return nil, fmt.Errorf("Temporal endpoint %q requires activityStartToCloseTimeout", temporalEndpoint.Name)
		}
		if temporalEndpoint.MaximumAttempts < 1 {
			return nil, fmt.Errorf("Temporal endpoint %q requires maximumAttempts", temporalEndpoint.Name)
		}
		if temporalEndpoint.Schedule != "" {
			if temporalEndpoint.ScheduleID == "" || temporalEndpoint.Timezone == "" {
				return nil, fmt.Errorf("scheduled Temporal endpoint %q requires scheduleId and timezone", temporalEndpoint.Name)
			}
			if temporalEndpoint.Timezone != "UTC" {
				return nil, fmt.Errorf("scheduled Temporal endpoint %q requires timezone UTC", temporalEndpoint.Name)
			}
			if temporalEndpoint.OverlapPolicy == "" || temporalEndpoint.MissedRunPolicy == "" {
				return nil, fmt.Errorf("scheduled Temporal endpoint %q requires overlapPolicy and missedRunPolicy", temporalEndpoint.Name)
			}
		} else if temporalEndpoint.ScheduleID != "" ||
			(temporalEndpoint.Timezone != "" && temporalEndpoint.Timezone != "UTC") {
			return nil, fmt.Errorf("on-demand Temporal endpoint %q cannot configure scheduleId or timezone without schedule", temporalEndpoint.Name)
		}
	}
	for _, v := range config.GetPools() {
		if _, exists := runtimeCfg.poolByName[v.Name]; exists {
			return nil, fmt.Errorf("duplicate pool name: %s", v.Name)
		}
		runtimeCfg.poolByName[v.Name] = v
	}
	for _, v := range config.GetTypes() {
		if _, exists := runtimeCfg.typesByName[v.Name]; exists {
			return nil, fmt.Errorf("duplicate type name: %s", v.Name)
		}
		runtimeCfg.typesByName[v.Name] = v
	}
	for _, v := range config.GetLinks() {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("validate link error: %w", err)
		}
		id := LinkID{From: v.From, To: v.To}
		if _, exists := runtimeCfg.linksByID[id]; exists {
			return nil, fmt.Errorf("duplicate link from=%d to=%d", v.From, v.To)
		}
		runtimeCfg.linksByID[id] = v
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

func (cfg *RuntimeConfig) GetEndpointConfigByName(name string) EndpointConfig {
	return cfg.endpointsByName[name]
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
