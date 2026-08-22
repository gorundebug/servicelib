/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import "github.com/gorundebug/servicelib/api"

// EndpointConfig is the interface for all endpoint types.
type EndpointConfig interface {
	GetID() int
	GetName() string
	GetIdDataConnector() int
	GetProperty(name string) interface{}
}

// HttpEndpointConfig is the configuration for an HTTP endpoint.
type HttpEndpointConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	IdDataConnector          int                    `yaml:"idDataConnector" mapstructure:"idDataConnector"`
	HttpMethodType           api.HTTPMethodType     `yaml:"httpMethodType,omitempty" mapstructure:"httpMethodType"`
	Path                     string                 `yaml:"path,omitempty" mapstructure:"path"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *HttpEndpointConfig) GetID() int                          { return e.ID }
func (e *HttpEndpointConfig) GetName() string                     { return e.Name }
func (e *HttpEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *HttpEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }

// GrpcEndpointConfig is the configuration for a gRPC endpoint.
type GrpcEndpointConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	GrpcMethodType           api.GrpcMethodType     `yaml:"grpcMethodType,omitempty" mapstructure:"grpcMethodType"`
	MethodName               string                 `yaml:"methodName,omitempty" mapstructure:"methodName"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	IdDataConnector          int                    `yaml:"idDataConnector" mapstructure:"idDataConnector"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *GrpcEndpointConfig) GetID() int                          { return e.ID }
func (e *GrpcEndpointConfig) GetName() string                     { return e.Name }
func (e *GrpcEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *GrpcEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }

// KafkaEndpointConfig is the configuration for a Kafka endpoint.
type KafkaEndpointConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	IdDataConnector          int                    `yaml:"idDataConnector" mapstructure:"idDataConnector"`
	Enabled                  bool                   `yaml:"enabled" mapstructure:"enabled"`
	CreateTopic              bool                   `yaml:"createTopic,omitempty" mapstructure:"createTopic"`
	Topic                    string                 `yaml:"topic,omitempty" mapstructure:"topic"`
	Partitions               int                    `yaml:"partitions,omitempty" mapstructure:"partitions"`
	ConsumerGroup            string                 `yaml:"consumerGroup,omitempty" mapstructure:"consumerGroup"`
	ReplicationFactor        int                    `yaml:"replicationFactor,omitempty" mapstructure:"replicationFactor"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *KafkaEndpointConfig) GetID() int                          { return e.ID }
func (e *KafkaEndpointConfig) GetName() string                     { return e.Name }
func (e *KafkaEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *KafkaEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }

// CustomEndpointConfig is the configuration for a custom endpoint.
type CustomEndpointConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	IdDataConnector          int                    `yaml:"idDataConnector" mapstructure:"idDataConnector"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *CustomEndpointConfig) GetID() int                          { return e.ID }
func (e *CustomEndpointConfig) GetName() string                     { return e.Name }
func (e *CustomEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *CustomEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }
