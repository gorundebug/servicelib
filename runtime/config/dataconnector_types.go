/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import "github.com/gorundebug/servicelib/api"

// DataConnectorConfig is the interface for all data connector types.
type DataConnectorConfig interface {
	GetID() int
	GetName() string
	GetType() api.DataConnectorType
	GetImplementation() api.DataConnectorImplementation
	GetProperty(name string) interface{}
}

// HttpDataConnectorConfig is the configuration for an HTTP data connector.
type HttpDataConnectorConfig struct {
	ID                   int                             `yaml:"id" mapstructure:"id"`
	Name                 string                          `yaml:"name" mapstructure:"name"`
	Implementation       api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	Module               string                          `yaml:"module,omitempty" mapstructure:"module"`
	Host                 string                          `yaml:"host,omitempty" mapstructure:"host"`
	Port                 int                             `yaml:"port,omitempty" mapstructure:"port"`
	UseDedicatedListener bool                            `yaml:"useDedicatedListener,omitempty" mapstructure:"useDedicatedListener"`
	Properties           map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *HttpDataConnectorConfig) GetID() int                     { return d.ID }
func (d *HttpDataConnectorConfig) GetName() string                { return d.Name }
func (d *HttpDataConnectorConfig) GetType() api.DataConnectorType { return api.DataConnectorTypeHTTP }
func (d *HttpDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *HttpDataConnectorConfig) GetProperty(name string) interface{} { return d.Properties[name] }

// GrpcDataConnectorConfig is the configuration for a gRPC data connector.
type GrpcDataConnectorConfig struct {
	ID                  int                             `yaml:"id" mapstructure:"id"`
	Name                string                          `yaml:"name" mapstructure:"name"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	Module              string                          `yaml:"module,omitempty" mapstructure:"module"`
	Address             string                          `yaml:"address,omitempty" mapstructure:"address"`
	ConnectionsCount    int                             `yaml:"connectionsCount,omitempty" mapstructure:"connectionsCount"`
	Properties          map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *GrpcDataConnectorConfig) GetID() int                     { return d.ID }
func (d *GrpcDataConnectorConfig) GetName() string                { return d.Name }
func (d *GrpcDataConnectorConfig) GetType() api.DataConnectorType { return api.DataConnectorTypeGRPC }
func (d *GrpcDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *GrpcDataConnectorConfig) GetProperty(name string) interface{} { return d.Properties[name] }

// KafkaDataConnectorConfig is the configuration for a Kafka data connector.
type KafkaDataConnectorConfig struct {
	ID                  int                             `yaml:"id" mapstructure:"id"`
	Name                string                          `yaml:"name" mapstructure:"name"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	Brokers             string                          `yaml:"brokers,omitempty" mapstructure:"brokers"`
	Version             string                          `yaml:"version,omitempty" mapstructure:"version"`
	DialTimeout         float32                         `yaml:"dialTimeout,omitempty" mapstructure:"dialTimeout"`
	UsePartitioner      bool                            `yaml:"usePartitioner,omitempty" mapstructure:"usePartitioner"`
	Async               bool                            `yaml:"async,omitempty" mapstructure:"async"`
	SecurityProtocol    api.KafkaSecurityProtocol       `yaml:"securityProtocol,omitempty" mapstructure:"securityProtocol"`
	SaslMechanism       api.KafkaSaslMechanism          `yaml:"saslMechanism,omitempty" mapstructure:"saslMechanism"`
	Username            string                          `yaml:"username,omitempty" mapstructure:"username"`
	Password            string                          `yaml:"password,omitempty" mapstructure:"password"`
	Properties          map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *KafkaDataConnectorConfig) GetID() int                     { return d.ID }
func (d *KafkaDataConnectorConfig) GetName() string                { return d.Name }
func (d *KafkaDataConnectorConfig) GetType() api.DataConnectorType { return api.DataConnectorTypeKafka }
func (d *KafkaDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *KafkaDataConnectorConfig) GetProperty(name string) interface{} { return d.Properties[name] }

// CronDataConnectorConfig configures a process-local cron scheduler.
type CronDataConnectorConfig struct {
	ID             int                             `yaml:"id" mapstructure:"id"`
	Name           string                          `yaml:"name" mapstructure:"name"`
	Implementation api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	Properties     map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *CronDataConnectorConfig) GetID() int                     { return d.ID }
func (d *CronDataConnectorConfig) GetName() string                { return d.Name }
func (d *CronDataConnectorConfig) GetType() api.DataConnectorType { return api.DataConnectorTypeCron }
func (d *CronDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *CronDataConnectorConfig) GetProperty(name string) interface{} { return d.Properties[name] }

// TemporalDataConnectorConfig configures one Temporal client and its Workers.
// Connection and capacity fields are reloadable runtime policy; graph objects
// retain only this connector's immutable ID.
type TemporalDataConnectorConfig struct {
	ID                      int                             `yaml:"id" mapstructure:"id"`
	Name                    string                          `yaml:"name" mapstructure:"name"`
	Implementation          api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	Address                 string                          `yaml:"address" mapstructure:"address"`
	Namespace               string                          `yaml:"namespace" mapstructure:"namespace"`
	Identity                string                          `yaml:"identity,omitempty" mapstructure:"identity"`
	APIKey                  string                          `yaml:"apiKey,omitempty" mapstructure:"apiKey"`
	TLSEnabled              bool                            `yaml:"tlsEnabled,omitempty" mapstructure:"tlsEnabled"`
	TLSServerName           string                          `yaml:"tlsServerName,omitempty" mapstructure:"tlsServerName"`
	TLSCAFile               string                          `yaml:"tlsCaFile,omitempty" mapstructure:"tlsCaFile"`
	TLSCertFile             string                          `yaml:"tlsCertFile,omitempty" mapstructure:"tlsCertFile"`
	TLSKeyFile              string                          `yaml:"tlsKeyFile,omitempty" mapstructure:"tlsKeyFile"`
	MaxConcurrentActivities int                             `yaml:"maxConcurrentActivities,omitempty" mapstructure:"maxConcurrentActivities"`
	MaxConcurrentWorkflows  int                             `yaml:"maxConcurrentWorkflows,omitempty" mapstructure:"maxConcurrentWorkflows"`
	Properties              map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *TemporalDataConnectorConfig) GetID() int      { return d.ID }
func (d *TemporalDataConnectorConfig) GetName() string { return d.Name }
func (d *TemporalDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypeTemporal
}
func (d *TemporalDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *TemporalDataConnectorConfig) GetProperty(name string) interface{} {
	return d.Properties[name]
}

// CustomDataConnectorConfig is the configuration for a custom data connector.
type CustomDataConnectorConfig struct {
	ID             int                             `yaml:"id" mapstructure:"id"`
	Name           string                          `yaml:"name" mapstructure:"name"`
	Implementation api.DataConnectorImplementation `yaml:"implementation" mapstructure:"implementation"`
	Properties     map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (d *CustomDataConnectorConfig) GetID() int      { return d.ID }
func (d *CustomDataConnectorConfig) GetName() string { return d.Name }
func (d *CustomDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypeCustom
}
func (d *CustomDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return d.Implementation
}
func (d *CustomDataConnectorConfig) GetProperty(name string) interface{} { return d.Properties[name] }
