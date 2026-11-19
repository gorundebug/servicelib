package config

import (
	"fmt"
	"github.com/gorundebug/servicelib/api"
)

// PostgreSQLBunDataConnectorConfig selects Bun, not an interchangeable library.
// ConnectionString is runtime configuration and may be supplied through env.
type PostgreSQLBunDataConnectorConfig struct {
	ID                 int                    `yaml:"id" mapstructure:"id"`
	Name               string                 `yaml:"name" mapstructure:"name"`
	ConnectionString   string                 `yaml:"connectionString" mapstructure:"connectionString"`
	ConnectionsCount   int                    `yaml:"connectionsCount" mapstructure:"connectionsCount"`
	ConnectTimeoutMs   int                    `yaml:"connectTimeoutMs" mapstructure:"connectTimeoutMs"`
	OperationTimeoutMs int                    `yaml:"operationTimeoutMs" mapstructure:"operationTimeoutMs"`
	Properties         map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (c *PostgreSQLBunDataConnectorConfig) GetID() int      { return c.ID }
func (c *PostgreSQLBunDataConnectorConfig) GetName() string { return c.Name }
func (c *PostgreSQLBunDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypePostgreSQLBun
}
func (c *PostgreSQLBunDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return "go/bun"
}
func (c *PostgreSQLBunDataConnectorConfig) GetProperty(name string) interface{} {
	return c.Properties[name]
}

// QueryEndpointConfig carries a storage operation, not a source/sink direction.
type QueryEndpointConfig struct {
	ID                       int                        `yaml:"id" mapstructure:"id"`
	Name                     string                     `yaml:"name" mapstructure:"name"`
	IdDataConnector          int                        `yaml:"idDataConnector" mapstructure:"idDataConnector"`
	Enabled                  bool                       `yaml:"enabled" mapstructure:"enabled"`
	TracingEnabled           bool                       `yaml:"tracingEnabled" mapstructure:"tracingEnabled"`
	QueryOperation           api.EndpointQueryOperation `yaml:"queryOperation" mapstructure:"queryOperation"`
	SQLFile                  string                     `yaml:"sqlFile,omitempty" mapstructure:"sqlFile"`
	FetchSize                int                        `yaml:"fetchSize" mapstructure:"fetchSize"`
	Bucket                   string                     `yaml:"bucket,omitempty" mapstructure:"bucket"`
	FunctionName             string                     `yaml:"functionName" mapstructure:"functionName"`
	FunctionPackage          string                     `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionModule           string                     `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	FunctionDescription      string                     `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                     `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	PublicFunction           bool                       `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	Properties               map[string]interface{}     `yaml:",inline" mapstructure:",remain"`
}

func (c *QueryEndpointConfig) GetID() int                          { return c.ID }
func (c *QueryEndpointConfig) GetName() string                     { return c.Name }
func (c *QueryEndpointConfig) GetIdDataConnector() int             { return c.IdDataConnector }
func (c *QueryEndpointConfig) GetTracingEnabled() bool             { return c.TracingEnabled }
func (c *QueryEndpointConfig) GetProperty(name string) interface{} { return c.Properties[name] }

// S3AWSDataConnectorConfig contains non-secret transport settings only.
// AWS credentials are resolved by the official runtime provider chain.
type S3AWSDataConnectorConfig struct {
	ID             int                    `yaml:"id" mapstructure:"id"`
	Name           string                 `yaml:"name" mapstructure:"name"`
	Region         string                 `yaml:"region" mapstructure:"region"`
	Address        string                 `yaml:"address" mapstructure:"address"`
	ForcePathStyle bool                   `yaml:"forcePathStyle" mapstructure:"forcePathStyle"`
	Properties     map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (c *S3AWSDataConnectorConfig) GetID() int                     { return c.ID }
func (c *S3AWSDataConnectorConfig) GetName() string                { return c.Name }
func (c *S3AWSDataConnectorConfig) GetType() api.DataConnectorType { return api.DataConnectorTypeS3AWS }
func (c *S3AWSDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return "go/aws"
}
func (c *S3AWSDataConnectorConfig) GetProperty(name string) interface{} { return c.Properties[name] }

// ValidateQueryEndpoint binds operation semantics to the selected native connector.
func ValidateQueryEndpoint(endpoint *QueryEndpointConfig, connector DataConnectorConfig) error {
	if connector == nil {
		return fmt.Errorf("Query endpoint %q references a missing connector", endpoint.Name)
	}
	valid := false
	switch connector.GetType() {
	case api.DataConnectorTypePostgreSQLBun:
		valid = endpoint.QueryOperation == "Rows" || endpoint.QueryOperation == "Execute"
	case api.DataConnectorTypeS3AWS:
		switch endpoint.QueryOperation {
		case "GetObject", "HeadObject", "ListObjects", "PutObject", "DeleteObject":
			valid = true
		}
	}
	if !valid {
		return fmt.Errorf("Query endpoint %q operation %q is not supported by connector %q", endpoint.Name, endpoint.QueryOperation, connector.GetName())
	}
	return nil
}
