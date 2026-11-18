package config

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gorundebug/servicelib/api"
)

// NATSDataConnectorConfig shares connection settings while Type preserves the
// distinct ephemeral Core NATS and durable JetStream contracts.
type NATSDataConnectorConfig struct {
	ID                  int                             `yaml:"id,omitempty" mapstructure:"id"`
	Name                string                          `yaml:"name,omitempty" mapstructure:"name"`
	Type                api.DataConnectorType           `yaml:"type,omitempty" mapstructure:"type"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation,omitempty" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	Servers             []string                        `yaml:"servers,omitempty" mapstructure:"servers"`
	ConnectTimeoutMs    int                             `yaml:"connectTimeoutMs,omitempty" mapstructure:"connectTimeoutMs"`
	OperationTimeoutMs  int                             `yaml:"operationTimeoutMs,omitempty" mapstructure:"operationTimeoutMs"`
	Username            string                          `yaml:"username,omitempty" mapstructure:"username"`
	Password            string                          `yaml:"password,omitempty" mapstructure:"password"`
	Token               string                          `yaml:"token,omitempty" mapstructure:"token"`
	CredentialsFile     string                          `yaml:"credentialsFile,omitempty" mapstructure:"credentialsFile"`
	TLSEnabled          bool                            `yaml:"tlsEnabled,omitempty" mapstructure:"tlsEnabled"`
	TLSServerName       string                          `yaml:"tlsServerName,omitempty" mapstructure:"tlsServerName"`
	TLSCAFile           string                          `yaml:"tlsCaFile,omitempty" mapstructure:"tlsCaFile"`
	TLSCertFile         string                          `yaml:"tlsCertFile,omitempty" mapstructure:"tlsCertFile"`
	TLSKeyFile          string                          `yaml:"tlsKeyFile,omitempty" mapstructure:"tlsKeyFile"`
	Properties          map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (c *NATSDataConnectorConfig) GetID() int                     { return c.ID }
func (c *NATSDataConnectorConfig) GetName() string                { return c.Name }
func (c *NATSDataConnectorConfig) GetType() api.DataConnectorType { return c.Type }
func (c *NATSDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return c.Implementation
}
func (c *NATSDataConnectorConfig) GetProperty(name string) interface{} { return c.Properties[name] }

// Timeouts are validated once before connecting, not per message.
func (c *NATSDataConnectorConfig) ConnectTimeout() time.Duration {
	if c.ConnectTimeoutMs == 0 {
		return 2 * time.Second
	}
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}
func (c *NATSDataConnectorConfig) OperationTimeout() time.Duration {
	if c.OperationTimeoutMs == 0 {
		return 5 * time.Second
	}
	return time.Duration(c.OperationTimeoutMs) * time.Millisecond
}

func (c *NATSDataConnectorConfig) Validate() error {
	if c == nil {
		return errors.New("NATS connector configuration is missing")
	}
	if c.Type != api.DataConnectorTypeNATS && c.Type != api.DataConnectorTypeJetStream {
		return errors.New("unsupported NATS connector type")
	}
	if len(c.Servers) == 0 {
		return errors.New("NATS servers must be configured")
	}
	tlsURL, plainURL := false, false
	for _, server := range c.Servers {
		u, err := url.Parse(server)
		if err != nil || u == nil || u.Hostname() == "" || (u.Scheme != "nats" && u.Scheme != "tls") ||
			u.User != nil || u.Opaque != "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			strings.Contains(server, "#") || strings.ContainsFunc(server, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return errors.New("NATS servers require nats:// or tls:// URLs without embedded credentials, query or path")
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return errors.New("NATS server port must be between 1 and 65535")
			}
		} else if strings.HasSuffix(u.Host, ":") {
			return errors.New("NATS server port must not be empty")
		}
		tlsURL = tlsURL || u.Scheme == "tls"
		plainURL = plainURL || u.Scheme == "nats"
	}
	if tlsURL && plainURL && !c.TLSEnabled {
		return errors.New("mixed TLS and plain NATS servers require tlsEnabled")
	}
	if !c.TLSEnabled && !tlsURL && (c.TLSCAFile != "" || c.TLSServerName != "" || c.TLSCertFile != "" || c.TLSKeyFile != "") {
		return errors.New("NATS TLS settings require tlsEnabled or tls:// servers")
	}
	const maxMillis = int64((1<<63 - 1) / int64(time.Millisecond))
	if c.ConnectTimeoutMs < 0 || c.OperationTimeoutMs < 0 || int64(c.ConnectTimeoutMs) > maxMillis || int64(c.OperationTimeoutMs) > maxMillis {
		return errors.New("NATS timeouts must be positive representable millisecond durations")
	}
	if (c.Username == "") != (c.Password == "") {
		return errors.New("NATS username and password must both be configured")
	}
	modes := 0
	if c.Username != "" {
		modes++
	}
	if c.Token != "" {
		modes++
	}
	if c.CredentialsFile != "" {
		modes++
	}
	if modes > 1 {
		return errors.New("NATS authentication modes are mutually exclusive")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("NATS TLS certificate and key must both be configured")
	}
	return nil
}

// NATSEndpointConfig describes native subject/subscription settings. Creation is
// opt-in; absent flags must never mutate existing streams or durable consumers.
type NATSEndpointConfig struct {
	ID                       int                    `yaml:"id,omitempty" mapstructure:"id"`
	Name                     string                 `yaml:"name,omitempty" mapstructure:"name"`
	IdDataConnector          int                    `yaml:"idDataConnector,omitempty" mapstructure:"idDataConnector"`
	Enabled                  bool                   `yaml:"enabled,omitempty" mapstructure:"enabled"`
	TracingEnabled           bool                   `yaml:"tracingEnabled,omitempty" mapstructure:"tracingEnabled"`
	Subject                  string                 `yaml:"subject,omitempty" mapstructure:"subject"`
	QueueGroup               string                 `yaml:"queueGroup,omitempty" mapstructure:"queueGroup"`
	StreamName               string                 `yaml:"streamName,omitempty" mapstructure:"streamName"`
	DurableName              string                 `yaml:"durableName,omitempty" mapstructure:"durableName"`
	CreateStream             bool                   `yaml:"createStream,omitempty" mapstructure:"createStream"`
	CreateConsumer           bool                   `yaml:"createConsumer,omitempty" mapstructure:"createConsumer"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *NATSEndpointConfig) GetID() int                          { return e.ID }
func (e *NATSEndpointConfig) GetName() string                     { return e.Name }
func (e *NATSEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *NATSEndpointConfig) GetTracingEnabled() bool             { return e.TracingEnabled }
func (e *NATSEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }
