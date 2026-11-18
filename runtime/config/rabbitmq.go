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

// RabbitMQDataConnectorConfig configures the official RabbitMQ AMQP 1.0 client.
type RabbitMQDataConnectorConfig struct {
	ID                  int                             `yaml:"id,omitempty" mapstructure:"id"`
	Name                string                          `yaml:"name,omitempty" mapstructure:"name"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation,omitempty" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	VirtualHost         string                          `yaml:"virtualHost,omitempty" mapstructure:"virtualHost"`
	Servers             []string                        `yaml:"servers,omitempty" mapstructure:"servers"`
	ConnectTimeoutMs    int                             `yaml:"connectTimeoutMs,omitempty" mapstructure:"connectTimeoutMs"`
	OperationTimeoutMs  int                             `yaml:"operationTimeoutMs,omitempty" mapstructure:"operationTimeoutMs"`
	Username            string                          `yaml:"username,omitempty" mapstructure:"username"`
	Password            string                          `yaml:"password,omitempty" mapstructure:"password"`
	TLSEnabled          bool                            `yaml:"tlsEnabled,omitempty" mapstructure:"tlsEnabled"`
	TLSServerName       string                          `yaml:"tlsServerName,omitempty" mapstructure:"tlsServerName"`
	TLSCAFile           string                          `yaml:"tlsCaFile,omitempty" mapstructure:"tlsCaFile"`
	TLSCertFile         string                          `yaml:"tlsCertFile,omitempty" mapstructure:"tlsCertFile"`
	TLSKeyFile          string                          `yaml:"tlsKeyFile,omitempty" mapstructure:"tlsKeyFile"`
	Properties          map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (c *RabbitMQDataConnectorConfig) GetID() int      { return c.ID }
func (c *RabbitMQDataConnectorConfig) GetName() string { return c.Name }
func (c *RabbitMQDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypeRabbitMQ
}
func (c *RabbitMQDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return c.Implementation
}
func (c *RabbitMQDataConnectorConfig) GetProperty(name string) interface{} { return c.Properties[name] }

// Timeouts are validated once before connecting, not per message.
func (c *RabbitMQDataConnectorConfig) ConnectTimeout() time.Duration {
	if c.ConnectTimeoutMs == 0 {
		return 2 * time.Second
	}
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}
func (c *RabbitMQDataConnectorConfig) OperationTimeout() time.Duration {
	if c.OperationTimeoutMs == 0 {
		return 5 * time.Second
	}
	return time.Duration(c.OperationTimeoutMs) * time.Millisecond
}

func (c *RabbitMQDataConnectorConfig) Validate() error {
	if c == nil {
		return errors.New("RabbitMQ connector configuration is missing")
	}
	if c.Implementation != "" && c.Implementation != api.DataConnectorImplementationRabbitMQAMQPGo {
		return errors.New("RabbitMQ Go requires the official rabbitmq-amqp-go-client AMQP 1.0 implementation")
	}
	for _, unsupported := range []string{"token", "credentialsFile"} {
		if _, present := c.Properties[unsupported]; present {
			return errors.New("RabbitMQ does not support token or credentialsFile authentication")
		}
	}
	if len(c.Servers) == 0 {
		return errors.New("RabbitMQ servers must be configured")
	}
	tlsURL, plainURL := false, false
	for _, server := range c.Servers {
		u, err := url.Parse(server)
		if err != nil || u == nil || u.Hostname() == "" || (u.Scheme != "amqp" && u.Scheme != "amqps") ||
			u.User != nil || u.Opaque != "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			strings.Contains(server, "#") || strings.ContainsFunc(server, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return errors.New("RabbitMQ servers require amqp:// or amqps:// URLs without embedded credentials, query or path")
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return errors.New("RabbitMQ server port must be between 1 and 65535")
			}
		} else if strings.HasSuffix(u.Host, ":") {
			return errors.New("RabbitMQ server port must not be empty")
		}
		tlsURL = tlsURL || u.Scheme == "amqps"
		plainURL = plainURL || u.Scheme == "amqp"
	}
	if tlsURL && plainURL && !c.TLSEnabled {
		return errors.New("mixed TLS and plain RabbitMQ servers require tlsEnabled")
	}
	if !c.TLSEnabled && !tlsURL && (c.TLSCAFile != "" || c.TLSServerName != "" || c.TLSCertFile != "" || c.TLSKeyFile != "") {
		return errors.New("RabbitMQ TLS settings require tlsEnabled or amqps:// servers")
	}
	const maxMillis = int64((1<<63 - 1) / int64(time.Millisecond))
	if c.ConnectTimeoutMs < 0 || c.OperationTimeoutMs < 0 || int64(c.ConnectTimeoutMs) > maxMillis || int64(c.OperationTimeoutMs) > maxMillis {
		return errors.New("RabbitMQ timeouts must be positive representable millisecond durations")
	}
	if (c.Username == "") != (c.Password == "") {
		return errors.New("RabbitMQ username and password must both be configured")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("RabbitMQ TLS certificate and key must both be configured")
	}
	if strings.ContainsFunc(c.VirtualHost, unicode.IsControl) {
		return errors.New("RabbitMQ virtualHost contains control characters")
	}
	return nil
}

type RabbitMQEndpointConfig struct {
	ID                       int                      `yaml:"id,omitempty" mapstructure:"id"`
	Name                     string                   `yaml:"name,omitempty" mapstructure:"name"`
	IdDataConnector          int                      `yaml:"idDataConnector,omitempty" mapstructure:"idDataConnector"`
	Enabled                  bool                     `yaml:"enabled,omitempty" mapstructure:"enabled"`
	TracingEnabled           bool                     `yaml:"tracingEnabled,omitempty" mapstructure:"tracingEnabled"`
	QueueName                string                   `yaml:"queueName,omitempty" mapstructure:"queueName"`
	ExchangeName             string                   `yaml:"exchangeName,omitempty" mapstructure:"exchangeName"`
	RoutingKey               string                   `yaml:"routingKey,omitempty" mapstructure:"routingKey"`
	QueueType                api.RabbitMQQueueType    `yaml:"queueType,omitempty" mapstructure:"queueType"`
	ExchangeType             api.RabbitMQExchangeType `yaml:"exchangeType,omitempty" mapstructure:"exchangeType"`
	CreateQueue              bool                     `yaml:"createQueue,omitempty" mapstructure:"createQueue"`
	CreateExchange           bool                     `yaml:"createExchange,omitempty" mapstructure:"createExchange"`
	CreateBinding            bool                     `yaml:"createBinding,omitempty" mapstructure:"createBinding"`
	ConsumerCredits          int                      `yaml:"consumerCredits,omitempty" mapstructure:"consumerCredits"`
	PublisherMaxInFlight     int                      `yaml:"publisherMaxInFlight,omitempty" mapstructure:"publisherMaxInFlight"`
	FunctionName             string                   `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                   `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionModule           string                   `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	FunctionDescription      string                   `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                   `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	PublicFunction           bool                     `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	Properties               map[string]interface{}   `yaml:",inline" mapstructure:",remain"`
}

func (e *RabbitMQEndpointConfig) GetID() int                          { return e.ID }
func (e *RabbitMQEndpointConfig) GetName() string                     { return e.Name }
func (e *RabbitMQEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *RabbitMQEndpointConfig) GetTracingEnabled() bool             { return e.TracingEnabled }
func (e *RabbitMQEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }

func (e *RabbitMQEndpointConfig) Credits() int {
	if e.ConsumerCredits == 0 {
		return 256
	}
	return e.ConsumerCredits
}
func (e *RabbitMQEndpointConfig) MaxInFlight() int {
	if e.PublisherMaxInFlight == 0 {
		return 256
	}
	return e.PublisherMaxInFlight
}
func (e *RabbitMQEndpointConfig) Validate(source bool) error {
	if e == nil {
		return errors.New("RabbitMQ endpoint configuration is missing")
	}
	if source && e.QueueName == "" {
		return errors.New("RabbitMQ source queueName is required")
	}
	if !source && e.QueueName == "" && e.ExchangeName == "" {
		return errors.New("RabbitMQ sink requires queueName or exchangeName")
	}
	if (e.CreateQueue || e.QueueType != "") && e.QueueName == "" {
		return errors.New("RabbitMQ queue settings require queueName")
	}
	if (e.CreateExchange || e.ExchangeType != "" || e.RoutingKey != "") && e.ExchangeName == "" {
		return errors.New("RabbitMQ exchange settings require exchangeName")
	}
	if e.CreateBinding && (e.QueueName == "" || e.ExchangeName == "") {
		return errors.New("RabbitMQ createBinding requires queueName and exchangeName")
	}
	if e.QueueType != "" && e.QueueType != api.Quorum && e.QueueType != api.Classic {
		return errors.New("RabbitMQ queueType must be quorum or classic")
	}
	if e.ExchangeType != "" && e.ExchangeType != api.Direct && e.ExchangeType != api.Topic && e.ExchangeType != api.Fanout {
		return errors.New("RabbitMQ exchangeType must be direct, topic or fanout")
	}
	if e.ExchangeType != "" && !e.CreateExchange {
		return errors.New("RabbitMQ exchangeType is a creation setting: AMQP 1.0 cannot inspect an existing exchange type without declaring it")
	}
	if e.ConsumerCredits < 0 || e.ConsumerCredits > 4096 || e.PublisherMaxInFlight < 0 || e.PublisherMaxInFlight > 4096 {
		return errors.New("RabbitMQ native admission limits must be 1..4096 or omitted")
	}
	if source && e.PublisherMaxInFlight != 0 {
		return errors.New("RabbitMQ source cannot set publisherMaxInFlight")
	}
	if !source && e.ConsumerCredits != 0 {
		return errors.New("RabbitMQ sink cannot set consumerCredits")
	}
	for _, v := range []string{e.QueueName, e.ExchangeName, e.RoutingKey} {
		if strings.ContainsFunc(v, unicode.IsControl) {
			return errors.New("RabbitMQ entity names and keys cannot contain control characters")
		}
	}
	return nil
}
