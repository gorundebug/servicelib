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

// RedisStreamsDataConnectorConfig configures the official go-redis client.
type RedisStreamsDataConnectorConfig struct {
	ID                  int                             `yaml:"id,omitempty" mapstructure:"id"`
	Name                string                          `yaml:"name,omitempty" mapstructure:"name"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation,omitempty" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	RedisDatabase       int                             `yaml:"redisDatabase,omitempty" mapstructure:"redisDatabase"`
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

func (c *RedisStreamsDataConnectorConfig) GetID() int      { return c.ID }
func (c *RedisStreamsDataConnectorConfig) GetName() string { return c.Name }
func (c *RedisStreamsDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypeRedisStreams
}
func (c *RedisStreamsDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return c.Implementation
}
func (c *RedisStreamsDataConnectorConfig) GetProperty(name string) interface{} {
	return c.Properties[name]
}

// Timeouts are validated once before connecting, not per message.
func (c *RedisStreamsDataConnectorConfig) ConnectTimeout() time.Duration {
	if c.ConnectTimeoutMs == 0 {
		return 2 * time.Second
	}
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}
func (c *RedisStreamsDataConnectorConfig) OperationTimeout() time.Duration {
	if c.OperationTimeoutMs == 0 {
		return 5 * time.Second
	}
	return time.Duration(c.OperationTimeoutMs) * time.Millisecond
}

func (c *RedisStreamsDataConnectorConfig) Validate() error {
	if c == nil {
		return errors.New("RedisStreams connector configuration is missing")
	}
	if c.Implementation != "" && c.Implementation != api.DataConnectorImplementationGoRedis {
		return errors.New("RedisStreams Go requires the official go-redis implementation")
	}
	for _, unsupported := range []string{"token", "credentialsFile"} {
		if _, present := c.Properties[unsupported]; present {
			return errors.New("RedisStreams does not support token or credentialsFile authentication")
		}
	}
	if len(c.Servers) != 1 {
		return errors.New("RedisStreams requires exactly one standalone server")
	}
	tlsURL := false
	for _, server := range c.Servers {
		u, err := url.Parse(server)
		if err != nil || u == nil || u.Hostname() == "" || (u.Scheme != "redis" && u.Scheme != "rediss") ||
			u.User != nil || u.Opaque != "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			strings.Contains(server, "#") || strings.ContainsFunc(server, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return errors.New("RedisStreams servers require redis:// or rediss:// URLs without embedded credentials, query or path")
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return errors.New("RedisStreams server port must be between 1 and 65535")
			}
		} else if strings.HasSuffix(u.Host, ":") {
			return errors.New("RedisStreams server port must not be empty")
		}
		tlsURL = tlsURL || u.Scheme == "rediss"
	}
	if !c.TLSEnabled && !tlsURL && (c.TLSCAFile != "" || c.TLSServerName != "" || c.TLSCertFile != "" || c.TLSKeyFile != "") {
		return errors.New("RedisStreams TLS settings require tlsEnabled or rediss:// servers")
	}
	const maxMillis = int64((1<<63 - 1) / int64(time.Millisecond))
	if c.ConnectTimeoutMs < 0 || c.OperationTimeoutMs < 0 || int64(c.ConnectTimeoutMs) > maxMillis || int64(c.OperationTimeoutMs) > maxMillis {
		return errors.New("RedisStreams timeouts must be positive representable millisecond durations")
	}
	if c.Username != "" && c.Password == "" {
		return errors.New("RedisStreams username requires password")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("RedisStreams TLS certificate and key must both be configured")
	}
	if c.RedisDatabase < 0 {
		return errors.New("RedisStreams redisDatabase must be nonnegative")
	}
	return nil
}

type RedisStreamsEndpointConfig struct {
	ID                       int                    `yaml:"id,omitempty" mapstructure:"id"`
	Name                     string                 `yaml:"name,omitempty" mapstructure:"name"`
	IdDataConnector          int                    `yaml:"idDataConnector,omitempty" mapstructure:"idDataConnector"`
	Enabled                  bool                   `yaml:"enabled,omitempty" mapstructure:"enabled"`
	TracingEnabled           bool                   `yaml:"tracingEnabled,omitempty" mapstructure:"tracingEnabled"`
	StreamKey                string                 `yaml:"streamKey,omitempty" mapstructure:"streamKey"`
	ConsumerGroup            string                 `yaml:"consumerGroup,omitempty" mapstructure:"consumerGroup"`
	ConsumerName             string                 `yaml:"consumerName,omitempty" mapstructure:"consumerName"`
	CreateConsumerGroup      bool                   `yaml:"createConsumerGroup,omitempty" mapstructure:"createConsumerGroup"`
	GroupStartId             string                 `yaml:"groupStartId,omitempty" mapstructure:"groupStartId"`
	ReadCount                int                    `yaml:"readCount,omitempty" mapstructure:"readCount"`
	ReadBlockMs              int                    `yaml:"readBlockMs,omitempty" mapstructure:"readBlockMs"`
	ClaimIdleMs              int                    `yaml:"claimIdleMs,omitempty" mapstructure:"claimIdleMs"`
	ClaimIntervalMs          int                    `yaml:"claimIntervalMs,omitempty" mapstructure:"claimIntervalMs"`
	CreateStream             bool                   `yaml:"createStream,omitempty" mapstructure:"createStream"`
	PublisherMaxInFlight     int                    `yaml:"publisherMaxInFlight,omitempty" mapstructure:"publisherMaxInFlight"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (e *RedisStreamsEndpointConfig) GetID() int                          { return e.ID }
func (e *RedisStreamsEndpointConfig) GetName() string                     { return e.Name }
func (e *RedisStreamsEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *RedisStreamsEndpointConfig) GetTracingEnabled() bool             { return e.TracingEnabled }
func (e *RedisStreamsEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }

// Zero values select documented defaults; explicit YAML bounds are also validated by the model.
func (e *RedisStreamsEndpointConfig) MaxInFlight() int {
	if e.PublisherMaxInFlight == 0 {
		return 256
	}
	return e.PublisherMaxInFlight
}
func (e *RedisStreamsEndpointConfig) Validate(source bool) error {
	if e == nil {
		return errors.New("RedisStreams endpoint configuration is missing")
	}
	if e.StreamKey == "" {
		return errors.New("RedisStreams streamKey is required")
	}
	for _, value := range []string{e.StreamKey, e.ConsumerGroup, e.ConsumerName} {
		if strings.ContainsFunc(value, unicode.IsControl) {
			return errors.New("RedisStreams names cannot contain control characters")
		}
	}
	if e.ReadCount < 0 || e.ReadCount > 4096 || e.ReadBlockMs < 0 || e.ReadBlockMs > 60000 || e.ClaimIdleMs < 0 || e.ClaimIdleMs > 2147483647 || e.ClaimIntervalMs < 0 || e.ClaimIntervalMs > 2147483647 || e.PublisherMaxInFlight < 0 || e.PublisherMaxInFlight > 4096 {
		return errors.New("RedisStreams read, reclaim or publisher limit is outside supported bounds")
	}
	if source {
		if e.ConsumerGroup == "" {
			return errors.New("RedisStreams source requires consumerGroup")
		}
		if e.CreateStream || e.PublisherMaxInFlight != 0 {
			return errors.New("RedisStreams source cannot set sink options")
		}
		if e.GroupStartId != "" && (!e.CreateConsumerGroup || (e.GroupStartId != "0-0" && e.GroupStartId != "$")) {
			return errors.New("RedisStreams groupStartId requires explicit group creation and must be 0-0 or $")
		}
	} else if e.ConsumerGroup != "" || e.ConsumerName != "" || e.CreateConsumerGroup || e.GroupStartId != "" || e.ReadCount != 0 || e.ReadBlockMs != 0 || e.ClaimIdleMs != 0 || e.ClaimIntervalMs != 0 {
		return errors.New("RedisStreams sink cannot set source options")
	}
	return nil
}
