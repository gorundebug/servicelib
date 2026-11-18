package config

import (
	"errors"
	"github.com/gorundebug/servicelib/api"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type PulsarDataConnectorConfig struct {
	ID                  int                             `yaml:"id,omitempty" mapstructure:"id"`
	Name                string                          `yaml:"name,omitempty" mapstructure:"name"`
	Implementation      api.DataConnectorImplementation `yaml:"implementation,omitempty" mapstructure:"implementation"`
	ProgrammingLanguage api.ProgrammingLanguage         `yaml:"programmingLanguage,omitempty" mapstructure:"programmingLanguage"`
	Servers             []string                        `yaml:"servers,omitempty" mapstructure:"servers"`
	AdminURL            string                          `yaml:"adminUrl,omitempty" mapstructure:"adminUrl"`
	ConnectTimeoutMs    int                             `yaml:"connectTimeoutMs,omitempty" mapstructure:"connectTimeoutMs"`
	OperationTimeoutMs  int                             `yaml:"operationTimeoutMs,omitempty" mapstructure:"operationTimeoutMs"`
	AdminToken          string                          `yaml:"adminToken,omitempty" mapstructure:"adminToken"`
	Token               string                          `yaml:"token,omitempty" mapstructure:"token"`
	TLSCAFile           string                          `yaml:"tlsCaFile,omitempty" mapstructure:"tlsCaFile"`
	TLSCertFile         string                          `yaml:"tlsCertFile,omitempty" mapstructure:"tlsCertFile"`
	TLSKeyFile          string                          `yaml:"tlsKeyFile,omitempty" mapstructure:"tlsKeyFile"`
	Properties          map[string]interface{}          `yaml:",inline" mapstructure:",remain"`
}

func (c *PulsarDataConnectorConfig) GetID() int      { return c.ID }
func (c *PulsarDataConnectorConfig) GetName() string { return c.Name }
func (c *PulsarDataConnectorConfig) GetType() api.DataConnectorType {
	return api.DataConnectorTypePulsar
}
func (c *PulsarDataConnectorConfig) GetImplementation() api.DataConnectorImplementation {
	return c.Implementation
}
func (c *PulsarDataConnectorConfig) GetProperty(name string) interface{} { return c.Properties[name] }
func (c *PulsarDataConnectorConfig) ConnectTimeout() time.Duration {
	if c.ConnectTimeoutMs == 0 {
		return 2 * time.Second
	}
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}
func (c *PulsarDataConnectorConfig) OperationTimeout() time.Duration {
	if c.OperationTimeoutMs == 0 {
		return 5 * time.Second
	}
	return time.Duration(c.OperationTimeoutMs) * time.Millisecond
}

func pulsarURL(value string, schemes ...string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(value, "#") || strings.ContainsFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return nil, errors.New("Pulsar URL must have only a scheme, host and optional port")
	}
	valid := false
	for _, scheme := range schemes {
		valid = valid || u.Scheme == scheme
	}
	if !valid {
		return nil, errors.New("unsupported Pulsar URL scheme")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, errors.New("invalid Pulsar port")
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("empty Pulsar port")
	}
	return u, nil
}
func (c *PulsarDataConnectorConfig) Validate() error {
	if c == nil {
		return errors.New("Pulsar connector configuration is missing")
	}
	if c.Implementation != "" && c.Implementation != api.DataConnectorImplementationPulsarGo {
		return errors.New("Pulsar Go requires official pulsar-client-go")
	}
	if len(c.Servers) != 1 {
		return errors.New("Pulsar requires one service URL")
	}
	service, err := pulsarURL(c.Servers[0], "pulsar", "pulsar+ssl")
	if err != nil {
		return err
	}
	admin, err := pulsarURL(c.AdminURL, "http", "https")
	if err != nil {
		return errors.New("Pulsar requires a valid adminUrl for policy and resource verification")
	}
	if service.Scheme == "pulsar+ssl" && admin.Scheme != "https" {
		return errors.New("Pulsar TLS transport requires HTTPS administration")
	}
	if c.TLSCAFile != "" || c.TLSCertFile != "" || c.TLSKeyFile != "" {
		if service.Scheme != "pulsar+ssl" || admin.Scheme != "https" {
			return errors.New("Pulsar TLS identity and CA require TLS service and admin URLs")
		}
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("Pulsar TLS identity requires both certificate and key")
	}
	if strings.ContainsAny(c.AdminToken, "\r\n") {
		return errors.New("Pulsar adminToken must not contain header control characters")
	}
	if (c.Token != "" || c.AdminToken != "") && c.TLSCertFile != "" {
		return errors.New("select Pulsar token or TLS identity authentication")
	}
	for _, k := range []string{"username", "password", "credentialsFile", "tlsServerName", "tlsEnabled"} {
		if _, ok := c.Properties[k]; ok {
			return errors.New("unsupported Pulsar authentication or TLS option: " + k)
		}
	}
	if c.ConnectTimeoutMs < 0 || c.OperationTimeoutMs < 0 || c.ConnectTimeoutMs > 2147483647 || c.OperationTimeoutMs > 2147483647 {
		return errors.New("Pulsar timeout milliseconds must fit the shared client range")
	}
	return nil
}

type PulsarEndpointConfig struct {
	ID                           int                                     `yaml:"id,omitempty" mapstructure:"id"`
	Name                         string                                  `yaml:"name,omitempty" mapstructure:"name"`
	IdDataConnector              int                                     `yaml:"idDataConnector,omitempty" mapstructure:"idDataConnector"`
	Enabled                      bool                                    `yaml:"enabled,omitempty" mapstructure:"enabled"`
	TracingEnabled               bool                                    `yaml:"tracingEnabled,omitempty" mapstructure:"tracingEnabled"`
	Topic                        string                                  `yaml:"topic,omitempty" mapstructure:"topic"`
	CreateTopic                  bool                                    `yaml:"createTopic,omitempty" mapstructure:"createTopic"`
	Partitions                   int                                     `yaml:"partitions,omitempty" mapstructure:"partitions"`
	SubscriptionName             string                                  `yaml:"subscriptionName,omitempty" mapstructure:"subscriptionName"`
	SubscriptionType             api.EndpointSubscriptionType            `yaml:"subscriptionType,omitempty" mapstructure:"subscriptionType"`
	CreateSubscription           bool                                    `yaml:"createSubscription,omitempty" mapstructure:"createSubscription"`
	SubscriptionInitialPosition  api.EndpointSubscriptionInitialPosition `yaml:"subscriptionInitialPosition,omitempty" mapstructure:"subscriptionInitialPosition"`
	ReceiverQueueSize            int                                     `yaml:"receiverQueueSize,omitempty" mapstructure:"receiverQueueSize"`
	NegativeAckRedeliveryDelayMs int                                     `yaml:"negativeAckRedeliveryDelayMs,omitempty" mapstructure:"negativeAckRedeliveryDelayMs"`
	PublisherMaxInFlight         int                                     `yaml:"publisherMaxInFlight,omitempty" mapstructure:"publisherMaxInFlight"`
	FunctionName                 string                                  `yaml:"functionName,omitempty" mapstructure:"functionName"`
	FunctionPackage              string                                  `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionModule               string                                  `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	FunctionDescription          string                                  `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup     string                                  `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	PublicFunction               bool                                    `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	Properties                   map[string]interface{}                  `yaml:",inline" mapstructure:",remain"`
}

func (e *PulsarEndpointConfig) GetID() int                          { return e.ID }
func (e *PulsarEndpointConfig) GetName() string                     { return e.Name }
func (e *PulsarEndpointConfig) GetIdDataConnector() int             { return e.IdDataConnector }
func (e *PulsarEndpointConfig) GetTracingEnabled() bool             { return e.TracingEnabled }
func (e *PulsarEndpointConfig) GetProperty(name string) interface{} { return e.Properties[name] }
func (e *PulsarEndpointConfig) MaxInFlight() int {
	if e.PublisherMaxInFlight == 0 {
		return 256
	}
	return e.PublisherMaxInFlight
}
func (e *PulsarEndpointConfig) QueueSize() int {
	if e.ReceiverQueueSize == 0 {
		return 256
	}
	return e.ReceiverQueueSize
}
func (e *PulsarEndpointConfig) SubscriptionMode() api.EndpointSubscriptionType {
	if e.SubscriptionType == "" {
		return api.Shared
	}
	return e.SubscriptionType
}
func (e *PulsarEndpointConfig) InitialPosition() api.EndpointSubscriptionInitialPosition {
	if e.SubscriptionInitialPosition == "" {
		return api.Earliest
	}
	return e.SubscriptionInitialPosition
}
func (e *PulsarEndpointConfig) RedeliveryDelay() time.Duration {
	if e.NegativeAckRedeliveryDelayMs == 0 {
		return time.Minute
	}
	return time.Duration(e.NegativeAckRedeliveryDelayMs) * time.Millisecond
}
func (e *PulsarEndpointConfig) Validate(source bool) error {
	if e == nil {
		return errors.New("Pulsar endpoint configuration is missing")
	}
	name, ok := strings.CutPrefix(e.Topic, "persistent://")
	parts := strings.Split(name, "/")
	if !ok || len(parts) != 3 {
		return errors.New("Pulsar topic requires persistent://tenant/namespace/topic")
	}
	for _, part := range parts {
		if part == "" || strings.ContainsAny(part, "%?#\\") || strings.ContainsFunc(part, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return errors.New("invalid Pulsar topic component")
		}
	}
	if e.Partitions < 0 || e.Partitions > 1000 || (e.Partitions != 0 && !e.CreateTopic) {
		return errors.New("Pulsar partitions require explicit topic creation and range 1..1000")
	}
	if e.ReceiverQueueSize < 0 || e.ReceiverQueueSize > 4096 || e.PublisherMaxInFlight < 0 || e.PublisherMaxInFlight > 4096 || e.NegativeAckRedeliveryDelayMs < 0 || e.NegativeAckRedeliveryDelayMs > 2147483647 {
		return errors.New("Pulsar queue or redelivery bound is invalid")
	}
	if source {
		if e.SubscriptionName == "" || strings.ContainsAny(e.SubscriptionName, "/%?#\\") || strings.ContainsFunc(e.SubscriptionName, unicode.IsControl) {
			return errors.New("Pulsar source requires a valid subscriptionName")
		}
		switch e.SubscriptionMode() {
		case api.Shared, api.Exclusive, api.Failover:
		default:
			return errors.New("unsupported Pulsar subscription type")
		}
		if e.SubscriptionInitialPosition != "" && (!e.CreateSubscription || (e.SubscriptionInitialPosition != api.Earliest && e.SubscriptionInitialPosition != api.Latest)) {
			return errors.New("Pulsar initial position requires explicit subscription creation and Earliest or Latest")
		}
		if e.PublisherMaxInFlight != 0 {
			return errors.New("Pulsar source cannot set publisher options")
		}
	} else if e.SubscriptionName != "" || e.SubscriptionType != "" || e.CreateSubscription || e.SubscriptionInitialPosition != "" || e.ReceiverQueueSize != 0 || e.NegativeAckRedeliveryDelayMs != 0 {
		return errors.New("Pulsar sink cannot set subscription options")
	}
	return nil
}
