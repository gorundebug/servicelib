package config

import (
	"testing"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestNATSConnectionValidation(t *testing.T) {
	valid := func() *NATSDataConnectorConfig {
		return &NATSDataConnectorConfig{Type: api.DataConnectorTypeNATS, Servers: []string{"nats://localhost:4222"}}
	}
	for _, test := range []struct {
		name   string
		change func(*NATSDataConnectorConfig)
	}{
		{"zero port", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost:0"} }},
		{"large port", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost:65536"} }},
		{"empty port", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost:"} }},
		{"url path", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost/"} }},
		{"url query", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost?"} }},
		{"url fragment", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost#"} }},
		{"mixed security", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://localhost", "tls://localhost"} }},
		{"implicit TLS", func(c *NATSDataConnectorConfig) { c.TLSCAFile = "/run/tls/ca.pem" }},
		{"overflow timeout", func(c *NATSDataConnectorConfig) { c.OperationTimeoutMs = 9223372036855 }},
		{"missing server", func(c *NATSDataConnectorConfig) { c.Servers = nil }},
		{"credentials in URL", func(c *NATSDataConnectorConfig) { c.Servers = []string{"nats://user:do-not-log@localhost:4222"} }},
		{"unknown scheme", func(c *NATSDataConnectorConfig) { c.Servers = []string{"https://localhost:4222"} }},
		{"mixed authentication", func(c *NATSDataConnectorConfig) {
			c.Username = "user"
			c.Password = "do-not-log"
			c.Token = "do-not-log"
		}},
		{"unpaired credentials", func(c *NATSDataConnectorConfig) { c.Username = "user" }},
		{"unpaired TLS key", func(c *NATSDataConnectorConfig) { c.TLSKeyFile = "do-not-log" }},
		{"negative timeout", func(c *NATSDataConnectorConfig) { c.ConnectTimeoutMs = -1 }},
		{"foreign kind", func(c *NATSDataConnectorConfig) { c.Type = api.DataConnectorTypeKafka }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := valid()
			test.change(c)
			err := c.Validate()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "do-not-log")
		})
	}
	c := valid()
	require.NoError(t, c.Validate())
	require.Equal(t, 2*time.Second, c.ConnectTimeout())
	require.Equal(t, 5*time.Second, c.OperationTimeout())
	c.Type = api.DataConnectorTypeJetStream
	c.Servers = []string{"tls://localhost:4222"}
	c.Token = "secret"
	c.ConnectTimeoutMs = 350
	c.OperationTimeoutMs = 750
	require.NoError(t, c.Validate())
	require.Equal(t, 350*time.Millisecond, c.ConnectTimeout())
	require.Equal(t, 750*time.Millisecond, c.OperationTimeout())
}

func TestNATSConfigPreservesNativeFieldsAndCreationOptIn(t *testing.T) {
	var c NATSDataConnectorConfig
	require.NoError(t, yaml.Unmarshal([]byte("id: 7\nname: events\ntype: 8\nservers: [nats://localhost:4222]\noperationTimeoutMs: 1250\napplicationOption: keep\n"), &c))
	require.NoError(t, c.Validate())
	require.Equal(t, "keep", c.GetProperty("applicationOption"))
	a := DataConnectorConfigToAPI(&c)
	require.Equal(t, api.DataConnectorTypeJetStream, a.Type)
	require.Equal(t, c.Servers, *a.Servers)
	require.Equal(t, 1250, *a.OperationTimeoutMs)
	(*a.Servers)[0] = "changed"
	require.Equal(t, "nats://localhost:4222", c.Servers[0])
	var e NATSEndpointConfig
	require.NoError(t, yaml.Unmarshal([]byte("id: 3\nidDataConnector: 7\nenabled: true\nsubject: orders.*\nstreamName: orders\ndurableName: worker\nfunctionName: Read\n"), &e))
	require.False(t, e.CreateStream)
	require.False(t, e.CreateConsumer)
	out := EndpointConfigToAPI(&e)
	require.Equal(t, "orders.*", *out.Subject)
	require.Equal(t, "orders", *out.StreamName)
	require.Equal(t, "worker", *out.DurableName)
	require.Equal(t, "Read", out.FunctionName)
	require.True(t, *out.Enabled)
	e.CreateStream = true
	e.CreateConsumer = true
	out = EndpointConfigToAPI(&e)
	require.True(t, *out.CreateStream)
	require.True(t, *out.CreateConsumer)
}
