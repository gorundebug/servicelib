package config

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"testing"
	"time"
)

func TestRabbitMQConnectionValidation(t *testing.T) {
	valid := func() *RabbitMQDataConnectorConfig {
		return &RabbitMQDataConnectorConfig{Servers: []string{"amqp://localhost:5672"}}
	}
	for _, u := range []string{"amqp://localhost/", "amqp://localhost?", "amqp://localhost#", "amqp://user:secret@localhost", "amqp://localhost:0", "amqp://localhost:65536", "amqp://localhost:", "http://localhost", "amqp://local host"} {
		t.Run(u, func(t *testing.T) {
			c := valid()
			c.Servers = []string{u}
			e := c.Validate()
			require.Error(t, e)
			require.NotContains(t, e.Error(), "secret")
		})
	}
	for _, modify := range []func(*RabbitMQDataConnectorConfig){
		func(c *RabbitMQDataConnectorConfig) { c.Servers = nil },
		func(c *RabbitMQDataConnectorConfig) { c.Servers = append(c.Servers, "amqps://localhost") },
		func(c *RabbitMQDataConnectorConfig) { c.Username = "secret" },
		func(c *RabbitMQDataConnectorConfig) { c.TLSCAFile = "secret" },
		func(c *RabbitMQDataConnectorConfig) { c.TLSEnabled = true; c.TLSKeyFile = "secret" },
		func(c *RabbitMQDataConnectorConfig) { c.ConnectTimeoutMs = -1 },
		func(c *RabbitMQDataConnectorConfig) { c.OperationTimeoutMs = 9223372036855 },
		func(c *RabbitMQDataConnectorConfig) { c.Implementation = "amqp091-go" },
		func(c *RabbitMQDataConnectorConfig) { c.VirtualHost = "bad\n" },
	} {
		c := valid()
		modify(c)
		e := c.Validate()
		require.Error(t, e)
		require.NotContains(t, e.Error(), "secret")
	}
	c := valid()
	require.NoError(t, c.Validate())
	require.Equal(t, 2*time.Second, c.ConnectTimeout())
	require.Equal(t, 5*time.Second, c.OperationTimeout())
	c.Servers = []string{"amqps://localhost"}
	c.Username = "user"
	c.Password = "secret"
	c.OperationTimeoutMs = 750
	require.NoError(t, c.Validate())
	require.Equal(t, 750*time.Millisecond, c.OperationTimeout())
}
func TestRabbitMQEndpointValidation(t *testing.T) {
	for _, c := range []RabbitMQEndpointConfig{
		{}, {QueueName: "q", ConsumerCredits: 4097}, {QueueName: "q", ConsumerCredits: -1}, {QueueName: "q", PublisherMaxInFlight: 1}, {QueueName: "q", QueueType: "stream"}, {QueueName: "q", CreateBinding: true}, {QueueName: "q", CreateExchange: true}, {QueueName: "q", ExchangeName: "e", ExchangeType: api.Direct}, {QueueName: "q\n"},
	} {
		require.Error(t, c.Validate(true))
	}
	e := RabbitMQEndpointConfig{QueueName: "q"}
	require.NoError(t, e.Validate(true))
	require.NoError(t, e.Validate(false))
	require.Equal(t, 256, e.Credits())
	require.Equal(t, 256, e.MaxInFlight())
	require.False(t, e.CreateQueue)
	require.False(t, e.CreateExchange)
	require.False(t, e.CreateBinding)
	e.ExchangeName = "e"
	e.CreateExchange = true
	e.CreateBinding = true
	e.ExchangeType = api.Topic
	require.NoError(t, e.Validate(true))
	e.ConsumerCredits = 1
	require.Error(t, e.Validate(false))
}
func TestRabbitMQProjectionAndOptIn(t *testing.T) {
	var c RabbitMQDataConnectorConfig
	require.NoError(t, yaml.Unmarshal([]byte("id: 9\nservers: [amqp://localhost]\nvirtualHost: tenant\nimplementation: rabbitmq-amqp-go-client\nextra: retained\n"), &c))
	require.NoError(t, c.Validate())
	require.Equal(t, "retained", c.GetProperty("extra"))
	a := DataConnectorConfigToAPI(&c)
	require.Equal(t, api.DataConnectorTypeRabbitMQ, a.Type)
	require.Equal(t, "tenant", *a.VirtualHost)
	(*a.Servers)[0] = "changed"
	require.Equal(t, "amqp://localhost", c.Servers[0])
	e := RabbitMQEndpointConfig{QueueName: "q", ExchangeName: "e", RoutingKey: "r", QueueType: api.Quorum, ExchangeType: api.Topic, CreateQueue: true, CreateExchange: true, CreateBinding: true, ConsumerCredits: 17, FunctionName: "Read"}
	out := EndpointConfigToAPI(&e)
	require.Equal(t, "q", *out.QueueName)
	require.Equal(t, "r", *out.RoutingKey)
	require.True(t, *out.CreateQueue)
	require.True(t, *out.CreateExchange)
	require.True(t, *out.CreateBinding)
	require.Equal(t, 17, *out.ConsumerCredits)
	require.Equal(t, "Read", out.FunctionName)
	*out.QueueType = api.Classic
	require.Equal(t, api.Quorum, e.QueueType)
}

func TestRabbitMQRejectsForeignAuthenticationProperties(t *testing.T) {
	for _, key := range []string{"token", "credentialsFile"} {
		c := &RabbitMQDataConnectorConfig{Servers: []string{"amqp://localhost"}, Properties: map[string]interface{}{key: "secret"}}
		err := c.Validate()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
}
