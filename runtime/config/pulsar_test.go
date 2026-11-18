package config

import (
	"fmt"
	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPulsarConfigProjectionAndDefaults(t *testing.T) {
	c := &PulsarDataConnectorConfig{Servers: []string{"pulsar://broker:6650"}, AdminURL: "http://broker:8080", Token: "test-token"}
	require.NoError(t, c.Validate())
	require.Equal(t, 2*time.Second, c.ConnectTimeout())
	require.Equal(t, 5*time.Second, c.OperationTimeout())
	a := DataConnectorConfigToAPI(c)
	require.Equal(t, api.DataConnectorTypePulsar, a.Type)
	require.Equal(t, c.AdminURL, *a.AdminUrl)
	require.Equal(t, c.Token, *a.Token)
	(*a.Servers)[0] = "changed"
	require.Equal(t, "pulsar://broker:6650", c.Servers[0])
	e := &PulsarEndpointConfig{Topic: "persistent://tenant/ns/events", SubscriptionName: "workers"}
	require.NoError(t, e.Validate(true))
	require.Equal(t, api.Shared, e.SubscriptionMode())
	require.Equal(t, api.Earliest, e.InitialPosition())
	require.Equal(t, 256, e.QueueSize())
	require.Equal(t, time.Minute, e.RedeliveryDelay())
	require.False(t, e.CreateTopic)
	require.False(t, e.CreateSubscription)
	e.CreateSubscription = true
	e.SubscriptionInitialPosition = api.Latest
	e.SubscriptionType = api.Failover
	e.ReceiverQueueSize = 32
	out := EndpointConfigToAPI(e)
	require.Equal(t, "workers", *out.SubscriptionName)
	require.True(t, *out.CreateSubscription)
	require.Equal(t, api.Latest, *out.SubscriptionInitialPosition)
	require.Equal(t, api.Failover, *out.SubscriptionType)
	require.Equal(t, 32, *out.ReceiverQueueSize)
	require.Error(t, e.Validate(false))
	sink := &PulsarEndpointConfig{Topic: e.Topic, CreateTopic: true, Partitions: 3, PublisherMaxInFlight: 17}
	require.NoError(t, sink.Validate(false))
	require.Equal(t, 17, sink.MaxInFlight())
	require.Equal(t, 3, *EndpointConfigToAPI(sink).Partitions)
	require.Error(t, sink.Validate(true))
}
func TestPulsarConfigRejectsUnsupportedConnections(t *testing.T) {
	for _, address := range []string{"", "http://broker", "pulsar://user:secret@broker", "pulsar://broker/path", "pulsar://broker?secret=1", "pulsar://broker#", "pulsar://broker:0", "pulsar://broker:", "pulsar://broker:65536"} {
		c := &PulsarDataConnectorConfig{Servers: []string{address}, AdminURL: "http://admin"}
		err := c.Validate()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	c := &PulsarDataConnectorConfig{Servers: []string{"pulsar+ssl://broker"}, AdminURL: "https://admin", TLSCAFile: "ca", TLSCertFile: "cert", TLSKeyFile: "key"}
	require.NoError(t, c.Validate())
	c.Token = "secret"
	require.Error(t, c.Validate())
	c.Token = ""
	c.AdminURL = "http://admin"
	require.Error(t, c.Validate())
	c.AdminURL = "https://admin"
	c.OperationTimeoutMs = 2147483648
	require.Error(t, c.Validate())
	c.OperationTimeoutMs = 0
	c.TLSKeyFile = ""
	require.Error(t, c.Validate())
}
func TestPulsarEndpointValidation(t *testing.T) {
	for _, topic := range []string{"", "events", "non-persistent://a/b/c", "persistent://a/b", "persistent://a/b/c/d", "persistent://a//c", "persistent://a/b/c?x=y", "persistent://a/b/%2f", "persistent://a/b/c\n"} {
		require.Error(t, (&PulsarEndpointConfig{Topic: topic}).Validate(false))
	}
	for _, e := range []PulsarEndpointConfig{
		{Topic: "persistent://a/b/c", SubscriptionName: "g", SubscriptionType: "KeyShared"},
		{Topic: "persistent://a/b/c", SubscriptionName: "g", SubscriptionInitialPosition: api.Latest},
		{Topic: "persistent://a/b/c", SubscriptionName: "g", ReceiverQueueSize: 4097},
		{Topic: "persistent://a/b/c", SubscriptionName: "g", NegativeAckRedeliveryDelayMs: -1},
		{Topic: "persistent://a/b/c", SubscriptionName: "g", Partitions: 1},
	} {
		require.Error(t, e.Validate(true))
	}
}

func TestPulsarAdminTokenRuntimeOnly(t *testing.T) {
	c := &PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: "http://broker", Token: "native-token", AdminToken: "admin-secret"}
	require.NoError(t, c.Validate())
	require.NotContains(t, fmt.Sprintf("%+v", DataConnectorConfigToAPI(c)), "admin-secret")
	c.AdminToken = "secret\r\nheader"
	require.Error(t, c.Validate())
	c.AdminToken = "admin-secret"
	c.Token = ""
	c.Servers = []string{"pulsar+ssl://broker"}
	c.AdminURL = "https://broker"
	c.TLSCertFile = "cert"
	c.TLSKeyFile = "key"
	require.Error(t, c.Validate())
}
