package config

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"testing"
)

func TestRedisStreamsConfigProjection(t *testing.T) {
	var c RedisStreamsDataConnectorConfig
	require.NoError(t, yaml.Unmarshal([]byte("id: 10\nservers: [rediss://localhost:6379]\nredisDatabase: 3\nimplementation: go-redis\nextra: retained\n"), &c))
	require.NoError(t, c.Validate())
	require.Equal(t, "retained", c.GetProperty("extra"))
	a := DataConnectorConfigToAPI(&c)
	require.Equal(t, api.DataConnectorTypeRedisStreams, a.Type)
	require.Equal(t, 3, *a.RedisDatabase)
	(*a.Servers)[0] = "changed"
	require.Equal(t, "rediss://localhost:6379", c.Servers[0])
	e := RedisStreamsEndpointConfig{StreamKey: "events", ConsumerGroup: "workers", ConsumerName: "worker-1", CreateConsumerGroup: true, GroupStartId: "$", ReadCount: 64, ReadBlockMs: 1000, ClaimIdleMs: 60000, ClaimIntervalMs: 1000, FunctionName: "Receive"}
	require.NoError(t, e.Validate(true))
	out := EndpointConfigToAPI(&e)
	require.Equal(t, "events", *out.StreamKey)
	require.Equal(t, "workers", *out.ConsumerGroup)
	require.Equal(t, "worker-1", *out.ConsumerName)
	require.Equal(t, "$", *out.GroupStartId)
	require.True(t, *out.CreateConsumerGroup)
	require.Equal(t, 60000, *out.ClaimIdleMs)
	require.Equal(t, 1000, *out.ClaimIntervalMs)
	require.Equal(t, 64, *out.ReadCount)
	require.Equal(t, 1000, *out.ReadBlockMs)
	sink := RedisStreamsEndpointConfig{StreamKey: "output", CreateStream: true, PublisherMaxInFlight: 17}
	require.NoError(t, sink.Validate(false))
	require.True(t, *EndpointConfigToAPI(&sink).CreateStream)
	require.Equal(t, 17, *EndpointConfigToAPI(&sink).PublisherMaxInFlight)
	require.Error(t, e.Validate(false))
	require.Error(t, sink.Validate(true))
}

func TestRedisStreamsConfigRejectsUnsupportedConnections(t *testing.T) {
	for _, servers := range [][]string{nil, {"redis://a", "redis://b"}, {"redis://user:secret@a"}, {"redis://a/1"}, {"redis://a?x=y"}, {"redis://a#"}, {"redis://a:0"}, {"redis://a:"}, {"http://a"}} {
		c := RedisStreamsDataConnectorConfig{Servers: servers}
		err := c.Validate()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	c := RedisStreamsDataConnectorConfig{Servers: []string{"redis://localhost"}, Password: "secret"}
	require.NoError(t, c.Validate()) // Default Redis user supports password-only AUTH.
	c.RedisDatabase = -1
	require.Error(t, c.Validate())
	c.RedisDatabase = 0
	c.TLSCertFile = "cert"
	require.Error(t, c.Validate())
}

func TestRedisStreamsEndpointBoundsAndCreation(t *testing.T) {
	for _, e := range []RedisStreamsEndpointConfig{
		{}, {StreamKey: "x", ConsumerGroup: "g", ReadCount: 4097},
		{StreamKey: "x", ConsumerGroup: "g", ReadBlockMs: 60001},
		{StreamKey: "x", ConsumerGroup: "g", ClaimIdleMs: -1},
		{StreamKey: "x", ConsumerGroup: "g", ClaimIntervalMs: 2147483648},
		{StreamKey: "x", ConsumerGroup: "g", GroupStartId: "$"},
		{StreamKey: "x", ConsumerGroup: "g", GroupStartId: "123-0", CreateConsumerGroup: true},
		{StreamKey: "x\n", ConsumerGroup: "g"},
	} {
		require.Error(t, e.Validate(true))
	}
	e := RedisStreamsEndpointConfig{StreamKey: "x"}
	require.NoError(t, e.Validate(false))
	require.False(t, e.CreateStream)
	require.Equal(t, 256, e.MaxInFlight())
	e.PublisherMaxInFlight = 4097
	require.Error(t, e.Validate(false))
}
