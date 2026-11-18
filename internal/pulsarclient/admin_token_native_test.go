package pulsarclient

import (
	"context"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestNativePulsarAdminTokenSeparation(t *testing.T) {
	path := os.Getenv("PULSAR_AUTH_MANIFEST")
	if path == "" {
		t.Skip("set PULSAR_AUTH_MANIFEST from isolated fixture")
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var f struct {
		URL      string            `json:"url"`
		AdminURL string            `json:"adminUrl"`
		Tokens   map[string]string `json:"tokens"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	cfg := func(role, admin string) *config.PulsarDataConnectorConfig {
		return &config.PulsarDataConnectorConfig{Servers: []string{f.URL}, AdminURL: f.AdminURL, Token: f.Tokens[role], AdminToken: f.Tokens[admin], ConnectTimeoutMs: 1000, OperationTimeoutMs: 2000}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	ep := &config.PulsarEndpointConfig{Topic: "persistent://acl/events/allowed", SubscriptionName: "workers"}
	consumer, err := Open(ctx, cfg("consumer", "tenant-admin"), ep, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, consumer.Close(ctx)) })
	producer, err := Open(ctx, cfg("producer", "tenant-admin"), &config.PulsarEndpointConfig{Topic: ep.Topic}, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, producer.Close(ctx)) })
	id, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte("go-separated")})
	require.NoError(t, err)
	message, err := consumer.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, id.Serialize(), message.ID().Serialize())
	require.NoError(t, consumer.Ack(ctx, message))
	for _, test := range []struct {
		role, admin, topic string
		source             bool
	}{{"producer", "tenant-admin", ep.Topic, true}, {"consumer", "tenant-admin", ep.Topic, false}, {"topic-producer", "tenant-admin", "persistent://acl/events/forbidden", false}, {"producer", "tenant-admin", "persistent://acl/denied/allowed", false}, {"admin", "outsider", ep.Topic, false}, {"producer", "", ep.Topic, false}} {
		_, err := Open(ctx, cfg(test.role, test.admin), &config.PulsarEndpointConfig{Topic: test.topic, SubscriptionName: func() string {
			if test.source {
				return "workers"
			}
			return ""
		}()}, test.source)
		require.Error(t, err)
		for _, token := range f.Tokens {
			require.NotContains(t, err.Error(), token)
		}
	}
	bad := cfg("admin", "tenant-admin")
	bad.AdminToken = "invalid-admin-token"
	_, err = Open(ctx, bad, &config.PulsarEndpointConfig{Topic: ep.Topic}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "401")
	started := time.Now()
	require.NoError(t, producer.Close(ctx))
	require.NoError(t, consumer.Close(ctx))
	require.Less(t, time.Since(started), 3*time.Second)
}
