package pulsarclient

import (
	"context"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func nativeFixture(t *testing.T) (context.Context, *config.PulsarDataConnectorConfig, *config.PulsarEndpointConfig) {
	t.Helper()
	address := os.Getenv("PULSAR_TEST_ADMIN_URL")
	if address == "" {
		t.Skip("set PULSAR_TEST_ADMIN_URL")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 40*time.Second)
	t.Cleanup(cancel)
	c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://127.0.0.1:16650"}, AdminURL: address, OperationTimeoutMs: 2000}
	a, err := newAdmin(c)
	require.NoError(t, err)
	ns := "public/sa-native-" + xid.New().String()
	path := "/admin/v2/namespaces/" + ns
	require.NoError(t, a.request(ctx, http.MethodPut, path, map[string]interface{}{}, nil))
	t.Cleanup(func() {
		defer a.close()
		require.NoError(t, a.request(ctx, http.MethodDelete, "/admin/v2/persistent/"+ns+"/events?force=true", nil, nil))
		require.NoError(t, a.request(ctx, http.MethodDelete, path, nil, nil))
	})
	require.NoError(t, a.request(ctx, http.MethodPost, path+"/autoTopicCreation", map[string]interface{}{"allowAutoTopicCreation": false}, nil))
	require.NoError(t, a.request(ctx, http.MethodPost, path+"/autoSubscriptionCreation", map[string]interface{}{"allowAutoSubscriptionCreation": false}, nil))
	e := &config.PulsarEndpointConfig{Topic: "persistent://" + ns + "/events", SubscriptionName: "workers", CreateTopic: true, CreateSubscription: true, ReceiverQueueSize: 1, NegativeAckRedeliveryDelayMs: 50}
	return ctx, c, e
}
func TestNativePulsarMessaging(t *testing.T) {
	ctx, c, e := nativeFixture(t)
	first, err := Open(ctx, c, e, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close(ctx)) })
	writer, err := Open(ctx, c, &config.PulsarEndpointConfig{Topic: e.Topic}, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close(ctx)) })
	payload := []byte{0, 255, 254, 1}
	eventTime := time.Unix(1700000000, 0)
	id, err := writer.Send(ctx, &pulsar.ProducerMessage{Payload: payload, Key: "business-key", Properties: map[string]string{"traceparent": "test-trace"}, EventTime: eventTime})
	require.NoError(t, err)
	message, err := first.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, id.Serialize(), message.ID().Serialize())
	require.Equal(t, payload, message.Payload())
	require.Equal(t, "business-key", message.Key())
	require.Equal(t, "test-trace", message.Properties()["traceparent"])
	require.Equal(t, eventTime.UnixMilli(), message.EventTime().UnixMilli())
	// Closing a consumer with unfinished work preserves its durable subscription.
	require.NoError(t, first.Close(ctx))
	e.CreateTopic = false
	e.CreateSubscription = false
	second, err := Open(ctx, c, e, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close(ctx)) })
	replay, err := second.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, id.Serialize(), replay.ID().Serialize())
	require.NoError(t, second.Nack(ctx, replay))
	redelivery, err := second.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, id.Serialize(), redelivery.ID().Serialize())
	require.Greater(t, redelivery.RedeliveryCount(), uint32(0))
	require.NoError(t, second.Ack(ctx, redelivery))
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = second.Receive(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, first.Ack(ctx, message), ErrClosed)
}
func TestNativePulsarAsyncDrain(t *testing.T) {
	ctx, c, e := nativeFixture(t)
	require.NoError(t, EnsureResources(ctx, c, e, true))
	writer, err := Open(ctx, c, &config.PulsarEndpointConfig{Topic: e.Topic, PublisherMaxInFlight: 1}, false)
	require.NoError(t, err)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); require.NoError(t, writer.Close(ctx)) })
	var resultID pulsar.MessageID
	var resultErr error
	require.NoError(t, writer.SendAsync(ctx, &pulsar.ProducerMessage{Payload: []byte("async")}, func(id pulsar.MessageID, err error) {
		resultID, resultErr = id, err
		close(entered)
		<-release
		close(finished)
	}))
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, resultErr)
	require.NotNil(t, resultID)
	require.ErrorIs(t, writer.SendAsync(ctx, &pulsar.ProducerMessage{Payload: []byte("rejected")}, func(pulsar.MessageID, error) { t.Error("capacity rejection invoked native callback") }), ErrCapacity)
	deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, writer.Close(deadline), context.DeadlineExceeded)
	_, err = writer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte("after close")})
	require.ErrorIs(t, err, ErrClosed)
	once.Do(func() { close(release) })
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, writer.Close(ctx))
	reader, err := Open(ctx, c, e, true)
	require.NoError(t, err)
	defer reader.Close(ctx)
	msg, err := reader.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, "async", string(msg.Payload()))
	require.NoError(t, reader.Ack(ctx, msg))
	short, cancelRead := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelRead()
	_, err = reader.Receive(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
