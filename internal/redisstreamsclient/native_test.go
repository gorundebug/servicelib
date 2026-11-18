package redisstreamsclient

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestNativeStreamsLifecycle(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("set REDIS_STREAMS_TEST_URL for isolated native Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cfg := &config.RedisStreamsDataConnectorConfig{Servers: []string{address}, Password: os.Getenv("REDIS_STREAMS_TEST_PASSWORD"), OperationTimeoutMs: 1000}
	key := "sa-test-" + xid.New().String()
	options, err := Options(cfg)
	require.NoError(t, err)
	admin := redis.NewClient(options)
	defer admin.Close()
	defer func() { require.NoError(t, admin.Del(ctx, key).Err()) }()
	out := &config.RedisStreamsEndpointConfig{StreamKey: key}
	writer, err := Open(ctx, cfg, out, false)
	require.NoError(t, err)
	defer writer.Close()
	_, err = writer.Publish(ctx, map[string]interface{}{"payload": "not-created"})
	require.ErrorIs(t, err, redis.Nil)
	require.Zero(t, admin.Exists(ctx, key).Val())
	in := &config.RedisStreamsEndpointConfig{StreamKey: key, ConsumerGroup: "workers", ReadCount: 1, ReadBlockMs: 50, ClaimIdleMs: 1, CreateConsumerGroup: true}
	first, err := Open(ctx, cfg, in, true)
	require.NoError(t, err)
	defer first.Close()
	id, err := writer.Publish(ctx, map[string]interface{}{"payload": "hello"})
	require.NoError(t, err)
	messages, err := first.Receive(ctx)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, id, messages[0].ID)
	require.Equal(t, "hello", messages[0].Values["payload"])
	require.EqualValues(t, 1, admin.XPending(ctx, key, "workers").Val().Count)
	second, err := Open(ctx, cfg, in, true)
	require.NoError(t, err)
	defer second.Close()
	require.NotEqual(t, first.consumer, second.consumer)
	time.Sleep(5 * time.Millisecond)
	reclaimed, deleted, err := second.Claim(ctx)
	require.NoError(t, err)
	require.Empty(t, deleted)
	require.Len(t, reclaimed, 1)
	require.Equal(t, id, reclaimed[0].ID)
	require.NoError(t, second.Ack(ctx, id))
	require.Zero(t, admin.XPending(ctx, key, "workers").Val().Count)
	require.EqualValues(t, 1, admin.XLen(ctx, key).Val()) // ACK never deletes stream records.
	// A blocked reader must not hold up the command connection.
	readDone := make(chan error, 1)
	go func() { _, e := first.Receive(ctx); readDone <- e }()
	id, err = writer.Publish(ctx, map[string]interface{}{"payload": "next"})
	require.NoError(t, err)
	require.NoError(t, <-readDone)
	require.NoError(t, admin.XDel(ctx, key, id).Err())
	time.Sleep(5 * time.Millisecond)
	_, deleted, err = second.Claim(ctx)
	require.NoError(t, err)
	require.Contains(t, deleted, id)
	stopped, cancelRead := context.WithCancel(ctx)
	cancelRead()
	_, err = first.Receive(stopped)
	require.Error(t, err)
	// Explicit creation is still opt-in for a distinct sink key.
	newKey := key + "-out"
	defer admin.Del(ctx, newKey)
	out.StreamKey = newKey
	out.CreateStream = true
	creator, err := Open(ctx, cfg, out, false)
	require.NoError(t, err)
	defer creator.Close()
	createdID, err := creator.Publish(ctx, map[string]interface{}{"payload": []byte("bytes")})
	require.NoError(t, err)
	require.NotEmpty(t, createdID)
}

func TestNativeOrderedBinaryFields(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("set REDIS_STREAMS_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cfg := &config.RedisStreamsDataConnectorConfig{Servers: []string{address}, Password: os.Getenv("REDIS_STREAMS_TEST_PASSWORD"), OperationTimeoutMs: 1000}
	key := "sa-fields-" + xid.New().String()
	reader, err := Open(ctx, cfg, &config.RedisStreamsEndpointConfig{StreamKey: key, ConsumerGroup: "fields", CreateConsumerGroup: true, ClaimIdleMs: 1, ReadBlockMs: 50}, true)
	require.NoError(t, err)
	defer reader.Close()
	defer reader.commands.Del(ctx, key)
	writer, err := Open(ctx, cfg, &config.RedisStreamsEndpointConfig{StreamKey: key}, false)
	require.NoError(t, err)
	defer writer.Close()
	fields := []Field{{"same", "first"}, {"\x00\xff", "\x00\xff\xfe"}, {"same", "last"}, {"", ""}}
	id, err := writer.PublishFields(ctx, fields)
	require.NoError(t, err)
	entries, err := reader.Receive(ctx)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, id, entries[0].ID)
	require.Equal(t, fields, entries[0].Fields)
	require.Equal(t, "last", entries[0].Values["same"])
	time.Sleep(5 * time.Millisecond)
	reclaimed, deleted, err := reader.Claim(ctx)
	require.NoError(t, err)
	require.Empty(t, deleted)
	require.Len(t, reclaimed, 1)
	require.Equal(t, fields, reclaimed[0].Fields)
	require.NoError(t, reader.Ack(ctx, id))
}
