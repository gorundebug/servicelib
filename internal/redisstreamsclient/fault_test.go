package redisstreamsclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

// respFrame preserves a complete RESP2 frame, including nested stream replies.
func respFrame(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, fmt.Errorf("short RESP frame")
	}
	switch line[0] {
	case '$':
		n, e := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if e != nil {
			return nil, e
		}
		if n >= 0 {
			payload := make([]byte, n+2)
			if _, e = io.ReadFull(r, payload); e != nil {
				return nil, e
			}
			line = append(line, payload...)
		}
	case '*':
		n, e := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if e != nil {
			return nil, e
		}
		for i := 0; i < n; i++ {
			part, e := respFrame(r)
			if e != nil {
				return nil, e
			}
			line = append(line, part...)
		}
	}
	return line, nil
}

func TestNativeLostPublishReplyDoesNotReplay(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	options, err := redis.ParseURL(address)
	require.NoError(t, err)
	options.Password = os.Getenv("REDIS_STREAMS_TEST_PASSWORD")
	admin := redis.NewClient(options)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "sa-fault-" + xid.New().String()
	defer admin.Del(ctx, key)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var dropped atomic.Bool
	var writes atomic.Int32
	var workers sync.WaitGroup
	var conns sync.Map
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			client, e := listener.Accept()
			if e != nil {
				return
			}
			conns.Store(client, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				defer conns.Delete(client)
				upstream, e := net.DialTimeout("tcp", options.Addr, time.Second)
				if e != nil {
					return
				}
				defer upstream.Close()
				_ = upstream.SetDeadline(time.Now().Add(10 * time.Second))
				input, output := bufio.NewReader(client), bufio.NewReader(upstream)
				for {
					command, e := respFrame(input)
					if e != nil {
						return
					}
					xadd := strings.Contains(string(command), "\r\nxadd\r\n")
					if xadd {
						writes.Add(1)
					}
					if _, e = upstream.Write(command); e != nil {
						return
					}
					reply, e := respFrame(output)
					if e != nil {
						return
					}
					if xadd && dropped.CompareAndSwap(false, true) {
						return
					} // Redis committed; client never sees its ID.
					if _, e = client.Write(reply); e != nil {
						return
					}
				}
			}()
		}
	}()
	defer func() {
		listener.Close()
		conns.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
		workers.Wait()
	}()
	cfg := &config.RedisStreamsDataConnectorConfig{Servers: []string{"redis://" + listener.Addr().String()}, Password: options.Password, OperationTimeoutMs: 500}
	session, err := Open(ctx, cfg, &config.RedisStreamsEndpointConfig{StreamKey: key, CreateStream: true}, false)
	require.NoError(t, err)
	defer session.Close()
	id, err := session.Publish(ctx, map[string]interface{}{"id": "ambiguous"})
	require.Error(t, err)
	require.Empty(t, id)
	require.True(t, dropped.Load())
	require.EqualValues(t, 1, writes.Load())
	require.EqualValues(t, 1, admin.XLen(ctx, key).Val())
	id, err = session.Publish(ctx, map[string]interface{}{"id": "next-explicit-call"})
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.EqualValues(t, 2, writes.Load())
	require.EqualValues(t, 2, admin.XLen(ctx, key).Val())
}

func TestNativeClaimAdvancesEmptyPage(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cfg := &config.RedisStreamsDataConnectorConfig{Servers: []string{address}, Password: os.Getenv("REDIS_STREAMS_TEST_PASSWORD")}
	options, err := Options(cfg)
	require.NoError(t, err)
	admin := redis.NewClient(options)
	defer admin.Close()
	key := "sa-cursor-" + xid.New().String()
	defer admin.Del(ctx, key)
	ep := &config.RedisStreamsEndpointConfig{StreamKey: key, ConsumerGroup: "workers", CreateConsumerGroup: true, ReadCount: 1, ClaimIdleMs: 60000}
	source, err := Open(ctx, cfg, ep, true)
	require.NoError(t, err)
	defer source.Close()
	// COUNT 1 scans up to ten pending entries. None of the first page is old enough.
	for i := 0; i < 15; i++ {
		require.NoError(t, admin.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"id": i}}).Err())
	}
	require.NoError(t, admin.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "workers", Consumer: "original", Streams: []string{key, ">"}, Count: 15, Block: -1}).Err())
	messages, deleted, err := source.Claim(ctx)
	require.NoError(t, err)
	require.Empty(t, messages)
	require.Empty(t, deleted)
	require.NotEqual(t, "0-0", source.cursor)
	firstCursor := source.cursor
	_, _, err = source.Claim(ctx)
	require.NoError(t, err)
	require.NotEqual(t, firstCursor, source.cursor)
	require.Equal(t, "0-0", source.cursor)
}

// A retained delivery must not use a replacement command connection to ACK.
// Killing only this test session leaves the shared broker and other tests intact.
func TestNativeDeliveryEpochAfterReconnect(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	for _, connectionKind := range []string{"commands", "reader"} {
		t.Run(connectionKind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cfg := &config.RedisStreamsDataConnectorConfig{Servers: []string{address}, Password: os.Getenv("REDIS_STREAMS_TEST_PASSWORD"), OperationTimeoutMs: 1000}
			options, err := Options(cfg)
			require.NoError(t, err)
			admin := redis.NewClient(options)
			defer admin.Close()
			key := "sa-epoch-" + xid.New().String()
			defer admin.Del(ctx, key)
			s, err := Open(ctx, cfg, &config.RedisStreamsEndpointConfig{StreamKey: key, ConsumerGroup: "workers", CreateConsumerGroup: true, ClaimIdleMs: 1, ReadBlockMs: 50}, true)
			require.NoError(t, err)
			defer s.Close()
			require.NoError(t, admin.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"payload": "retained"}}).Err())
			entries, err := s.Receive(ctx)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			client := s.commands
			if connectionKind == "reader" {
				client = s.reader
			}
			connection, err := client.ClientID(ctx).Result()
			require.NoError(t, err)
			require.NoError(t, admin.Do(ctx, "CLIENT", "KILL", "ID", connection).Err())
			if connectionKind == "reader" {
				// Observe replacement of the independent blocking-read connection.
				_, _ = s.Receive(ctx)
				_, err = s.Receive(ctx)
				require.NoError(t, err)
			}
			// Depending on idle socket detection, the first attempt either observes EOF or
			// reaches OnConnect. Neither may send XACK on the replacement connection.
			require.Error(t, s.AckEntry(ctx, entries[0]))
			require.Error(t, s.AckEntry(ctx, entries[0]))
			require.EqualValues(t, 1, admin.XPending(ctx, key, "workers").Val().Count)
			// A new claim is a fresh delivery and can settle normally after recovery.
			time.Sleep(5 * time.Millisecond)
			reclaimed, _, err := s.Claim(ctx)
			require.NoError(t, err)
			require.Len(t, reclaimed, 1)
			require.ErrorIs(t, s.AckEntry(ctx, entries[0]), ErrStaleDelivery)
			require.NoError(t, s.AckEntry(ctx, reclaimed[0]))
			require.Zero(t, admin.XPending(ctx, key, "workers").Val().Count)
			require.NoError(t, s.Close())
			require.ErrorIs(t, s.AckEntry(ctx, reclaimed[0]), ErrStaleDelivery)

		})
	}
}
