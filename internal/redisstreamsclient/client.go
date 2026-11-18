// Package redisstreamsclient owns native Redis connections for Streams endpoints.
package redisstreamsclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/rs/xid"
)

// Session separates blocking reads from settlement and publication connections.
// Receive and Claim belong to one source loop; their cursor is not shared across endpoints.
type Session struct {
	commands, reader *redis.Client
	endpoint         config.RedisStreamsEndpointConfig
	consumer, cursor string
	timeout          time.Duration
	settlement       sync.Mutex
	epoch            atomic.Uint64
}

func Options(c *config.RedisStreamsDataConnectorConfig) (*redis.Options, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	u, _ := url.Parse(c.Servers[0])
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	o := &redis.Options{Addr: net.JoinHostPort(u.Hostname(), port), DB: c.RedisDatabase, Username: c.Username, Password: c.Password,
		DialTimeout: c.ConnectTimeout(), ReadTimeout: c.OperationTimeout(), WriteTimeout: c.OperationTimeout(), ContextTimeoutEnabled: true,
		MaxRetries: -1, Protocol: 2, DisableIdentity: true, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}}
	// Never retry an ambiguous XADD. All deliberate source recovery happens in the source loop.
	if c.TLSEnabled || u.Scheme == "rediss" {
		o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLSServerName}
		if o.TLSConfig.ServerName == "" {
			o.TLSConfig.ServerName = u.Hostname()
		}
		if c.TLSCAFile != "" {
			pem, err := os.ReadFile(c.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("read Redis CA: %w", err)
			}
			roots, err := x509.SystemCertPool()
			if err != nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("Redis CA contains no certificates")
			}
			o.TLSConfig.RootCAs = roots
		}
		if c.TLSCertFile != "" {
			cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load Redis TLS identity: %w", err)
			}
			o.TLSConfig.Certificates = []tls.Certificate{cert}
		}
	}
	return o, nil
}

func Open(ctx context.Context, c *config.RedisStreamsDataConnectorConfig, e *config.RedisStreamsEndpointConfig, source bool) (*Session, error) {
	if err := e.Validate(source); err != nil {
		return nil, err
	}
	options, err := Options(c)
	if err != nil {
		return nil, err
	}
	s := &Session{endpoint: *e, consumer: e.ConsumerName, cursor: "0-0", timeout: c.OperationTimeout()}
	if source {
		// One command connection keeps settlement epochs unambiguous. The blocking
		// reader has its own connection and never occupies this pool.
		options.PoolSize, options.MaxActiveConns = 1, 1
		options.OnConnect = func(ctx context.Context, _ *redis.Conn) error {
			s.epoch.Add(1)
			return s.checkEpoch(ctx)
		}
	}
	s.commands = redis.NewClient(options)
	if s.consumer == "" {
		s.consumer = fmt.Sprintf("sa-%d-%s", e.ID, xid.New().String())
	}
	success := false
	defer func() {
		if !success {
			s.Close()
		}
	}()
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err = s.commands.Ping(op).Err(); err != nil {
		return nil, err
	}
	info, err := s.commands.Info(op, "server").Result()
	if err != nil {
		return nil, fmt.Errorf("Redis startup INFO server: %w", err)
	}
	if err = validateServerVersion(info); err != nil {
		return nil, err
	}
	role, err := s.commands.Do(op, "ROLE").Slice()
	if err != nil {
		return nil, fmt.Errorf("Redis startup ROLE: %w", err)
	}
	if len(role) == 0 || role[0] != "master" {
		return nil, errors.New("Redis Streams requires a standalone primary")
	}
	cluster, err := s.commands.Info(op, "cluster").Result()
	if err != nil {
		return nil, fmt.Errorf("Redis startup INFO cluster: %w", err)
	}
	if strings.Contains(cluster, "cluster_enabled:1") {
		return nil, errors.New("Redis Streams cluster topology is not supported")
	}

	if source {
		if e.CreateConsumerGroup {
			start := e.GroupStartId
			if start == "" {
				start = "0-0"
			}
			err = s.commands.XGroupCreateMkStream(op, e.StreamKey, e.ConsumerGroup, start).Err()
			if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP ") {
				return nil, err
			}
		} else {
			groups, err := s.commands.XInfoGroups(op, e.StreamKey).Result()
			if err != nil {
				return nil, err
			}
			found := false
			for _, g := range groups {
				found = found || g.Name == e.ConsumerGroup
			}
			if !found {
				return nil, errors.New("Redis Streams consumer group does not exist; creation is disabled")
			}
		}
		readOptions := *options
		readOptions.PoolSize = 1
		readOptions.MaxActiveConns = 1
		readOptions.ReadTimeout = s.BlockDuration() + s.timeout
		s.reader = redis.NewClient(&readOptions)
	}
	success = true
	return s, nil
}
func (s *Session) Close() error {
	s.epoch.Add(1)
	var readErr error
	if s.reader != nil {
		readErr = s.reader.Close()
	}
	return errors.Join(readErr, s.commands.Close())
}
func (s *Session) Count() int64 {
	if s.endpoint.ReadCount == 0 {
		return 64
	}
	return int64(s.endpoint.ReadCount)
}
func (s *Session) BlockDuration() time.Duration {
	if s.endpoint.ReadBlockMs == 0 {
		return time.Second
	}
	return time.Duration(s.endpoint.ReadBlockMs) * time.Millisecond
}
func (s *Session) ClaimInterval() time.Duration {
	if s.endpoint.ClaimIntervalMs == 0 {
		return time.Second
	}
	return time.Duration(s.endpoint.ClaimIntervalMs) * time.Millisecond
}
func (s *Session) Receive(ctx context.Context) ([]Entry, error) {
	if s.reader == nil {
		return nil, errors.New("Redis sink cannot receive")
	}
	op, cancel := context.WithTimeout(ctx, s.BlockDuration()+s.timeout)
	defer cancel()
	streams, err := s.reader.Do(op, "XREADGROUP", "GROUP", s.endpoint.ConsumerGroup, s.consumer,
		"COUNT", s.Count(), "BLOCK", s.BlockDuration().Milliseconds(), "STREAMS", s.endpoint.StreamKey, ">").Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var messages []Entry
	for _, raw := range streams {
		stream, ok := raw.([]interface{})
		if !ok || len(stream) != 2 {
			return nil, errors.New("invalid Redis stream response")
		}
		entries, err := decodeEntries(stream[1])
		if err != nil {
			return nil, err
		}
		for i := range entries {
			entries[i].epoch = s.epoch.Load()
		}
		messages = append(messages, entries...)
	}
	return messages, nil
}

// Claim retains the returned cursor even for empty pages. Deleted pending IDs
// are returned separately so the source can report data loss without fabricating messages.
func (s *Session) Claim(ctx context.Context) ([]Entry, []string, error) {
	if s.reader == nil {
		return nil, nil, errors.New("Redis sink cannot reclaim")
	}
	s.settlement.Lock()
	defer s.settlement.Unlock()
	idle := time.Duration(s.endpoint.ClaimIdleMs) * time.Millisecond
	if idle == 0 {
		idle = time.Minute
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	reply, err := s.commands.Do(op, "XAUTOCLAIM", s.endpoint.StreamKey, s.endpoint.ConsumerGroup,
		s.consumer, idle.Milliseconds(), s.cursor, "COUNT", s.Count()).Slice()
	if err != nil {
		return nil, nil, err
	}
	if len(reply) != 3 {
		return nil, nil, errors.New("invalid Redis reclaim response")
	}
	next, ok := reply[0].(string)
	if !ok {
		return nil, nil, errors.New("invalid Redis reclaim cursor")
	}
	messages, err := decodeEntries(reply[1])
	if err != nil {
		return nil, nil, err
	}
	rawDeleted, ok := reply[2].([]interface{})
	if !ok {
		return nil, nil, errors.New("invalid Redis deleted entry list")
	}
	deleted := make([]string, len(rawDeleted))
	for i, raw := range rawDeleted {
		id, ok := raw.(string)
		if !ok {
			return nil, nil, errors.New("invalid Redis deleted entry ID")
		}
		deleted[i] = id
	}
	for i := range messages {
		messages[i].epoch = s.epoch.Load()
	}
	s.cursor = next
	return messages, deleted, nil
}

// AckEntry fences retained deliveries when a local connection has been replaced.
// It does not provide broker-side ownership fencing after XAUTOCLAIM.
func (s *Session) AckEntry(ctx context.Context, entry Entry) error {
	return s.Ack(context.WithValue(ctx, settlementEpochKey{}, entry.epoch), entry.ID)
}

type settlementEpochKey struct{}

var ErrStaleDelivery = errors.New("Redis delivery belongs to an expired connection epoch")

func (s *Session) checkEpoch(ctx context.Context) error {
	if epoch, ok := ctx.Value(settlementEpochKey{}).(uint64); ok && (epoch == 0 || epoch != s.epoch.Load()) {
		return ErrStaleDelivery
	}
	return nil
}
func (s *Session) Ack(ctx context.Context, id string) error {
	s.settlement.Lock()
	defer s.settlement.Unlock()
	if err := s.checkEpoch(ctx); err != nil {
		return err
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	n, err := s.commands.XAck(op, s.endpoint.StreamKey, s.endpoint.ConsumerGroup, id).Result()
	if err == nil && n == 0 {
		return errors.New("Redis entry is no longer pending")
	}
	return err
}

// Publish never replays a timed out command. Entry ID is available only after a successful reply.
func (s *Session) Publish(ctx context.Context, values map[string]interface{}) (string, error) {
	if len(values) == 0 {
		return "", errors.New("Redis publication requires field/value records")
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.commands.XAdd(op, &redis.XAddArgs{Stream: s.endpoint.StreamKey, NoMkStream: !s.endpoint.CreateStream, ID: "*", Values: values}).Result()
}

func validateServerVersion(info string) error {
	for _, line := range strings.Split(info, "\n") {
		version, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.Split(version, ".")
		if len(parts) < 2 {
			break
		}
		major, e1 := strconv.Atoi(parts[0])
		minor, e2 := strconv.Atoi(parts[1])
		if e1 == nil && e2 == nil && (major > 7 || (major == 7 && minor >= 2)) {
			return nil
		}
		return errors.New("Redis Streams requires Redis 7.2 or newer")
	}
	return errors.New("Redis startup did not return a valid redis_version")
}
