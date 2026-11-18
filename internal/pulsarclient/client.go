package pulsarclient

import (
	"context"
	"crypto/tls"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrClosed = errors.New("Pulsar session is closed")
var ErrCapacity = errors.New("Pulsar publication capacity exhausted")

// ErrNotSent marks rejection before handing a message to the native producer.
// Preserve the cause so context cancellation and capacity remain inspectable.
var ErrNotSent = errors.New("Pulsar publication was not submitted")

// Session owns one native endpoint. Closing never removes its subscription.
type Session struct {
	client    pulsar.Client
	consumer  pulsar.Consumer
	producer  pulsar.Producer
	mu        sync.Mutex
	closed    bool
	active    int
	drained   chan struct{}
	closeOnce sync.Once
	closeDone chan struct{}
	capacity  int
}

func Open(ctx context.Context, c *config.PulsarDataConnectorConfig, e *config.PulsarEndpointConfig, source bool) (*Session, error) {
	if err := EnsureResources(ctx, c, e, source); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timeout := c.OperationTimeout()
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	opts := pulsar.ClientOptions{URL: c.Servers[0], ConnectionTimeout: c.ConnectTimeout(), OperationTimeout: timeout,
		TLSValidateHostname: true, TLSMinVersion: tls.VersionTLS12, TLSTrustCertsFilePath: c.TLSCAFile,
		MetricsRegisterer: prometheus.NewRegistry(), MemoryLimitBytes: 64 << 20}
	if c.Token != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(c.Token)
	}
	if c.TLSCertFile != "" {
		opts.Authentication = pulsar.NewAuthenticationTLS(c.TLSCertFile, c.TLSKeyFile)
	}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, err
	}
	s := &Session{client: client, capacity: e.MaxInFlight(), drained: make(chan struct{}), closeDone: make(chan struct{})}
	if source {
		mode := pulsar.Shared
		switch e.SubscriptionMode() {
		case api.Exclusive:
			mode = pulsar.Exclusive
		case api.Failover:
			mode = pulsar.Failover
		}
		// No batching in our producers; accept external batched messages with individual
		// batch-index ACKs. The ACK API confirms local dispatch, not broker durability.
		s.consumer, err = client.Subscribe(pulsar.ConsumerOptions{Topic: e.Topic, SubscriptionName: e.SubscriptionName, Type: mode,
			ReceiverQueueSize: e.QueueSize(), NackRedeliveryDelay: e.RedeliveryDelay(), AckGroupingOptions: &pulsar.AckGroupingOptions{MaxSize: 1},
			EnableBatchIndexAcknowledgment: true, AckWithResponse: false})
	} else {
		s.producer, err = client.CreateProducer(pulsar.ProducerOptions{Topic: e.Topic, DisableBatching: true,
			DisableBlockIfQueueFull: true, MaxPendingMessages: e.MaxInFlight(), SendTimeout: c.OperationTimeout()})
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}
func (s *Session) enter() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.active >= s.capacity {
		return ErrCapacity
	}
	s.active++
	return nil
}
func (s *Session) leave() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.closed && s.active == 0 {
		close(s.drained)
	}
}
func (s *Session) Receive(ctx context.Context) (pulsar.Message, error) {
	if s.consumer == nil {
		return nil, errors.New("Pulsar sink cannot receive")
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	return s.consumer.Receive(ctx)
}
func (s *Session) Ack(ctx context.Context, m pulsar.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if s.consumer == nil || m == nil {
		return errors.New("Pulsar ACK requires source delivery")
	}
	return s.consumer.Ack(m)
}
func (s *Session) Nack(ctx context.Context, m pulsar.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if s.consumer == nil || m == nil {
		return errors.New("Pulsar NACK requires source delivery")
	}
	s.consumer.Nack(m)
	return nil
}
func snapshot(m *pulsar.ProducerMessage) (*pulsar.ProducerMessage, error) {
	if m == nil {
		return nil, errors.New("Pulsar publication is missing")
	}
	if m.Value != nil || m.Schema != nil || m.Transaction != nil || m.SequenceID != nil || len(m.ReplicationClusters) > 0 || m.DisableReplication || m.DeliverAfter != 0 || !m.DeliverAt.IsZero() {
		return nil, errors.New("unsupported Pulsar per-message schema, transaction, replication or scheduling option")
	}
	copy := *m
	copy.Payload = append([]byte(nil), m.Payload...)
	copy.Properties = maps.Clone(m.Properties)
	return &copy, nil
}

// SendAsync snapshots mutable native fields before submission. Callback execution
// retains admission capacity and is included in Close's drain boundary.
func (s *Session) SendAsync(ctx context.Context, m *pulsar.ProducerMessage, callback func(pulsar.MessageID, error)) error {
	if s.producer == nil {
		return errors.New("Pulsar source cannot publish")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	copy, err := snapshot(m)
	if err != nil {
		return err
	}
	if err = s.enter(); err != nil {
		return err
	}
	s.producer.SendAsync(ctx, copy, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) { defer s.leave(); callback(id, err) })
	return nil
}
func (s *Session) Send(ctx context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if s.producer == nil {
		return nil, errors.Join(ErrNotSent, errors.New("Pulsar source cannot publish"))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrNotSent, err)
	}
	copy, err := snapshot(m)
	if err != nil {
		return nil, errors.Join(ErrNotSent, err)
	}
	if err = s.enter(); err != nil {
		return nil, errors.Join(ErrNotSent, err)
	}
	defer s.leave()
	return s.producer.Send(ctx, copy)
}

// Close stops admission and drains accepted publications to the supplied deadline.
// Native close is owned once; repeated calls can wait for the same completion.
func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.active == 0 {
			close(s.drained)
		}
	}
	s.mu.Unlock()
	select {
	case <-s.drained:
	case <-ctx.Done():
	}
	s.closeOnce.Do(func() { go func() { defer close(s.closeDone); s.client.Close() }() })
	select {
	case <-s.closeDone:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
