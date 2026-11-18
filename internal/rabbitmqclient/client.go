// Package rabbitmqclient contains only native RabbitMQ AMQP 1.0 connection,
// topology, and lifecycle policy shared by its source and sink adapters.
package rabbitmqclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const MaxPendingBytes = 8 * 1024 * 1024

var ErrStaleDelivery = errors.New("RabbitMQ delivery no longer belongs to the active receiver generation")
var ErrUnavailable = errors.New("RabbitMQ connection is recovering or closed")

type Generation struct {
	Conn      *rmq.AmqpConnection
	Consumer  *rmq.Consumer
	Publisher *rmq.Publisher
	session   *Session
}

func (g *Generation) Active() bool {
	g.session.mu.Lock()
	current := g.session.current == g
	g.session.mu.Unlock()
	_, open := g.Conn.State().(*rmq.StateOpen)
	return current && open && g.session.ctx.Err() == nil
}

type Session struct {
	mu         sync.Mutex
	current    *Generation
	changed    chan struct{}
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	cfg        *config.RabbitMQDataConnectorConfig
	endpoint   *config.RabbitMQEndpointConfig
	source     bool
	report     func(error)
	nextServer int
}

func Open(ctx context.Context, cfg *config.RabbitMQDataConnectorConfig, ep *config.RabbitMQEndpointConfig, source bool, report func(error)) (*Session, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := ep.Validate(source); err != nil {
		return nil, err
	}
	owner, cancel := context.WithCancel(ctx)
	s := &Session{ctx: owner, cancel: cancel, cfg: cfg, endpoint: ep, source: source, report: report, changed: make(chan struct{}), done: make(chan struct{})}
	g, err := s.connect()
	if err != nil {
		cancel()
		return nil, err
	}
	s.current = g
	go s.run()
	return s, nil
}
func (s *Session) notify(err error) {
	if err != nil && s.ctx.Err() == nil && s.report != nil {
		s.report(err)
	}
}
func (s *Session) replace(g *Generation) {
	s.mu.Lock()
	s.current = g
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}
func (s *Session) Current(ctx context.Context) (*Generation, error) {
	for {
		s.mu.Lock()
		g, changed := s.current, s.changed
		s.mu.Unlock()
		if g != nil && g.Active() {
			return g, nil
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-s.ctx.Done():
			return nil, ErrUnavailable
		case <-changed:
		}
	}
}
func (s *Session) Snapshot() (*Generation, error) {
	s.mu.Lock()
	g := s.current
	s.mu.Unlock()
	if g == nil || !g.Active() {
		return nil, ErrUnavailable
	}
	return g, nil
}
func (s *Session) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (s *Session) run() {
	defer close(s.done)
	defer s.cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			s.mu.Lock()
			g := s.current
			s.mu.Unlock()
			s.replace(nil)
			closeGeneration(s.ctx, g)
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		g := s.current
		s.mu.Unlock()
		if g != nil && g.Active() {
			continue
		}
		if g != nil {
			if state, ok := g.Conn.State().(*rmq.StateClosed); ok {
				s.notify(state.GetError())
			}
			s.replace(nil)
			closeGeneration(s.ctx, g)
		}
		recovered := false
		for attempt := 0; attempt < 60; attempt++ {
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			next, err := s.connect()
			if err != nil {
				s.notify(err)
				continue
			}
			if s.ctx.Err() != nil {
				closeGeneration(s.ctx, next)
				return
			}
			s.replace(next)
			recovered = true
			break
		}
		if !recovered {
			s.notify(errors.New("RabbitMQ reconnect attempts exhausted"))
			return
		}
	}
}
func closeGeneration(ctx context.Context, g *Generation) {
	if g == nil {
		return
	}
	closed, cancel := context.WithCancel(ctx)
	cancel()
	_ = g.Conn.Close(closed)
}
func (s *Session) connect() (*Generation, error) {
	opts, err := Options(s.cfg)
	if err != nil {
		return nil, err
	}
	var conn *rmq.AmqpConnection
	for i := 0; i < len(s.cfg.Servers); i++ {
		server := s.cfg.Servers[s.nextServer%len(s.cfg.Servers)]
		s.nextServer++
		u, _ := url.Parse(server)
		if s.cfg.TLSEnabled {
			u.Scheme = "amqps"
		}
		vhost := s.cfg.VirtualHost
		if vhost == "" {
			vhost = "/"
		}
		u.Path = "/" + vhost
		u.RawPath = "/" + url.PathEscape(vhost)
		dial, cancel := context.WithTimeout(s.ctx, s.cfg.ConnectTimeout())
		conn, err = rmq.Dial(dial, u.String(), opts)
		cancel()
		if err == nil {
			break
		}
		if s.ctx.Err() != nil {
			return nil, context.Cause(s.ctx)
		}
	}
	if err != nil {
		return nil, err
	}
	g := &Generation{Conn: conn, session: s}
	success := false
	defer func() {
		if !success {
			closeGeneration(s.ctx, g)
		}
	}()
	op, cancel := context.WithTimeout(s.ctx, s.cfg.OperationTimeout())
	defer cancel()
	if err = EnsureTopology(op, conn, s.endpoint, s.source); err != nil {
		return nil, err
	}
	if s.source {
		g.Consumer, err = conn.NewConsumer(op, s.endpoint.QueueName, &rmq.ConsumerOptions{InitialCredits: int32(s.endpoint.Credits()), SettleStrategy: rmq.ExplicitSettle})
	} else {
		var address rmq.ITargetAddress
		if s.endpoint.ExchangeName != "" {
			address = &rmq.ExchangeAddress{Exchange: s.endpoint.ExchangeName, Key: s.endpoint.RoutingKey}
		} else {
			address = &rmq.QueueAddress{Queue: s.endpoint.QueueName}
		}
		g.Publisher, err = conn.NewPublisher(op, address, &rmq.PublisherOptions{MaxInFlight: s.endpoint.MaxInFlight(), PublishTimeout: s.cfg.OperationTimeout()})
	}
	if err != nil {
		return nil, err
	}
	success = true
	return g, nil
}

func Options(cfg *config.RabbitMQDataConnectorConfig) (*rmq.AmqpConnOptions, error) {
	opts := &rmq.AmqpConnOptions{RecoveryConfiguration: &rmq.RecoveryConfiguration{ActiveRecovery: false}, TopologyRecoveryOptions: rmq.TopologyRecoveryDisabled, WriteTimeout: cfg.OperationTimeout()}
	if cfg.Username != "" {
		opts.SASLType = amqp.SASLTypePlain(cfg.Username, cfg.Password)
	} else if cfg.TLSCertFile != "" {
		opts.SASLType = amqp.SASLTypeExternal("")
	} else {
		opts.SASLType = amqp.SASLTypeAnonymous()
	}
	secure := cfg.TLSEnabled
	for _, server := range cfg.Servers {
		u, _ := url.Parse(server)
		secure = secure || u.Scheme == "amqps"
	}
	if secure {
		t := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}
		if cfg.TLSCAFile != "" {
			pem, e := os.ReadFile(cfg.TLSCAFile)
			if e != nil {
				return nil, e
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("RabbitMQ CA bundle contains no certificates")
			}
			t.RootCAs = pool
		}
		if cfg.TLSCertFile != "" {
			cert, e := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
			if e != nil {
				return nil, e
			}
			t.Certificates = []tls.Certificate{cert}
		}
		opts.TLSConfig = t
	}
	return opts, nil
}
func EnsureTopology(ctx context.Context, conn *rmq.AmqpConnection, ep *config.RabbitMQEndpointConfig, source bool) error {
	m := conn.Management()
	if ep.QueueName != "" {
		q, err := m.QueueInfo(ctx, ep.QueueName)
		if errors.Is(err, rmq.ErrDoesNotExist) && ep.CreateQueue {
			switch ep.QueueType {
			case api.Classic:
				q, err = m.DeclareQueue(ctx, &rmq.ClassicQueueSpecification{Name: ep.QueueName})
			default:
				q, err = m.DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: ep.QueueName})
			}
		}
		if err != nil {
			return fmt.Errorf("RabbitMQ queue %q: %w", ep.QueueName, err)
		}
		if q.Type() != rmq.Classic && q.Type() != rmq.Quorum {
			return fmt.Errorf("RabbitMQ queue %q uses unsupported queue type %q", ep.QueueName, q.Type())
		}
		if ep.QueueType != "" && string(q.Type()) != string(ep.QueueType) {
			return fmt.Errorf("RabbitMQ queue type does not match configured queueType")
		}
	}
	if ep.ExchangeName != "" && ep.CreateExchange {
		var spec rmq.IExchangeSpecification
		switch ep.ExchangeType {
		case api.Topic:
			spec = &rmq.TopicExchangeSpecification{Name: ep.ExchangeName}
		case api.Fanout:
			spec = &rmq.FanOutExchangeSpecification{Name: ep.ExchangeName}
		default:
			spec = &rmq.DirectExchangeSpecification{Name: ep.ExchangeName}
		}
		if _, err := m.DeclareExchange(ctx, spec); err != nil {
			return err
		}
	}
	if ep.CreateBinding {
		_, err := m.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{SourceExchange: ep.ExchangeName, DestinationQueue: ep.QueueName, BindingKey: ep.RoutingKey})
		if err != nil {
			return err
		}
	}
	// A native sender attach validates an existing exchange without declaring it.
	if source && ep.ExchangeName != "" && !ep.CreateExchange {
		p, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: ep.ExchangeName, Key: ep.RoutingKey}, nil)
		if err != nil {
			return err
		}
		return p.Close(ctx)
	}
	return nil
}

// Invalidate requests a fresh native connection/link generation after a link
// failure. No unsettled publication is replayed by the adapter.
func (s *Session) Invalidate(g *Generation) {
	s.mu.Lock()
	if s.current != g {
		s.mu.Unlock()
		return
	}
	s.current = nil
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	closeGeneration(s.ctx, g)
}
