// Package natsclient contains only the connection and resource policy shared by
// the native NATS source and sink. It is not a broker abstraction.
package natsclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const PendingMessages = 256
const PendingBytes = 8 * 1024 * 1024

// Gate prevents new requests during shutdown without spawning waiter goroutines.
// Its zero value is closed; Open starts a single lifecycle.
type Gate struct {
	mu     sync.Mutex
	open   bool
	active int
	done   chan struct{}
}

func (g *Gate) Open() { g.mu.Lock(); defer g.mu.Unlock(); g.open = true; g.done = make(chan struct{}) }
func (g *Gate) Enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return false
	}
	g.active++
	return true
}

// Retain tracks child delivery work of an already admitted handler, including
// during drain. New top-level requests must use Enter instead.
func (g *Gate) Retain() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == 0 {
		return false
	}
	g.active++
	return true
}
func (g *Gate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if !g.open && g.active == 0 {
		close(g.done)
	}
}
func (g *Gate) Close() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done == nil {
		g.done = make(chan struct{})
		close(g.done)
	}
	if g.open {
		g.open = false
		if g.active == 0 {
			close(g.done)
		}
	}
	return g.done
}

type contextDialer struct {
	owner   context.Context
	ctx     context.Context
	timeout time.Duration
	mu      sync.Mutex
	conn    net.Conn
	stop    func() bool
}

func (d *contextDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	ctx := d.ctx
	d.mu.Unlock()
	c, e := (&net.Dialer{Timeout: d.timeout}).DialContext(ctx, network, address)
	if e != nil {
		return nil, e
	}
	d.mu.Lock()
	if d.stop != nil {
		d.stop()
	}
	d.conn = c
	d.stop = context.AfterFunc(ctx, func() { c.Close() })
	d.mu.Unlock()
	return c, nil
}
func (d *contextDialer) finishStartup() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stop != nil {
		d.stop()
	}
	d.ctx = d.owner
	if d.conn != nil {
		c := d.conn
		d.stop = context.AfterFunc(d.owner, func() { c.Close() })
	}
}
func (d *contextDialer) release() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stop != nil {
		d.stop()
	}
}

// Connect uses the startup deadline only for initial connection. Reconnect dials
// retain the lifecycle context, with a finite attempt policy and 8 MiB buffer.
// report must be nonblocking; callers use a bounded error channel.
func Connect(owner context.Context, cfg *config.NATSDataConnectorConfig, report func(error)) (*nats.Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(owner, cfg.ConnectTimeout())
	defer cancel()
	d := &contextDialer{owner: owner, ctx: ctx, timeout: cfg.ConnectTimeout()}
	notify := func(err error) {
		if err != nil && owner.Err() == nil && report != nil {
			report(err)
		}
	}
	opts := []nats.Option{nats.SetCustomDialer(d), nats.Timeout(cfg.ConnectTimeout()), nats.MaxReconnects(60), nats.ReconnectWait(250 * time.Millisecond), nats.ReconnectBufSize(PendingBytes), nats.SyncQueueLen(PendingMessages),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { notify(e) }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) { notify(e) }),
		nats.ClosedHandler(func(c *nats.Conn) {
			d.release()
			if e := c.LastError(); e != nil {
				notify(e)
			} else {
				notify(nats.ErrConnectionClosed)
			}
		}),
	}
	if cfg.Username != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}
	if cfg.TLSEnabled || cfg.TLSServerName != "" || cfg.TLSCAFile != "" || cfg.TLSCertFile != "" {
		opts = append(opts, nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}))
	}
	if cfg.TLSCAFile != "" {
		opts = append(opts, nats.RootCAs(cfg.TLSCAFile))
	}
	if cfg.TLSCertFile != "" {
		opts = append(opts, nats.ClientCert(cfg.TLSCertFile, cfg.TLSKeyFile))
	}
	c, err := nats.Connect(strings.Join(cfg.Servers, ","), opts...)
	if ctx.Err() != nil {
		if c != nil {
			c.Close()
		}
		d.release()
		return nil, ctx.Err()
	}
	d.finishStartup()
	if err != nil {
		d.release()
	}
	return c, err
}

// Stream only creates resources when explicitly requested; existing resources
// are never updated. Even a racing create cannot replace an existing stream.
func Stream(ctx context.Context, js jetstream.JetStream, cfg *config.NATSEndpointConfig) (jetstream.Stream, error) {
	if cfg.StreamName == "" {
		return nil, fmt.Errorf("JetStream streamName is required")
	}
	s, err := js.Stream(ctx, cfg.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) && cfg.CreateStream {
		return js.CreateStream(ctx, jetstream.StreamConfig{Name: cfg.StreamName, Subjects: []string{cfg.Subject}, Storage: jetstream.FileStorage, Replicas: 1})
	}
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, err
	}
	for _, subject := range info.Config.Subjects {
		if Covers(subject, cfg.Subject) {
			return s, nil
		}
	}
	return nil, fmt.Errorf("JetStream stream does not cover endpoint subject %q", cfg.Subject)
}

func Consumer(ctx context.Context, s jetstream.Stream, cfg *config.NATSEndpointConfig) (jetstream.Consumer, error) {
	if cfg.DurableName == "" {
		return nil, fmt.Errorf("JetStream durableName is required")
	}
	c, err := s.Consumer(ctx, cfg.DurableName)
	if errors.Is(err, jetstream.ErrConsumerNotFound) && cfg.CreateConsumer {
		c, err = s.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: cfg.DurableName, AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: cfg.Subject, DeliverPolicy: jetstream.DeliverAllPolicy, ReplayPolicy: jetstream.ReplayInstantPolicy, MaxAckPending: PendingMessages})
	}
	if err != nil {
		return nil, err
	}
	info, err := c.Info(ctx)
	if err != nil {
		return nil, err
	}
	if info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.DeliverSubject != "" || info.Config.Durable != cfg.DurableName {
		return nil, fmt.Errorf("JetStream consumer must be durable pull with explicit ACK")
	}
	if info.Config.FilterSubject != cfg.Subject || len(info.Config.FilterSubjects) > 0 {
		return nil, fmt.Errorf("JetStream consumer filter must equal endpoint subject")
	}
	return c, nil
}

func Subject(subject string, publish bool) error {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n") {
		return fmt.Errorf("invalid NATS subject %q", subject)
	}
	parts := strings.Split(subject, ".")
	for i, p := range parts {
		if p == "" || (strings.ContainsAny(p, "*>") && (publish || (p != "*" && p != ">") || (p == ">" && i != len(parts)-1))) {
			return fmt.Errorf("invalid NATS subject %q", subject)
		}
	}
	return nil
}

// Covers reports whether every subject matched by requested belongs to pattern.
func Covers(pattern, requested string) bool {
	p, r := strings.Split(pattern, "."), strings.Split(requested, ".")
	for i, token := range p {
		if i >= len(r) {
			return false
		}
		if token == ">" {
			return true
		}
		if r[i] == ">" {
			return false
		}
		if token != "*" && token != r[i] {
			return false
		}
	}
	return len(p) == len(r)
}
