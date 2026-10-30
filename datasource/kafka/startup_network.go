package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type startupDialer interface {
	Dial(string, string) (net.Conn, error)
	DialContext(context.Context, string, string) (net.Conn, error)
}

// BindClientStartup makes synchronous Sarama construction cancellable without
// abandoning its constructor in a goroutine. Configure it before using cfg.
// Cancellation closes startup sockets, interrupts retry waits and prevents new
// connections. TLS and SASL still run in Sarama, over those same sockets.
//
// Call finish exactly once after the complete startup operation, including any
// initial assignment recovery. Success detaches the startup context: established
// connections and future reconnects retain their ordinary runtime policy. A
// cancelled startup stays cancelled; the caller must close the failed client.
// The returned error preserves cancellation even if Sarama reports no brokers.
func BindClientStartup(ctx context.Context, cfg *sarama.Config) (finish func() error, err error) {
	if ctx == nil || cfg == nil {
		return nil, errors.New("Kafka startup requires a context and config")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var dialer startupDialer = &net.Dialer{
		Timeout: cfg.Net.DialTimeout, KeepAlive: cfg.Net.KeepAlive, LocalAddr: cfg.Net.LocalAddr,
	}
	if cfg.Net.Proxy.Enable {
		var ok bool
		dialer, ok = cfg.Net.Proxy.Dialer.(startupDialer)
		if !ok {
			return nil, errors.New("Kafka startup proxy must support DialContext")
		}
	}
	// Keep the dial's cancellation separate from the caller's startup context.
	// Successful detachment must also preserve a DialContext already in flight.
	dialCtx, cancelDial := context.WithCancel(context.WithoutCancel(ctx))
	n := &startupNetwork{ctx: ctx, dialCtx: dialCtx, cancelDial: cancelDial, dialer: dialer, conns: make(map[*startupConn]struct{})}
	n.stop = context.AfterFunc(ctx, n.cancel)
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = n
	cfg.Metadata.Retry.BackoffFunc = n.backoff(cfg.Metadata.Retry.Backoff, cfg.Metadata.Retry.BackoffFunc)
	cfg.Producer.Transaction.Retry.BackoffFunc = n.backoff(cfg.Producer.Transaction.Retry.Backoff, cfg.Producer.Transaction.Retry.BackoffFunc)
	return n.finish, nil
}

type startupNetwork struct {
	mu         sync.Mutex
	ctx        context.Context // nil only after successful detachment
	dialCtx    context.Context
	cancelDial context.CancelFunc
	dialer     startupDialer
	conns      map[*startupConn]struct{}
	stop       func() bool
}

func (n *startupNetwork) Dial(network, address string) (net.Conn, error) {
	n.mu.Lock()
	ctx, dialCtx := n.ctx, n.dialCtx
	if ctx == nil {
		n.mu.Unlock()
		return n.dialer.Dial(network, address)
	}
	if err := ctx.Err(); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	n.mu.Unlock()
	conn, err := n.dialer.DialContext(dialCtx, network, address)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	if n.ctx == nil {
		n.mu.Unlock()
		return conn, nil
	}
	if err := ctx.Err(); err != nil {
		n.mu.Unlock()
		_ = conn.Close()
		return nil, err
	}
	tracked := &startupConn{Conn: conn, owner: n}
	n.conns[tracked] = struct{}{}
	n.mu.Unlock()
	return tracked, nil
}

func (n *startupNetwork) cancel() {
	n.mu.Lock()
	if n.ctx == nil {
		n.mu.Unlock()
		return
	}
	n.cancelDial()
	conns := n.conns
	n.conns = make(map[*startupConn]struct{})
	n.mu.Unlock()
	for conn := range conns {
		_ = conn.Close()
	}
}

func (n *startupNetwork) finish() error {
	n.mu.Lock()
	if n.ctx == nil {
		n.mu.Unlock()
		return nil
	}
	err := n.ctx.Err()
	if err == nil {
		n.ctx = nil
		n.dialCtx = nil
		n.cancelDial = nil
		n.conns = nil
	}
	n.mu.Unlock()
	n.stop()
	if err != nil {
		n.cancel()
	}
	return err
}

func (n *startupNetwork) backoff(fixed time.Duration, original func(int, int) time.Duration) func(int, int) time.Duration {
	return func(retries, max int) time.Duration {
		n.mu.Lock()
		ctx := n.ctx
		n.mu.Unlock()
		delay := fixed
		if original != nil {
			delay = original(retries, max)
		}
		if ctx == nil || delay <= 0 {
			return delay
		}
		// Waiting here, rather than inside Sarama's time.Sleep, makes an
		// already-running retry wait interruptible without changing retry counts.
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		return 0
	}
}

type startupConn struct {
	net.Conn
	owner *startupNetwork
}

func (c *startupConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}
