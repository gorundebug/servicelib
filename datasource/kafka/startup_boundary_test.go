package kafka

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type gatedStartupContext struct {
	context.Context
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *gatedStartupContext) Err() error {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestStartupNetworkCancellationCheckIsAtomicWithFinish(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &gatedStartupContext{Context: parent, entered: make(chan struct{}), release: make(chan struct{})}
	unblockCheck := sync.OnceFunc(func() { close(ctx.release) })
	defer unblockCheck()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	dialer := &pausedStartupDialer{entered: make(chan context.Context, 1), release: make(chan struct{}), conn: conn}
	unblockDial := sync.OnceFunc(func() { close(dialer.release) })
	defer unblockDial()
	cfg := sarama.NewConfig()
	cfg.Net.Proxy.Enable, cfg.Net.Proxy.Dialer = true, dialer
	finish, err := BindClientStartup(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	network := cfg.Net.Proxy.Dialer.(*startupNetwork)
	ctx.armed.Store(true)
	result := make(chan error, 1)
	go func() {
		got, err := network.Dial("tcp", "fixture")
		if got != nil {
			_ = got.Close()
		}
		result <- err
	}()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not check cancellation")
	}
	// If this lock were free, successful finish + cancellation of the caller
	// could overtake this check and incorrectly reject a runtime connection.
	if network.mu.TryLock() {
		network.mu.Unlock()
		t.Fatal("finish can overtake the startup cancellation check")
	}
	unblockCheck()
	select {
	case <-dialer.entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	cancel()
	unblockDial()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("runtime connection rejected after successful startup", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight dial did not complete")
	}
}
