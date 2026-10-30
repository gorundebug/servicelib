package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// The peer accepts TCP but never returns a Kafka/TLS response. Cancellation
// must close every accepted socket before this fixture is cleaned up.
func startupBlackhole(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
				closed <- struct{}{}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return listener.Addr().String(), closed
}

func TestStartupNetworkInterruptsSaramaConstructors(t *testing.T) {
	for _, mode := range []string{"client", "consumer", "admin", "tls", "sasl"} {
		t.Run(mode, func(t *testing.T) {
			address, closed := startupBlackhole(t)
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			cfg := sarama.NewConfig()
			cfg.Net.ReadTimeout = 5 * time.Second
			cfg.Metadata.Retry.Backoff = time.Second
			if mode == "tls" {
				cfg.Net.TLS.Enable = true
				cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			if mode == "sasl" {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.User = "fixture"
				cfg.Net.SASL.Password = "fixture"
			}
			finish, err := BindClientStartup(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			switch mode {
			case "consumer":
				var group sarama.ConsumerGroup
				group, err = sarama.NewConsumerGroup([]string{address}, "startup-fixture", cfg)
				if group != nil {
					_ = group.Close()
				}
			case "admin":
				var admin sarama.ClusterAdmin
				admin, err = sarama.NewClusterAdmin([]string{address}, cfg)
				if admin != nil {
					_ = admin.Close()
				}
			default:
				var client sarama.Client
				client, err = sarama.NewClient([]string{address}, cfg)
				if client != nil {
					_ = client.Close()
				}
			}
			if err == nil {
				t.Fatal("blackhole constructor succeeded")
			}
			if err := finish(); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("lost deadline", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatal("constructor exceeded startup budget", elapsed)
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("startup socket leaked")
			}
			if _, err := cfg.Net.Proxy.Dialer.Dial("tcp", address); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("failed startup reconnected", err)
			}
		})
	}
}

func TestStartupNetworkDetachesSuccessfulClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := sarama.NewConfig()
	const delay = 37 * time.Millisecond
	cfg.Metadata.Retry.BackoffFunc = func(int, int) time.Duration { return delay }
	finish, err := BindClientStartup(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := cfg.Net.Proxy.Dialer.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte{42}); err != nil {
		t.Fatal("detached socket closed", err)
	}
	var data [1]byte
	if _, err := io.ReadFull(peer, data[:]); err != nil || data[0] != 42 {
		t.Fatal("runtime I/O failed", err)
	}
	reconnected, err := cfg.Net.Proxy.Dialer.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal("startup cancellation prevented reconnect", err)
	}
	defer reconnected.Close()
	other, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got := cfg.Metadata.Retry.BackoffFunc(1, 2); got != delay {
		t.Fatal("runtime backoff changed", got)
	}
}

func TestStartupNetworkInterruptsRetryWait(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := sarama.NewConfig()
	cfg.Producer.Transaction.Retry.Backoff = 5 * time.Second
	finish, err := BindClientStartup(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan time.Duration, 1)
	go func() { result <- cfg.Producer.Transaction.Retry.BackoffFunc(1, 50) }()
	cancel()
	select {
	case delay := <-result:
		if delay != 0 {
			t.Fatal(delay)
		}
	case <-time.After(time.Second):
		t.Fatal("retry wait ignored cancellation")
	}
	if err := finish(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
