package kafka

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type pausedStartupDialer struct {
	entered chan context.Context
	release chan struct{}
	conn    net.Conn
}

func (d *pausedStartupDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("unexpected non-context startup dial")
}

func (d *pausedStartupDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	d.entered <- ctx
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-d.release:
		return d.conn, nil
	}
}

func TestStartupNetworkInFlightDialTransition(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "cancelled-startup"
		if ready {
			name = "successful-detachment"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			dialer := &pausedStartupDialer{entered: make(chan context.Context, 1), release: make(chan struct{}), conn: conn}
			defer close(dialer.release)
			cfg := sarama.NewConfig()
			cfg.Net.Proxy.Enable, cfg.Net.Proxy.Dialer = true, dialer
			finish, err := BindClientStartup(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				got, err := cfg.Net.Proxy.Dialer.Dial("tcp", "fixture")
				if got != nil {
					_ = got.Close()
				}
				result <- err
			}()
			var inFlight context.Context
			select {
			case inFlight = <-dialer.entered:
			case <-time.After(time.Second):
				t.Fatal("dial did not enter")
			}
			if ready {
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				cancel()
				if inFlight.Err() != nil {
					t.Fatal("successful startup cancelled an in-flight runtime connection", inFlight.Err())
				}
				dialer.release <- struct{}{}
			} else {
				cancel()
				if err := finish(); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			select {
			case err := <-result:
				if ready && err != nil {
					t.Fatal("in-flight connection failed after readiness", err)
				}
				if !ready && !errors.Is(err, context.Canceled) {
					t.Fatal("startup dial ignored cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("dial was abandoned")
			}
		})
	}
}

func TestStartupTopicRetryPolicy(t *testing.T) {
	for _, failure := range []error{sarama.ErrNotController, io.EOF, sarama.ErrTopicAuthorizationFailed} {
		calls := 0
		err := retryStartupTopic(t.Context(), 2, 0, func() error { calls++; return failure })
		want := 3
		if errors.Is(failure, sarama.ErrTopicAuthorizationFailed) {
			want = 1
		}
		if !errors.Is(err, failure) || calls != want {
			t.Fatalf("policy changed: calls=%d err=%v", calls, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, result := make(chan struct{}), make(chan error, 1)
	go func() {
		result <- retryStartupTopic(ctx, 5, 5*time.Second, func() error { close(entered); return sarama.ErrNotController })
	}()
	<-entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("lost provisioning cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provisioning retry ignored cancellation")
	}
}
