package natsclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

func TestConnectCancellationInterruptsHandshake(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := l.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, e = Connect(ctx, &config.NATSDataConnectorConfig{Type: api.DataConnectorTypeNATS, Servers: []string{"nats://" + l.Addr().String()}, ConnectTimeoutMs: 2000}, nil)
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("got %v", e)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("handshake outlived context")
	}
	select {
	case c := <-accepted:
		c.Close()
	case <-time.After(time.Second):
		t.Fatal("no connection accepted")
	}
}

func TestGateShutdownStopsAdmissionAndWaitsForActive(t *testing.T) {
	var g Gate
	g.Open()
	if !g.Enter() {
		t.Fatal("not admitted")
	}
	done := g.Close()
	if g.Enter() {
		t.Fatal("admitted after shutdown")
	}
	select {
	case <-done:
		t.Fatal("closed with active work")
	default:
	}
	g.Leave()
	select {
	case <-done:
	default:
		t.Fatal("not drained")
	}
	g.Close()
}

func TestSubjectValidation(t *testing.T) {
	for _, s := range []string{"", "a..b", "a.>.b", "a*", "a b", "a.\n"} {
		if Subject(s, false) == nil {
			t.Errorf("accepted %q", s)
		}
	}
	for _, s := range []string{"a.*", "a.>", "a.b"} {
		if e := Subject(s, false); e != nil {
			t.Error(e)
		}
	}
	if Subject("a.*", true) == nil {
		t.Fatal("wildcard publication accepted")
	}
}
