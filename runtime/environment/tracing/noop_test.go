package tracing

import (
	"context"
	"net/http"
	"testing"
)

type noopTestHandler struct{}

func (*noopTestHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestNoopTracingEngineDoesNotInstallTransportInstrumentation(t *testing.T) {
	engine := NewNoopTracingEngine()
	if engine.Tracing() != nil {
		t.Fatal("noop tracing engine returned a tracer")
	}
	if engine.GRPCStatsHandler() != nil || engine.GRPCClientHandler() != nil {
		t.Fatal("noop tracing engine returned a gRPC stats handler")
	}
	transport := http.DefaultTransport
	if got := engine.HTTPClientTransport(transport); got != transport {
		t.Fatal("noop tracing engine wrapped the HTTP transport")
	}
	handler := &noopTestHandler{}
	if got := engine.HTTPServerHandler(handler, "test"); got != handler {
		t.Fatal("noop tracing engine wrapped the HTTP handler")
	}
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatalf("noop tracing shutdown: %v", err)
	}
}
