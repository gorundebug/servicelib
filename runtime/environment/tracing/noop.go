package tracing

import (
	"context"
	"net/http"

	"google.golang.org/grpc/stats"
)

// NoopTracingEngine disables tracing at the integration boundary. In
// particular, it contributes no gRPC stats handlers and wraps no HTTP
// transports or handlers, so disabled tracing adds no per-request middleware.
type NoopTracingEngine struct{}

func NewNoopTracingEngine() TracingEngine { return NoopTracingEngine{} }

func (NoopTracingEngine) Tracing() Tracing                 { return nil }
func (NoopTracingEngine) GRPCStatsHandler() stats.Handler  { return nil }
func (NoopTracingEngine) GRPCClientHandler() stats.Handler { return nil }
func (NoopTracingEngine) HTTPClientTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		return http.DefaultTransport
	}
	return base
}
func (NoopTracingEngine) HTTPServerHandler(next http.Handler, _ string) http.Handler {
	return next
}
func (NoopTracingEngine) Shutdown(context.Context) error { return nil }
