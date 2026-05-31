/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// TestOTelMetricNames verifies that the Prometheus metric names produced by
// otelhttp and otelgrpc match those hardcoded in the Grafana dashboards
// (grafana/dashboards/07_http_server.jsonnet, 08_http_client.jsonnet,
//  09_grpc_server.jsonnet, 10_grpc_client.jsonnet).
//
// If this test fails after upgrading otelhttp or otelgrpc, update both the
// expectedXxx slices below AND the corresponding .jsonnet dashboard files.
//
// Dependency versions this test was written for:
//   go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp           v0.68.0
//   go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0
//   go.opentelemetry.io/otel/exporters/prometheus                           v0.65.0
package opentelemetry_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── Expected metric family names ──────────────────────────────────────────────
// These must stay in sync with grafana/dashboards/07-10_*.jsonnet

var expectedHTTPServerMetrics = []string{
	"http_server_request_duration_seconds",
	"http_server_request_body_size_bytes",
	"http_server_response_body_size_bytes",
	// NOTE: http_server_active_requests not emitted by otelhttp v0.68
}

var expectedHTTPClientMetrics = []string{
	"http_client_request_duration_seconds",
	"http_client_request_body_size_bytes",
	// NOTE: http_client_response_body_size_bytes is not emitted by otelhttp v0.68.
}

var expectedGRPCServerMetrics = []string{
	// otelgrpc v0.68 (semconv v1.26+): renamed from rpc.server.duration (ms)
	// to rpc.server.call.duration (s); per-message metrics removed.
	"rpc_server_call_duration_seconds",
}

var expectedGRPCClientMetrics = []string{
	// otelgrpc v0.68 (semconv v1.26+): renamed from rpc.client.duration (ms)
	// to rpc.client.call.duration (s); per-message metrics removed.
	"rpc_client_call_duration_seconds",
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func newProvider(t *testing.T) (*sdkmetric.MeterProvider, *promclient.Registry) {
	t.Helper()
	registry := promclient.NewRegistry()
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, registry
}

// gatherNames collects all metric family names from the registry.
func gatherNames(t *testing.T, registry *promclient.Registry) map[string]bool {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names
}

// assertMetricNames checks that every expected name is present and prints a
// diagnostic showing which names are actually in the registry.
func assertMetricNames(t *testing.T, names map[string]bool, expected []string) {
	t.Helper()
	var missing []string
	for _, want := range expected {
		if !names[want] {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		var all []string
		for n := range names {
			all = append(all, n)
		}
		t.Errorf(
			"missing metric(s) — update .jsonnet dashboards and expected* slices:\n  missing: %s\n  present: %s",
			strings.Join(missing, ", "),
			strings.Join(all, ", "),
		)
	}
}

// ── minimal gRPC service for triggering stats handler ────────────────────────

// grpcPingHandler is a minimal unary service handler that decodes emptypb.Empty
// and returns emptypb.Empty. Used to produce gRPC stats without a real proto service.
var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.TestService",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler: func(_ interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				return &emptypb.Empty{}, nil
			},
		},
	},
	Streams: []grpc.StreamDesc{},
}

func startBufconnServer(t *testing.T, opts ...grpc.ServerOption) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(opts...)
	srv.RegisterService(&testServiceDesc, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })
	return lis
}

func dialBufconn(t *testing.T, lis *bufconn.Listener, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	}
	// "passthrough:///" prefix bypasses name resolution and uses the context dialer directly.
	conn, err := grpc.NewClient("passthrough:///bufnet", append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestOTelMetricNames(t *testing.T) {
	t.Run("HTTPServer", func(t *testing.T) {
		provider, registry := newProvider(t)

		handler := otelhttp.NewHandler(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("ok"))
			}),
			"test",
			otelhttp.WithMeterProvider(provider),
		)

		// POST with body to trigger request_body_size metric
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"x":1}`))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assertMetricNames(t, gatherNames(t, registry), expectedHTTPServerMetrics)
	})

	t.Run("HTTPClient", func(t *testing.T) {
		provider, registry := newProvider(t)

		// Target server that echoes a body so response_body_size is recorded
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("pong"))
		}))
		t.Cleanup(ts.Close)

		transport := otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithMeterProvider(provider))
		client := &http.Client{Transport: transport}

		resp, err := client.Post(ts.URL, "application/json", bytes.NewBufferString(`{"x":1}`))
		require.NoError(t, err)
		// Must drain the body so otelhttp transport records response_body_size_bytes.
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		assertMetricNames(t, gatherNames(t, registry), expectedHTTPClientMetrics)
	})

	t.Run("GRPCServer", func(t *testing.T) {
		provider, registry := newProvider(t)

		lis := startBufconnServer(t, grpc.StatsHandler(
			otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(provider)),
		))
		conn := dialBufconn(t, lis)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		out := new(emptypb.Empty)
		err := conn.Invoke(ctx, "/test.TestService/Ping", &emptypb.Empty{}, out)
		require.NoError(t, err)

		assertMetricNames(t, gatherNames(t, registry), expectedGRPCServerMetrics)
	})

	t.Run("GRPCClient", func(t *testing.T) {
		provider, registry := newProvider(t)

		lis := startBufconnServer(t)
		conn := dialBufconn(t, lis,
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithMeterProvider(provider))),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		out := new(emptypb.Empty)
		err := conn.Invoke(ctx, "/test.TestService/Ping", &emptypb.Empty{}, out)
		require.NoError(t, err)

		assertMetricNames(t, gatherNames(t, registry), expectedGRPCClientMetrics)
	})
}
