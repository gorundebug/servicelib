/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	tally "github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	contribtally "go.temporal.io/sdk/contrib/tally"
)

const sdkMetricsBindAddressEnvironment = "TEMPORAL_SDK_METRICS_BIND_ADDRESS"

var sdkMetrics = struct {
	sync.Mutex
	address string
	handler client.MetricsHandler
	closer  io.Closer
}{}

// sdkMetricsHandler enables the official Temporal SDK metrics only when the
// deployment explicitly provides a bind address. The exporter is process-wide:
// Temporal clients and workers share one runtime metric source, independently
// of how many graph connectors use it.
func sdkMetricsHandler() (client.MetricsHandler, error) {
	address := strings.TrimSpace(os.Getenv(sdkMetricsBindAddressEnvironment))
	if address == "" {
		return nil, nil
	}

	sdkMetrics.Lock()
	defer sdkMetrics.Unlock()
	if sdkMetrics.handler != nil {
		if sdkMetrics.address != address {
			return nil, fmt.Errorf(
				"Temporal SDK metrics already listen on %q, cannot also use %q",
				sdkMetrics.address, address,
			)
		}
		return sdkMetrics.handler, nil
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for Temporal SDK metrics on %q: %w", address, err)
	}
	reporter := promreporter.NewReporter(promreporter.Options{})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter:  reporter,
		SanitizeOptions: &contribtally.PrometheusSanitizeOptions,
		Separator:       promreporter.DefaultSeparator,
	}, time.Second)
	handler := contribtally.NewMetricsHandler(contribtally.NewPrometheusNamingScope(scope))
	server := &http.Server{
		Handler:           reporter.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = server.Serve(listener)
	}()

	sdkMetrics.address = address
	sdkMetrics.handler = handler
	sdkMetrics.closer = closer
	return handler, nil
}
