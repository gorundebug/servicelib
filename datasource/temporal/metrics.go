/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

// Temporal SDK metrics belong to the Temporal connector lifecycle.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	prometheus "github.com/prometheus/client_golang/prometheus"
	tally "github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/log"
)

const sdkMetricsBindAddressEnvironment = "TEMPORAL_SDK_METRICS_BIND_ADDRESS"

var sdkMetrics = struct {
	sync.Mutex
	address string
	handler client.MetricsHandler
	closer  io.Closer
}{}

var temporalPrometheusSanitizeOptions = tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	ReplacementCharacter: tally.DefaultReplacementCharacter,
}

// temporalMetricsHandler is the narrow adapter from Temporal's public metrics
// contract to the Tally reporter used by Temporal's Prometheus integration.
// Keeping the adapter here avoids the retired contrib/tally module, whose old
// dependency graph conflicts with modern split google.golang.org/genproto
// modules.
type temporalMetricsHandler struct {
	scope tally.Scope
}

func (h temporalMetricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	return temporalMetricsHandler{scope: h.scope.Tagged(tags)}
}

func (h temporalMetricsHandler) Counter(name string) client.MetricsCounter {
	if !strings.HasSuffix(name, "_total") {
		name += "_total"
	}
	return h.scope.Counter(name)
}

func (h temporalMetricsHandler) Gauge(name string) client.MetricsGauge {
	return h.scope.Gauge(name)
}

func (h temporalMetricsHandler) Timer(name string) client.MetricsTimer {
	if !strings.HasSuffix(name, "_seconds") {
		name += "_seconds"
	}
	return h.scope.Timer(name)
}

// sdkMetricsHandler enables the official Temporal SDK metrics only when the
// deployment explicitly provides a bind address. The exporter is process-wide:
// Temporal clients and workers share one runtime metric source, independently
// of how many graph connectors use it.
func sdkMetricsHandler(environment runtime.RuntimeEnvironment) (client.MetricsHandler, error) {
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
	registry := prometheus.NewRegistry()
	reporter := promreporter.NewReporter(promreporter.Options{
		DefaultTimerType: promreporter.HistogramTimerType,
		Registerer:       registry,
		Gatherer:         registry,
	})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter:  reporter,
		SanitizeOptions: &temporalPrometheusSanitizeOptions,
		Separator:       promreporter.DefaultSeparator,
	}, time.Second)
	handler := temporalMetricsHandler{scope: scope}
	server := &http.Server{
		Handler:           reporter.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			environment.Log().Error(
				context.Background(),
				"Temporal SDK metrics server failed",
				log.Str("address", address),
				log.Err(err),
			)
		}
	}()

	sdkMetrics.address = address
	sdkMetrics.handler = handler
	sdkMetrics.closer = closer
	return handler, nil
}
