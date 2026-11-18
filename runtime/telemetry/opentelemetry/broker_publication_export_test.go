package opentelemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Exercise the production OTel -> Prometheus translation rather than the
// in-memory test registry: Grafana relies on this exact family and its labels.
func TestBrokerPublicationPrometheusExport(t *testing.T) {
	registry := promclient.NewRegistry()
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	backend := &Metrics{meter: provider.Meter("broker-export")}
	profiles := map[string][]string{
		"nats":         {"client-accepted", "flushed", "error"},
		"jetstream":    {"acknowledged", "error"},
		"pulsar":       {"accepted", "not-sent", "rejected", "unknown"},
		"redisstreams": {"accepted", "not-sent", "rejected", "unknown"},
		"rabbitmq":     {"accepted", "not-sent", "rejected", "unknown", "unroutable", "modified"},
	}
	expected := 0
	for protocol, outcomes := range profiles {
		publication, err := brokermetrics.NewPublication(backend, protocol, "events", "publish", outcomes...)
		require.NoError(t, err)
		for _, outcome := range outcomes {
			publication.Observe(t.Context(), outcome)
			publication.Observe(metrics.WithoutRecording(t.Context()), outcome)
			expected++
		}
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	seen := 0
	for _, family := range families {
		if family.GetName() != "broker_endpoint_publication_outcomes_total" {
			continue
		}
		for _, series := range family.Metric {
			require.Equal(t, float64(1), series.GetCounter().GetValue())
			labels := map[string]string{}
			for _, label := range series.Label {
				labels[label.GetName()] = label.GetValue()
			}
			require.Equal(t, "events", labels["connector"])
			require.Equal(t, "publish", labels["endpoint"])
			require.Contains(t, profiles[labels["protocol"]], labels["outcome"])
			seen++
		}
	}
	require.Equal(t, expected, seen)
	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "# TYPE broker_endpoint_publication_outcomes_total counter")
	require.NotContains(t, response.Body.String(), "publication_outcomes_total_total")
	t.Logf("Prometheus scrape exposes %d bounded-label outcomes across %d broker profiles", seen, len(profiles))
}

func TestBrokerSourceObservationPrometheusExport(t *testing.T) {
	registry := promclient.NewRegistry()
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	backend := &Metrics{meter: provider.Meter("broker-source-export")}
	profiles := map[string][]string{
		"nats": {"received"}, "jetstream": {"received", "redelivered"},
		"pulsar": {"received", "redelivered"}, "rabbitmq": {"received", "redelivered"},
		"redisstreams": {"received", "reclaimed"},
	}
	expected := 0
	for protocol, observations := range profiles {
		source, err := brokermetrics.NewSource(backend, protocol, "events", "read")
		require.NoError(t, err)
		for _, observation := range observations {
			source.Observe(t.Context(), observation)
			expected++
		}
		source.Observe(t.Context(), "acknowledged") // never a supported observation
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	seen := 0
	for _, family := range families {
		if family.GetName() != "broker_endpoint_source_observations_total" {
			continue
		}
		for _, series := range family.Metric {
			require.Equal(t, float64(1), series.GetCounter().GetValue())
			labels := map[string]string{}
			for _, label := range series.Label {
				labels[label.GetName()] = label.GetValue()
			}
			require.Contains(t, profiles[labels["protocol"]], labels["observation"])
			require.Equal(t, "events", labels["connector"])
			require.Equal(t, "read", labels["endpoint"])
			seen++
		}
	}
	require.Equal(t, expected, seen)
	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "# TYPE broker_endpoint_source_observations_total counter")
	require.NotContains(t, response.Body.String(), "source_observations_total_total")
}
