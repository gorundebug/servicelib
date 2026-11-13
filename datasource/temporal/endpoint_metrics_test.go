package temporal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

// The fixture provides only endpoint metadata, not a replacement metric backend.
// Constructors below use real Workflow metrics and the actual Tally exporter.
type endpointMetricConfig struct{ config.Config }

func (endpointMetricConfig) GetServices() []*config.ServiceConfig            { return nil }
func (endpointMetricConfig) GetStreams() []config.StreamConfig               { return nil }
func (endpointMetricConfig) GetDataConnectors() []config.DataConnectorConfig { return nil }
func (endpointMetricConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{&config.HttpEndpointConfig{ID: 1, Name: "endpoint"}}
}
func (endpointMetricConfig) GetPools() []*config.PoolConfig     { return nil }
func (endpointMetricConfig) GetLinks() []*config.LinkConfig     { return nil }
func (endpointMetricConfig) GetModules() []*config.ModuleConfig { return nil }
func (endpointMetricConfig) GetTypes() []*config.TypeConfig     { return nil }

type endpointMetricEnvironment struct {
	runtime.RuntimeEnvironment
	configuration *config.RuntimeConfig
	metricSource  metrics.Metrics
}

func (e endpointMetricEnvironment) RuntimeConfig() *config.RuntimeConfig { return e.configuration }
func (e endpointMetricEnvironment) Metrics() metrics.Metrics             { return e.metricSource }

type endpointMetricConnector struct {
	config.DataConnectorConfig
	kind api.DataConnectorType
}

func (c endpointMetricConnector) GetType() api.DataConnectorType { return c.kind }

type endpointMetricSink struct {
	runtime.DataSink
	connector endpointMetricConnector
}

func (s endpointMetricSink) GetName() string                       { return fmt.Sprintf("connector_%d", s.connector.kind) }
func (s endpointMetricSink) GetConfig() config.DataConnectorConfig { return s.connector }

type endpointMetricSource struct {
	runtime.DataSource
	connector endpointMetricConnector
}

func (s endpointMetricSource) GetName() string                       { return fmt.Sprintf("connector_%d", s.connector.kind) }
func (s endpointMetricSource) GetConfig() config.DataConnectorConfig { return s.connector }

func TestWorkflowMixedEndpointMetricsUseConsistentPrometheusLabels(t *testing.T) {
	orders := [][]api.DataConnectorType{
		{api.DataConnectorTypeGRPC, api.DataConnectorTypeTemporal, api.DataConnectorTypeHTTP, api.DataConnectorTypeKafka, api.DataConnectorTypeCron, api.DataConnectorTypeCustom},
		{api.DataConnectorTypeCustom, api.DataConnectorTypeCron, api.DataConnectorTypeKafka, api.DataConnectorTypeHTTP, api.DataConnectorTypeTemporal, api.DataConnectorTypeGRPC},
	}
	for index, order := range orders {
		t.Run(fmt.Sprintf("registration-order-%d", index), func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reporter := promreporter.NewReporter(promreporter.Options{
				DefaultTimerType: promreporter.HistogramTimerType,
				Registerer:       registry, Gatherer: registry,
			})
			scope, closer := tally.NewRootScope(tally.ScopeOptions{
				CachedReporter: reporter, SanitizeOptions: &temporalPrometheusSanitizeOptions,
				Separator: promreporter.DefaultSeparator,
			}, time.Hour)
			t.Cleanup(func() { _ = closer.Close() })
			var suite testsuite.WorkflowTestSuite
			suite.SetMetricsHandler(temporalMetricsHandler{scope: scope})
			env := suite.NewTestWorkflowEnvironment()
			fn := func(ctx workflow.Context) error {
				cfg, err := config.NewRuntimeConfig(endpointMetricConfig{})
				if err != nil {
					return err
				}
				environment := endpointMetricEnvironment{configuration: cfg, metricSource: newWorkflowMetrics(ctx)}
				for _, kind := range order {
					connector := endpointMetricConnector{kind: kind}
					sink, err := runtime.MakeDataSinkEndpoint(endpointMetricSink{connector: connector}, 1, environment)
					if err != nil {
						return err
					}
					source, err := runtime.MakeDataSourceEndpoint(endpointMetricSource{connector: connector}, 1, environment)
					if err != nil {
						return err
					}
					callCtx := context.Background()
					sink.OnRequestEnd(callCtx, sink.OnRequestStart(callCtx), nil)
					source.OnRequestEnd(callCtx, source.OnRequestStart(callCtx), nil)
				}
				return nil
			}
			env.ExecuteWorkflow(fn)
			require.NoError(t, env.GetWorkflowError())
			require.NoError(t, closer.Close())
			families, err := registry.Gather()
			require.NoError(t, err)
			seen := map[string]int{}
			for _, family := range families {
				name := family.GetName()
				if !strings.HasPrefix(name, "datasink_endpoint_") && !strings.HasPrefix(name, "datasource_endpoint_") {
					continue
				}
				for _, series := range family.Metric {
					labels := map[string]string{}
					for _, pair := range series.Label {
						labels[pair.GetName()] = pair.GetValue()
					}
					protocol, present := labels["protocol"]
					require.True(t, present, "%s must always have the protocol label", name)
					if labels["connector"] == fmt.Sprintf("connector_%d", api.DataConnectorTypeGRPC) {
						require.Equal(t, "grpc", protocol)
					} else {
						require.Empty(t, protocol)
					}
					seen[name]++
				}
			}
			for _, direction := range []string{"datasink", "datasource"} {
				for _, metric := range []string{"events_total", "messages_total", "active_requests", "request_duration_seconds"} {
					require.GreaterOrEqual(t, seen[direction+"_endpoint_"+metric], len(order))
				}
			}
		})
	}
}
