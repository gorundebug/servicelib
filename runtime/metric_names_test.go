/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// TestDataConnectorMetricNames verifies that datasource/datasink constructors
// register exactly the expected metric names. If this test fails after a metric
// rename, update the expected slices below AND the corresponding
// grafana/dashboards/03_datasource.jsonnet / 04_datasink.jsonnet.
package runtime

import (
	"context"
	"net/http"
	"reflect"
	"time"

	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
)

// ── minimal config ───────────────────────────────────────────────���────────────

type testConnectorConfig struct{}

func (c *testConnectorConfig) GetServices() []*config.ServiceConfig { return nil }
func (c *testConnectorConfig) GetStreams() []config.StreamConfig    { return nil }
func (c *testConnectorConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{
		&config.HttpDataConnectorConfig{ID: 1, Name: "http-connector"},
	}
}
func (c *testConnectorConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{
		&config.HttpEndpointConfig{ID: 1, Name: "http-endpoint", IdDataConnector: 1},
	}
}
func (c *testConnectorConfig) GetPools() []*config.PoolConfig     { return nil }
func (c *testConnectorConfig) GetLinks() []*config.LinkConfig     { return nil }
func (c *testConnectorConfig) GetModules() []*config.ModuleConfig { return nil }
func (c *testConnectorConfig) GetTypes() []*config.TypeConfig     { return nil }
func (c *testConnectorConfig) GetProperty(string) interface{}     { return nil }
func (c *testConnectorConfig) ApplyEnvironment() error            { return nil }

// ── mockRuntimeEnv ─────────────────────────────────────────────────────��──────

// mockRuntimeEnv implements RuntimeEnvironment for metric-registration tests.
// Only Metrics(), ServiceConfig(), and RuntimeConfig() are implemented;
// all other methods are provided by the embedded nil interface and will panic
// if called (which they should not be during construction).
type mockRuntimeEnv struct {
	RuntimeEnvironment
	m   metrics.Metrics
	cfg *config.RuntimeConfig
}

func (e *mockRuntimeEnv) Metrics() metrics.Metrics { return e.m }
func (e *mockRuntimeEnv) Tracing() tracing.Tracing { return nil }
func (e *mockRuntimeEnv) ServiceConfig() *config.ServiceConfig {
	return &config.ServiceConfig{Name: "test-svc"}
}
func (e *mockRuntimeEnv) RuntimeConfig() *config.RuntimeConfig { return e.cfg }
func (e *mockRuntimeEnv) Log() envlog.Logger                   { return nil }

// Satisfy environment.ServiceEnvironment (embedded via RuntimeEnvironment).
func (e *mockRuntimeEnv) ServiceDependencies() environment.ServiceDependencies { return nil }
func (e *mockRuntimeEnv) ServiceContext() interface{}                          { return nil }

// Satisfy remaining RuntimeEnvironment methods that the compiler requires but
// that are never called during metric registration.
func (e *mockRuntimeEnv) ServiceInit() error                               { return nil }
func (e *mockRuntimeEnv) Start(context.Context) error                      { return nil }
func (e *mockRuntimeEnv) ReloadConfig()                                    {}
func (e *mockRuntimeEnv) Stop(context.Context)                             {}
func (e *mockRuntimeEnv) Release()                                         {}
func (e *mockRuntimeEnv) GetRuntime() ServiceExecutionRuntime              { return nil }
func (e *mockRuntimeEnv) MetricsEngine() metrics.MetricsEngine             { return nil }
func (e *mockRuntimeEnv) TracingEngine() tracing.TracingEngine             { return nil }
func (e *mockRuntimeEnv) HasCustomHTTPServer() bool                        { return false }
func (e *mockRuntimeEnv) GetTaskPool(string) pool.TaskPool                 { return nil }
func (e *mockRuntimeEnv) GetPriorityTaskPool(string) pool.PriorityTaskPool { return nil }
func (e *mockRuntimeEnv) AddDataSource(DataSource)                         {}
func (e *mockRuntimeEnv) GetDataSource(int) DataSource                     { return nil }
func (e *mockRuntimeEnv) AddDataSink(DataSink)                             {}
func (e *mockRuntimeEnv) GetDataSink(int) DataSink                         { return nil }
func (e *mockRuntimeEnv) Delay(time.Duration, func())                      {}
func (e *mockRuntimeEnv) GetSerde(reflect.Type) (serde.Serializer, error)  { return nil, nil }
func (e *mockRuntimeEnv) RegisterHTTPHandler(string, http.Handler)         {}
func (e *mockRuntimeEnv) RegisterStream(RuntimeStream)                     {}
func (e *mockRuntimeEnv) RegisterStorage(store.Storage)                    {}
func (e *mockRuntimeEnv) CreateKeyValueJoinStorage(api.JoinStorageType, store.JoinStorageConfig, Stream) store.Storage {
	return nil
}

func newMockRuntimeEnv(t *testing.T) *mockRuntimeEnv {
	t.Helper()
	runtimeCfg, err := config.NewRuntimeConfig(&testConnectorConfig{})
	require.NoError(t, err)
	return &mockRuntimeEnv{
		m:   testmetrics.New(),
		cfg: runtimeCfg,
	}
}

func testMetrics(env *mockRuntimeEnv) *testmetrics.TestMetrics {
	return env.m.(*testmetrics.TestMetrics)
}

// testDataSource wraps *InputDataSource to satisfy the DataSource interface
// by adding no-op Start/Stop methods (not called during metric registration).
type testDataSource struct{ *InputDataSource }

func (d *testDataSource) Start(context.Context) error { return nil }
func (d *testDataSource) Stop(context.Context)        {}

// testDataSink wraps *OutputDataSink similarly.
type testDataSink struct{ *OutputDataSink }

func (d *testDataSink) Start(context.Context) error { return nil }
func (d *testDataSink) Stop(context.Context)        {}

// ── Expected metric names ─────────────────────────────────────────────────────

var expectedDataSourceConnectorMetrics = []string{
	"datasource_connector_events_total",
}

var expectedDataSourceEndpointMetrics = []string{
	"datasource_endpoint_active_requests",
	"datasource_endpoint_events_total",
	"datasource_endpoint_messages_total",
	"datasource_endpoint_pending_requests",
	"datasource_endpoint_request_duration_seconds",
}

var expectedDataSinkConnectorMetrics = []string{
	"datasink_connector_events_total",
}

var expectedDataSinkEndpointMetrics = []string{
	"datasink_endpoint_active_requests",
	"datasink_endpoint_events_total",
	"datasink_endpoint_messages_total",
	"datasink_endpoint_request_duration_seconds",
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestDataSourceConnectorRegistersMetrics(t *testing.T) {
	env := newMockRuntimeEnv(t)
	connector := &config.HttpDataConnectorConfig{ID: 1, Name: "http-connector"}

	_, err := MakeInputDataSource(connector, env)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedDataSourceConnectorMetrics, testMetrics(env).RegisteredNames(),
		"datasource connector metrics changed — update expectedDataSourceConnectorMetrics AND grafana/dashboards/03_datasource.jsonnet")
}

func TestDataSourceEndpointRegistersMetrics(t *testing.T) {
	env := newMockRuntimeEnv(t)
	connector := &config.HttpDataConnectorConfig{ID: 1, Name: "http-connector"}

	dataSource, err := MakeInputDataSource(connector, env)
	require.NoError(t, err)
	env.m.(*testmetrics.TestMetrics).Reset()

	_, err = MakeDataSourceEndpoint(&testDataSource{dataSource}, 1, env)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedDataSourceEndpointMetrics, testMetrics(env).RegisteredNames(),
		"datasource endpoint metrics changed — update expectedDataSourceEndpointMetrics AND grafana/dashboards/03_datasource.jsonnet")
}

func TestDataSinkConnectorRegistersMetrics(t *testing.T) {
	env := newMockRuntimeEnv(t)
	connector := &config.HttpDataConnectorConfig{ID: 1, Name: "http-connector"}

	_, err := MakeOutputDataSink(connector, env)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedDataSinkConnectorMetrics, testMetrics(env).RegisteredNames(),
		"datasink connector metrics changed — update expectedDataSinkConnectorMetrics AND grafana/dashboards/04_datasink.jsonnet")
}

func TestDataSinkEndpointRegistersMetrics(t *testing.T) {
	env := newMockRuntimeEnv(t)
	connector := &config.HttpDataConnectorConfig{ID: 1, Name: "http-connector"}

	dataSink, err := MakeOutputDataSink(connector, env)
	require.NoError(t, err)
	env.m.(*testmetrics.TestMetrics).Reset()

	_, err = MakeDataSinkEndpoint(&testDataSink{dataSink}, 1, env)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedDataSinkEndpointMetrics, testMetrics(env).RegisteredNames(),
		"datasink endpoint metrics changed — update expectedDataSinkEndpointMetrics AND grafana/dashboards/04_datasink.jsonnet")
}
