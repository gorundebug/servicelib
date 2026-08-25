package runtime

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type endpointTracingConfig struct {
	endpoint config.EndpointConfig
}

func (c endpointTracingConfig) GetServices() []*config.ServiceConfig { return nil }
func (c endpointTracingConfig) GetStreams() []config.StreamConfig    { return nil }
func (c endpointTracingConfig) GetDataConnectors() []config.DataConnectorConfig {
	return nil
}
func (c endpointTracingConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{c.endpoint}
}
func (endpointTracingConfig) GetPools() []*config.PoolConfig     { return nil }
func (endpointTracingConfig) GetLinks() []*config.LinkConfig     { return nil }
func (endpointTracingConfig) GetModules() []*config.ModuleConfig { return nil }
func (endpointTracingConfig) GetTypes() []*config.TypeConfig     { return nil }
func (endpointTracingConfig) GetProperty(string) interface{}     { return nil }
func (endpointTracingConfig) ApplyEnvironment() error            { return nil }

type reloadableEndpointTracingEnvironment struct {
	runtime *config.RuntimeConfig
}

func (environment *reloadableEndpointTracingEnvironment) RuntimeConfig() *config.RuntimeConfig {
	return environment.runtime
}

func endpointTracingRuntime(t *testing.T, enabled bool) *config.RuntimeConfig {
	t.Helper()
	runtimeConfig, err := config.NewRuntimeConfig(endpointTracingConfig{
		endpoint: &config.CustomEndpointConfig{
			ID: 100, Name: "input", IdDataConnector: 10,
			TracingEnabled: enabled,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeConfig
}

func TestDataSourceEndpointTracingReadsEveryRuntimeConfigSnapshot(t *testing.T) {
	environment := &reloadableEndpointTracingEnvironment{runtime: endpointTracingRuntime(t, false)}
	ctx := context.Background()
	if tracing.SamplingEnabled(ApplyDataSourceEndpointTracing(ctx, environment, 100)) {
		t.Fatal("disabled endpoint unexpectedly enabled tracing")
	}

	environment.runtime = endpointTracingRuntime(t, true)
	if !tracing.SamplingEnabled(ApplyDataSourceEndpointTracing(ctx, environment, 100)) {
		t.Fatal("reloaded endpoint tracing policy was not observed")
	}

	environment.runtime = endpointTracingRuntime(t, false)
	sampled := tracing.EnableSampling(ctx)
	if !tracing.SamplingEnabled(ApplyDataSourceEndpointTracing(sampled, environment, 100)) {
		t.Fatal("incoming sampled context was disabled by endpoint policy")
	}
}
