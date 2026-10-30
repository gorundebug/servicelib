package kafka

import (
	"context"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type constructorConfig struct{ endpoint *config.KafkaEndpointConfig }

func (constructorConfig) GetServices() []*config.ServiceConfig { return nil }
func (constructorConfig) GetStreams() []config.StreamConfig    { return nil }
func (constructorConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{&config.KafkaDataConnectorConfig{ID: 1, Name: "broker"}}
}
func (c constructorConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{c.endpoint}
}
func (constructorConfig) GetPools() []*config.PoolConfig     { return nil }
func (constructorConfig) GetLinks() []*config.LinkConfig     { return nil }
func (constructorConfig) GetModules() []*config.ModuleConfig { return nil }
func (constructorConfig) GetTypes() []*config.TypeConfig     { return nil }
func (constructorConfig) GetProperty(string) interface{}     { return nil }
func (constructorConfig) ApplyEnvironment() error            { return nil }

type constructorEnvironment struct {
	runtime.RuntimeEnvironment
	cfg    *config.RuntimeConfig
	source runtime.DataSource
}

func (e *constructorEnvironment) RuntimeConfig() *config.RuntimeConfig                   { return e.cfg }
func (e *constructorEnvironment) GetDataSource(int) runtime.DataSource                   { return e.source }
func (e *constructorEnvironment) AddDataSource(s runtime.DataSource)                     { e.source = s }
func (*constructorEnvironment) RegisterEndpointConsumer(runtime.RuntimeEndpointConsumer) {}
func (*constructorEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*constructorEnvironment) Tracing() tracing.Tracing { return nil }
func (*constructorEnvironment) Log() log.Logger          { return log.NoopLogger{} }

type constructorStream struct {
	runtime.TypedInputStream[int, int, error]
	env *constructorEnvironment
}

func (s constructorStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (s constructorStream) GetEnvironment() environment.ServiceEnvironment    { return s.env }
func (constructorStream) GetEndpointId() int                                  { return 2 }
func (constructorStream) GetResultStream() runtime.TypedSerializedStream[int] { return nil }

type constructorErrorStream struct {
	runtime.TypedConsumedStream[error]
}

func (constructorErrorStream) Consume(context.Context, error) {}
func (constructorStream) GetErrorStream() runtime.TypedConsumedStream[error] {
	return constructorErrorStream{}
}
