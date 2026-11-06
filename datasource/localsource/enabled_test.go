package localsource

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type enabledTestConfig struct{ endpoint *config.CustomEndpointConfig }

func (enabledTestConfig) GetServices() []*config.ServiceConfig { return nil }
func (enabledTestConfig) GetStreams() []config.StreamConfig    { return nil }
func (enabledTestConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{&config.CustomDataConnectorConfig{ID: 1, Name: "broker"}}
}
func (c enabledTestConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{c.endpoint}
}
func (enabledTestConfig) GetPools() []*config.PoolConfig     { return nil }
func (enabledTestConfig) GetLinks() []*config.LinkConfig     { return nil }
func (enabledTestConfig) GetModules() []*config.ModuleConfig { return nil }
func (enabledTestConfig) GetTypes() []*config.TypeConfig     { return nil }
func (enabledTestConfig) GetProperty(string) interface{}     { return nil }
func (enabledTestConfig) ApplyEnvironment() error            { return nil }

type enabledTestEnvironment struct {
	runtime.RuntimeEnvironment
	cfg    *config.RuntimeConfig
	source runtime.DataSource
}

func (e *enabledTestEnvironment) RuntimeConfig() *config.RuntimeConfig                   { return e.cfg }
func (e *enabledTestEnvironment) GetDataSource(int) runtime.DataSource                   { return e.source }
func (e *enabledTestEnvironment) AddDataSource(s runtime.DataSource)                     { e.source = s }
func (*enabledTestEnvironment) RegisterEndpointConsumer(runtime.RuntimeEndpointConsumer) {}
func (*enabledTestEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*enabledTestEnvironment) Tracing() tracing.Tracing { return nil }
func (*enabledTestEnvironment) Log() log.Logger          { return log.NoopLogger{} }

type enabledTestProducer struct {
	started chan struct{}
	stopped bool
}

func (p *enabledTestProducer) Start(ctx context.Context, _ runtime.Consumer[int]) error {
	close(p.started)
	<-ctx.Done()
	return nil
}
func (p *enabledTestProducer) Stop(context.Context) { p.stopped = true }

type enabledTestConsumer struct {
	runtime.InputEndpointConsumer
	starts, stops int
}

func (c *enabledTestConsumer) Start(context.Context) error          { c.starts++; return nil }
func (c *enabledTestConsumer) Stop(context.Context)                 { c.stops++ }
func (c *enabledTestConsumer) EndpointRequest(context.Context, int) {}
func TestCustomSourceEnabledControlsProducerLifecycle(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			cfg, err := config.NewRuntimeConfig(enabledTestConfig{&config.CustomEndpointConfig{ID: 2, Name: "source", IdDataConnector: 1, Enabled: enabled}})
			if err != nil {
				t.Fatal(err)
			}
			env := &enabledTestEnvironment{cfg: cfg}
			producer := &enabledTestProducer{started: make(chan struct{})}
			endpoint, err := createCustomEndpoint[int](2, env, producer)
			if err != nil {
				t.Fatal(err)
			}
			consumer := &enabledTestConsumer{}
			endpoint.consumer = consumer
			if err := endpoint.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if enabled {
				select {
				case <-producer.started:
				case <-time.After(time.Second):
					t.Fatal("producer did not start")
				}
			}
			endpoint.Stop(t.Context())
			if producer.stopped != enabled || consumer.starts != map[bool]int{false: 0, true: 1}[enabled] || consumer.stops != consumer.starts {
				t.Fatalf("unexpected lifecycle: producer stopped=%v consumer=%+v", producer.stopped, consumer)
			}
			if !enabled {
				select {
				case <-producer.started:
					t.Fatal("disabled producer started")
				default:
				}
			}
		})
	}
}
