package redisstreams

import (
	"context"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"testing"
	"time"
)

type fixtureConfig struct {
	config.Config
	dc        *config.RedisStreamsDataConnectorConfig
	endpoints []config.EndpointConfig
}

func (c fixtureConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{c.dc}
}
func (c fixtureConfig) GetEndpoints() []config.EndpointConfig { return c.endpoints }
func (fixtureConfig) GetServices() []*config.ServiceConfig    { return nil }
func (fixtureConfig) GetStreams() []config.StreamConfig       { return nil }
func (fixtureConfig) GetPools() []*config.PoolConfig          { return nil }
func (fixtureConfig) GetModules() []*config.ModuleConfig      { return nil }
func (fixtureConfig) GetLinks() []*config.LinkConfig          { return nil }
func (fixtureConfig) GetTypes() []*config.TypeConfig          { return nil }

type fixtureEnv struct {
	observedMetrics metrics.Metrics
	runtime.RuntimeEnvironment
	cfg    *config.RuntimeConfig
	source runtime.DataSource
	sink   runtime.DataSink
}

func (e *fixtureEnv) RuntimeConfig() *config.RuntimeConfig                   { return e.cfg }
func (e *fixtureEnv) GetDataSource(int) runtime.DataSource                   { return e.source }
func (e *fixtureEnv) AddDataSource(s runtime.DataSource)                     { e.source = s }
func (e *fixtureEnv) GetDataSink(int) runtime.DataSink                       { return e.sink }
func (e *fixtureEnv) AddDataSink(s runtime.DataSink)                         { e.sink = s }
func (*fixtureEnv) RegisterEndpointConsumer(runtime.RuntimeEndpointConsumer) {}
func (e *fixtureEnv) Metrics() metrics.Metrics {
	if e.observedMetrics != nil {
		return e.observedMetrics
	}
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*fixtureEnv) Log() log.Logger          { return log.NoopLogger{} }
func (*fixtureEnv) Tracing() tracing.Tracing { return nil }

type errorStream struct {
	runtime.TypedConsumedStream[error]
}

func (errorStream) Consume(context.Context, error) {}

type inputStream struct {
	runtime.TypedInputStream[int, int, error]
	env       *fixtureEnv
	result    runtime.Consumer[int]
	emit      func(context.Context, int)
	hasResult bool
}

func (s *inputStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (s *inputStream) GetEnvironment() environment.ServiceEnvironment    { return s.env }
func (*inputStream) GetEndpointId() int                                  { return 2 }
func (*inputStream) GetName() string                                     { return "input" }
func (s *inputStream) GetResultStream() runtime.TypedSerializedStream[int] {
	if s.hasResult {
		return s
	}
	return nil
}
func (s *inputStream) SetResultConsumer(c runtime.Consumer[int])        { s.result = c }
func (*inputStream) GetErrorStream() runtime.TypedConsumedStream[error] { return errorStream{} }
func (s *inputStream) Consume(ctx context.Context, v int) {
	if s.emit != nil {
		s.emit(ctx, v)
	}
}

type handler struct {
	beginErr error
	consume  func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error
	ended    chan error
	failed   chan error
}

func (h *handler) OnError(_ context.Context, _ StreamContext[int, int, error], e error) {
	if h.failed != nil {
		h.failed <- e
	}
}
func (h *handler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, h.beginErr
}
func (h *handler) ConsumeMessage(ctx context.Context, sc StreamContext[int, int, error], _ struct{}, m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
	return h.consume(ctx, sc, m, r)
}
func (*handler) GetMessageID(context.Context, StreamContext[int, int, error], struct{}, int) string {
	return "id"
}
func (h *handler) EndRequest(_ context.Context, _ StreamContext[int, int, error], e error, _ struct{}) {
	if h.ended != nil {
		h.ended <- e
	}
}
func fixture(t *testing.T, url string, result bool) (*inputStream, *config.RedisStreamsEndpointConfig) {
	t.Helper()
	queue := "adapter-" + runtime.NewStreamID()
	ep := &config.RedisStreamsEndpointConfig{ID: 2, Name: "input", IdDataConnector: 1, Enabled: true, StreamKey: queue, ConsumerGroup: "workers", CreateConsumerGroup: true, ReadBlockMs: 50, ClaimIdleMs: 60000}
	out := &config.RedisStreamsEndpointConfig{ID: 3, Name: "output", IdDataConnector: 1, Enabled: true, StreamKey: queue, CreateStream: true}
	cfg, e := config.NewRuntimeConfig(fixtureConfig{dc: &config.RedisStreamsDataConnectorConfig{ID: 1, Name: "redis", Servers: []string{url}, Password: "adapter-test", OperationTimeoutMs: 1000}, endpoints: []config.EndpointConfig{ep, out}})
	if e != nil {
		t.Fatal(e)
	}
	return &inputStream{env: &fixtureEnv{cfg: cfg}, hasResult: result}, ep
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
