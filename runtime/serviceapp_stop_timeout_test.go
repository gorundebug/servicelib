package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// Each phase deliberately outlives the Stop deadline. Its eventual completion
// must not decrement another phase's counter or close another phase's channel.
type stopTimeoutGate struct {
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func (*stopTimeoutGate) Start(context.Context) error { return nil }
func (g *stopTimeoutGate) Stop(context.Context) {
	close(g.entered)
	<-g.release
	close(g.returned)
}

type stopTimeoutDelay struct{ *stopTimeoutGate }

func (*stopTimeoutDelay) Delay(context.Context, time.Duration, func()) error { return nil }

type stopTimeoutSink struct {
	*OutputDataSink
	*stopTimeoutGate
}

func (s *stopTimeoutSink) Start(ctx context.Context) error { return s.stopTimeoutGate.Start(ctx) }
func (s *stopTimeoutSink) Stop(ctx context.Context)        { s.stopTimeoutGate.Stop(ctx) }

type stopTimeoutManaged struct {
	admission *stopTimeoutGate
	client    *stopTimeoutGate
}

func (*stopTimeoutManaged) GetName() string                      { return "timeout" }
func (*stopTimeoutManaged) GetID() int                           { return 1 }
func (*stopTimeoutManaged) Start(context.Context) error          { return nil }
func (*stopTimeoutManaged) StartAdmission(context.Context) error { return nil }
func (m *stopTimeoutManaged) StopAdmission(ctx context.Context)  { m.admission.Stop(ctx) }
func (m *stopTimeoutManaged) Stop(ctx context.Context)           { m.client.Stop(ctx) }

type stopTimeoutLogger struct{ envlog.Logger }

func (stopTimeoutLogger) Warn(context.Context, string, ...envlog.Field) {}

type stopTimeoutEnvironment struct{ *ServiceApp }

func (stopTimeoutEnvironment) Log() envlog.Logger { return stopTimeoutLogger{} }

type stopTimeoutMetrics struct{ metrics.MetricsEngine }

func (stopTimeoutMetrics) Shutdown(context.Context) error { return nil }

type stopTimeoutTracing struct{ tracing.TracingEngine }

func (stopTimeoutTracing) Shutdown(context.Context) error { return nil }

type stopTimeoutLogs struct{ envlog.LogsEngine }

func (stopTimeoutLogs) Shutdown(context.Context) error { return nil }

func TestServiceStopDeadlineDoesNotReusePhaseState(t *testing.T) {
	cfg := &MockServiceConfig{}
	cfg.Services.MockService.ID = 1
	cfg.Services.MockService.Name = "shutdown-timeout"
	runtimeConfig, err := config.NewRuntimeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gates := make([]*stopTimeoutGate, 5)
	for i := range gates {
		gates[i] = &stopTimeoutGate{
			entered: make(chan struct{}), release: make(chan struct{}),
			returned: make(chan struct{}),
		}
	}
	app := &ServiceApp{
		id: 1, loader: &MockServiceLoader{},
		components: []environment.Lifecycle{gates[0]},
		managedDataConnectors: map[int]ManagedDataConnector{
			1: &stopTimeoutManaged{admission: gates[1], client: gates[4]},
		},
		delayPool: &stopTimeoutDelay{gates[2]},
		dataSinks: map[int]DataSink{
			1: &stopTimeoutSink{stopTimeoutGate: gates[3]},
		},
		metricsEngine: stopTimeoutMetrics{},
		tracingEngine: stopTimeoutTracing{},
		logsEngine:    stopTimeoutLogs{},
	}
	app.config.Store(runtimeConfig)
	app.environment = stopTimeoutEnvironment{app}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		app.Stop(ctx)
		close(done)
	}()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("shutdown did not reach the expected phase")
		}
	}
	await(gates[0].entered)
	cancel()
	await(done)
	for _, g := range gates {
		await(g.entered)
	}
	// Let old workers finish after Stop has already returned, in reverse order.
	// Before the fix these callbacks mutate the final reused WaitGroup.
	for i := len(gates) - 1; i >= 0; i-- {
		close(gates[i].release)
		await(gates[i].returned)
	}
	// Allow the surrounding deferred Done/close callbacks to run as well.
	time.Sleep(10 * time.Millisecond)
}
