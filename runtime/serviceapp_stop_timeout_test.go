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

// Each phase outlives the caller deadline. Timeout may return to the caller,
// but later teardown must wait for the preceding owners to finish.
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

func TestServiceStopDeadlinePreservesPhaseOwnership(t *testing.T) {
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
	operationRelease := make(chan struct{})
	finalizerEntered := make(chan struct{})
	finalizerRelease := make(chan struct{})
	app.RunParallel(t.Context(), func() {
		<-operationRelease
		close(finalizerEntered)
		<-finalizerRelease
	})
	ctx, cancel := context.WithCancel(t.Context())
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
	// Retrying Stop joins the original shutdown; it must not call Stop twice.
	retryDone := make(chan struct{})
	go func() { app.Stop(t.Context()); close(retryDone) }()
	for i, gate := range gates {
		await(gate.entered)
		if i+1 < len(gates) {
			select {
			case <-gates[i+1].entered:
				t.Errorf("phase %d started before phase %d finished", i+1, i)
			case <-time.After(10 * time.Millisecond):
			}
		}
		close(gate.release)
		await(gate.returned)
		if i == 2 { // Pools are done; accepted graph work still owns clients.
			for _, release := range []chan struct{}{operationRelease, finalizerRelease} {
				select {
				case <-gates[3].entered:
					t.Error("sink teardown started while graph/finalizer was active")
				case <-time.After(10 * time.Millisecond):
				}
				close(release)
				if release == operationRelease {
					await(finalizerEntered)
				}
			}
		}
	}
	await(retryDone)
}
