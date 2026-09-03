package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/store"
)

type startOrderConfig struct{ service *config.ServiceConfig }

func (c startOrderConfig) GetServices() []*config.ServiceConfig {
	return []*config.ServiceConfig{c.service}
}
func (startOrderConfig) GetStreams() []config.StreamConfig               { return nil }
func (startOrderConfig) GetDataConnectors() []config.DataConnectorConfig { return nil }
func (startOrderConfig) GetEndpoints() []config.EndpointConfig           { return nil }
func (startOrderConfig) GetPools() []*config.PoolConfig                  { return nil }
func (startOrderConfig) GetLinks() []*config.LinkConfig                  { return nil }
func (startOrderConfig) GetModules() []*config.ModuleConfig              { return nil }
func (startOrderConfig) GetTypes() []*config.TypeConfig                  { return nil }
func (startOrderConfig) GetProperty(string) interface{}                  { return nil }
func (startOrderConfig) ApplyEnvironment() error                         { return nil }

type recordingLifecycle struct {
	name   string
	events *[]string
}

func (r recordingLifecycle) Start(context.Context) error {
	*r.events = append(*r.events, "start:"+r.name)
	return nil
}
func (recordingLifecycle) Stop(context.Context) {}

type recordingManagedConnector struct{ recordingLifecycle }

func (r recordingManagedConnector) GetName() string { return r.name }
func (recordingManagedConnector) GetID() int        { return 1 }
func (r recordingManagedConnector) StartAdmission(context.Context) error {
	*r.events = append(*r.events, "start-admission:"+r.name)
	return nil
}
func (recordingManagedConnector) StopAdmission(context.Context) {}

type recordingStorage struct{ recordingLifecycle }

var _ store.Storage = (*recordingStorage)(nil)

type recordingDelayPool struct{ recordingLifecycle }

func (recordingDelayPool) Delay(context.Context, time.Duration, func()) error { return nil }

var _ environment.DelayPool = (*recordingDelayPool)(nil)

type recordingDataSink struct {
	*OutputDataSink
	recordingLifecycle
}

func (r recordingDataSink) Start(ctx context.Context) error { return r.recordingLifecycle.Start(ctx) }
func (r recordingDataSink) Stop(ctx context.Context)        { r.recordingLifecycle.Stop(ctx) }

var _ DataSink = (*recordingDataSink)(nil)

type recordingDataSource struct {
	*InputDataSource
	recordingLifecycle
}

func (r recordingDataSource) Start(ctx context.Context) error { return r.recordingLifecycle.Start(ctx) }
func (r recordingDataSource) Stop(ctx context.Context)        { r.recordingLifecycle.Stop(ctx) }

var _ DataSource = (*recordingDataSource)(nil)

func TestServiceStartsManagedAdmissionAfterDownstreamGraph(t *testing.T) {
	events := make([]string, 0, 7)
	service := &config.ServiceConfig{ID: 1, Name: "start-order"}
	runtimeConfig, err := config.NewRuntimeConfig(startOrderConfig{service: service})
	if err != nil {
		t.Fatal(err)
	}
	app := &ServiceApp{}
	if err := app.InitIsolatedGraphRuntime(runtimeConfig, app, service.ID); err != nil {
		t.Fatal(err)
	}
	app.managedDataConnectors[1] = &recordingManagedConnector{
		recordingLifecycle{name: "managed", events: &events},
	}
	app.storages = []store.Storage{&recordingStorage{
		recordingLifecycle{name: "storage", events: &events},
	}}
	app.delayPool = &recordingDelayPool{
		recordingLifecycle{name: "delay", events: &events},
	}
	app.components = []environment.Lifecycle{recordingLifecycle{name: "component", events: &events}}
	app.dataSinks[1] = &recordingDataSink{
		recordingLifecycle: recordingLifecycle{name: "sink", events: &events},
	}
	app.dataSources[1] = &recordingDataSource{
		recordingLifecycle: recordingLifecycle{name: "source", events: &events},
	}
	app.taskPools = map[string]pool.TaskPool{}
	app.priorityTaskPools = map[string]pool.PriorityTaskPool{}

	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"start:managed",
		"start:storage",
		"start:delay",
		"start:component",
		"start:sink",
		"start-admission:managed",
		"start:source",
	}
	if len(events) != len(want) {
		t.Fatalf("start events = %v, want %v", events, want)
	}
	for index := range want {
		if events[index] != want[index] {
			t.Fatalf("start events = %v, want %v", events, want)
		}
	}
}
