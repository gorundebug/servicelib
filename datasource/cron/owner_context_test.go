package cron

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/stretchr/testify/require"
)

type cronOwnerEnvironment struct{ runtime.RuntimeEnvironment }

func (cronOwnerEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}

type cronOwnerEndpoint struct {
	runtime.InputEndpoint
	registered context.Context
}

func (*cronOwnerEndpoint) GetID() int { return 1 }
func (e *cronOwnerEndpoint) register(ctx context.Context, _ gocron.Scheduler, _ *cronRun) error {
	e.registered = ctx
	return nil
}

func TestCronStartPassesOwnerToEndpointRegistration(t *testing.T) {
	base, err := runtime.MakeInputDataSource(&config.CronDataConnectorConfig{ID: 1, Name: "owner-cron"}, cronOwnerEnvironment{})
	require.NoError(t, err)
	source := &dataSource{InputDataSource: base}
	endpoint := &cronOwnerEndpoint{}
	source.AddEndpoint(endpoint)
	owner, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, source.Start(owner))
	defer source.Stop(t.Context())
	require.Same(t, owner, endpoint.registered)
}

func TestCronStartRejectsMissingOrCancelledOwner(t *testing.T) {
	source := &dataSource{}
	require.ErrorContains(t, source.Start(nil), "context is nil")
	owner, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("owner shutdown")
	cancel(cause)
	require.ErrorIs(t, source.Start(owner), cause)
}

func TestCronOccurrenceInheritsOwnerMetadataDeadlineAndCancellation(t *testing.T) {
	type key struct{}
	parent, finish := context.WithTimeout(context.WithValue(t.Context(), key{}, "owner"), 5*time.Second)
	defer finish()
	owner, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	scheduler, err := gocron.NewScheduler()
	require.NoError(t, err)
	defer scheduler.Shutdown()
	entered := make(chan context.Context, 1)
	finished := make(chan error, 1)
	_, err = scheduler.NewJob(gocron.DurationJob(time.Hour), cronOccurrenceTask(owner, func(ctx context.Context) {
		entered <- ctx
		<-ctx.Done()
		finished <- context.Cause(ctx)
	}), gocron.WithStartAt(gocron.WithStartImmediately()))
	require.NoError(t, err)
	scheduler.Start()
	var received context.Context
	select {
	case received = <-entered:
	case <-parent.Done():
		t.Fatal("cron occurrence did not start")
	}
	require.Same(t, owner, received)
	require.Equal(t, "owner", received.Value(key{}))
	want, _ := owner.Deadline()
	actual, ok := received.Deadline()
	require.True(t, ok)
	require.Equal(t, want, actual)
	cause := errors.New("owner explicitly cancelled")
	cancel(cause)
	select {
	case err := <-finished:
		require.ErrorIs(t, err, cause)
	case <-parent.Done():
		t.Fatal("owner cancellation was lost")
	}
}

func TestCronSchedulerShutdownDrainsWithoutDetachingOwner(t *testing.T) {
	owner, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	scheduler, err := gocron.NewScheduler()
	require.NoError(t, err)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer scheduler.Shutdown()
	defer unblock()
	entered := make(chan context.Context, 1)
	_, err = scheduler.NewJob(gocron.DurationJob(time.Hour), cronOccurrenceTask(owner, func(ctx context.Context) {
		entered <- ctx
		<-release
	}), gocron.WithStartAt(gocron.WithStartImmediately()))
	require.NoError(t, err)
	scheduler.Start()
	var received context.Context
	select {
	case received = <-entered:
	case <-owner.Done():
		t.Fatal("cron occurrence did not start")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- scheduler.Shutdown() }()
	select {
	case err := <-stopped:
		t.Fatalf("shutdown did not drain accepted work: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.Same(t, owner, received)
	require.NoError(t, received.Err())
	unblock()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-owner.Done():
		t.Fatal("scheduler did not finish draining")
	}
}
