package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/stretchr/testify/require"
)

func (cronOwnerEnvironment) Log() log.Logger { return log.NoopLogger{} }

type observedCronScheduler struct {
	gocron.Scheduler
	calls    atomic.Int32
	returned chan error
}

func (s *observedCronScheduler) Shutdown() error {
	s.calls.Add(1)
	err := s.Scheduler.Shutdown()
	s.returned <- err
	return err
}

func TestCronStopRetainsRunUntilCallbacksDrain(t *testing.T) {
	for _, shortSchedulerTimeout := range []bool{false, true} {
		name := "caller-timeout"
		if shortSchedulerTimeout {
			name = "scheduler-timeout"
		}
		t.Run(name, func(t *testing.T) {
			options := []gocron.SchedulerOption{}
			if shortSchedulerTimeout {
				options = append(options, gocron.WithStopTimeout(10*time.Millisecond))
			}
			scheduler, err := gocron.NewScheduler(options...)
			require.NoError(t, err)
			observed := &observedCronScheduler{Scheduler: scheduler, returned: make(chan error, 1)}
			run := &cronRun{scheduler: observed, accepting: true}
			base, err := runtime.MakeInputDataSource(&config.CronDataConnectorConfig{ID: 1, Name: "shutdown-owner"}, cronOwnerEnvironment{})
			require.NoError(t, err)
			source := &dataSource{InputDataSource: base, run: run}
			owner, cancelOwner := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelOwner()
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseCallback()
			_, err = scheduler.NewJob(gocron.DurationJob(time.Hour), cronOccurrenceTask(owner, func(ctx context.Context) {
				run.invoke(ctx, func(actual context.Context) { entered <- actual; <-release })
			}), gocron.WithStartAt(gocron.WithStartImmediately()))
			require.NoError(t, err)
			scheduler.Start()
			select {
			case actual := <-entered:
				require.Same(t, owner, actual)
			case <-time.After(time.Second):
				t.Fatal("callback did not start")
			}
			stopCtx, cancelStop := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancelStop()
			source.Stop(stopCtx)
			require.ErrorIs(t, stopCtx.Err(), context.DeadlineExceeded)
			require.NoError(t, owner.Err(), "stop must not cancel the callback owner")
			require.ErrorContains(t, source.Start(t.Context()), "is stopping")
			run.invoke(owner, func(context.Context) { t.Error("callback admitted after stop") })
			if shortSchedulerTimeout {
				select {
				case err := <-observed.returned:
					require.ErrorIs(t, err, gocron.ErrStopJobsTimedOut)
				case <-time.After(time.Second):
					t.Fatal("scheduler did not return its timeout")
				}
				require.ErrorContains(t, source.Start(t.Context()), "is stopping")
			}
			retryDone := make(chan struct{})
			go func() { source.Stop(t.Context()); close(retryDone) }()
			select {
			case <-retryDone:
				t.Fatal("retry lost ownership of running callback")
			case <-time.After(25 * time.Millisecond):
			}
			require.EqualValues(t, 1, observed.calls.Load())
			releaseCallback()
			select {
			case <-retryDone:
			case <-time.After(time.Second):
				t.Fatal("retry did not observe callback drain")
			}
			require.NoError(t, source.Start(t.Context()), "restart must work after physical drain")
			source.Stop(t.Context())
			require.EqualValues(t, 1, observed.calls.Load())
		})
	}
}

func TestCronCancelledStopCanBeRetried(t *testing.T) {
	scheduler, err := gocron.NewScheduler()
	require.NoError(t, err)
	base, err := runtime.MakeInputDataSource(&config.CronDataConnectorConfig{ID: 2, Name: "cancelled-stop"}, cronOwnerEnvironment{})
	require.NoError(t, err)
	run := &cronRun{scheduler: scheduler, accepting: true}
	source := &dataSource{InputDataSource: base, run: run}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	go run.invoke(t.Context(), func(context.Context) { close(entered); <-release })
	<-entered
	scheduler.Start()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source.Stop(ctx)
	require.ErrorContains(t, source.Start(t.Context()), "is stopping")
	finish()
	source.Stop(t.Context())
	require.NoError(t, source.Start(t.Context()))
	source.Stop(t.Context())
}
