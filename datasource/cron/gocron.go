/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package cron

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	robfigcron "github.com/robfig/cron/v3"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

type inputEndpoint interface {
	runtime.InputEndpoint
	register(gocron.Scheduler) error
}

type dataSource struct {
	*runtime.InputDataSource
	mu        sync.Mutex
	scheduler gocron.Scheduler
	started   bool
}

type endpoint[T, R, E any] struct {
	*runtime.DataSourceEndpoint
	consumer *endpointConsumer[T, R, E]
	job      gocron.Job
	tracker  *portableCron
}

type endpointConsumer[T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	function  runtime.ScheduleEndpointFunction[T]
	out       runtime.Collect[T]
	hasResult bool
	mu        sync.Mutex
	pending   map[string]chan struct{}
}

type resultConsumer[R any] struct {
	consume func(context.Context, R)
}

func (consumer *resultConsumer[R]) Consume(ctx context.Context, value R) {
	consumer.consume(ctx, value)
}

func (ec *endpointConsumer[T, R, E]) GetID() int { return ec.Endpoint().GetID() }

func (ec *endpointConsumer[T, R, E]) endpoint() runtime.InputEndpoint {
	if ec.DataSourceEndpointConsumer == nil {
		return nil
	}
	return ec.Endpoint()
}

func (ec *endpointConsumer[T, R, E]) endpointName() string {
	if endpoint := ec.endpoint(); endpoint != nil {
		return endpoint.GetName()
	}
	return "cron"
}

func (ec *endpointConsumer[T, R, E]) FunctionImplementation() interface{} {
	return ec.function
}

func (ec *endpointConsumer[T, R, E]) onTrigger(
	ctx context.Context,
	trigger runtime.ScheduleTrigger,
) error {
	if !ec.hasResult {
		ec.function.OnTrigger(ctx, trigger, ec.out)
		return nil
	}
	streamID, ok := runtime.StreamIdFromContext(ctx)
	if !ok || streamID.GetID() == "" {
		return fmt.Errorf("cron endpoint %q activation has no stream id", ec.endpointName())
	}
	done := make(chan struct{}, 1)
	id := streamID.GetID()
	ec.mu.Lock()
	if _, exists := ec.pending[id]; exists {
		ec.mu.Unlock()
		return fmt.Errorf("cron endpoint %q already has an active execution %q", ec.endpointName(), id)
	}
	ec.pending[id] = done
	ec.mu.Unlock()
	if endpoint := ec.endpoint(); endpoint != nil {
		endpoint.OnPendingAdd(ctx, id)
	}
	defer func() {
		ec.mu.Lock()
		delete(ec.pending, id)
		ec.mu.Unlock()
		if endpoint := ec.endpoint(); endpoint != nil {
			endpoint.OnPendingRemove(ctx, id)
		}
	}()

	ec.function.OnTrigger(ctx, trigger, ec.out)
	// A scheduled input with a result stream has the same contract as the
	// other datasource endpoints: the activation is complete only when the
	// correlated terminal result reaches the input stream. In particular this
	// keeps the cron job active while pooled/parallel graph work is outstanding.
	<-done
	return nil
}

func (ec *endpointConsumer[T, R, E]) consumeResult(ctx context.Context, _ R) {
	streamID, ok := runtime.StreamIdFromContext(ctx)
	if !ok || streamID.GetID() == "" {
		if endpoint := ec.endpoint(); endpoint != nil {
			endpoint.OnMissingStreamID(ctx)
		}
		return
	}
	id := streamID.GetID()
	ec.mu.Lock()
	done := ec.pending[id]
	ec.mu.Unlock()
	if done == nil {
		if endpoint := ec.endpoint(); endpoint != nil {
			endpoint.OnLateResult(ctx, id)
		}
		return
	}
	select {
	case done <- struct{}{}:
	default:
		if endpoint := ec.endpoint(); endpoint != nil {
			endpoint.OnDuplicateMessageID(ctx, id, id)
		}
	}
}

func (ep *endpoint[T, R, E]) register(scheduler gocron.Scheduler) error {
	cfg, ok := ep.GetConfig().(*config.CronEndpointConfig)
	if !ok {
		return fmt.Errorf("invalid config type for cron endpoint %q", ep.GetName())
	}
	if !cfg.Enabled {
		return nil
	}
	location, err := time.LoadLocation(strings.TrimSpace(cfg.Timezone))
	if err != nil {
		return fmt.Errorf("load timezone for cron endpoint %q: %w", ep.GetName(), err)
	}
	tracker := &portableCron{location: location}
	ep.tracker = tracker
	definition := gocron.CronJob(strings.TrimSpace(cfg.Schedule), false)
	options := []gocron.JobOption{
		gocron.WithName(ep.GetName()),
		gocron.WithCronImplementation(tracker),
	}
	if cfg.OverlapPolicy == api.ScheduleOverlapPolicySkip {
		options = append(options, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	ep.job, err = scheduler.NewJob(definition, gocron.NewTask(func(ctx context.Context) {
		ep.fire(ctx)
	}), options...)
	if err != nil {
		return fmt.Errorf("register cron endpoint %q: %w", ep.GetName(), err)
	}
	return nil
}

func cronExpression(expression, timezone string) string {
	return "CRON_TZ=" + strings.TrimSpace(timezone) + " " + strings.TrimSpace(expression)
}

func (ep *endpoint[T, R, E]) fire(ctx context.Context) {
	// gocron cancels its executor context as part of Shutdown. That cancellation
	// closes admission, but an occurrence already accepted by ServiceLib must
	// drain through its configured result boundary just like an RPC request.
	ctx = context.WithoutCancel(ctx)
	firedAt := time.Now().UTC()
	if ep.tracker == nil {
		return
	}
	scheduledAt, count := ep.tracker.consumeDue()
	if count == 0 || (count > 1 && ep.GetConfig().(*config.CronEndpointConfig).MissedRunPolicy == api.ScheduleMissedRunPolicySkip) {
		return
	}
	ctx = runtime.WithStreamId(ctx, runtime.NewStreamID())
	ctx = runtime.ApplyDataSourceEndpointTracing(ctx, ep.GetRuntimeEnvironment(), ep.GetID())
	start := ep.OnRequestStart(ctx)
	err := ep.consumer.onTrigger(ctx, runtime.NewScheduleTrigger(
		ep.GetID(), ep.GetName(), scheduledAt, firedAt, runtime.ScheduleBackendLocal,
	))
	ep.OnRequestEnd(ctx, start, err)
}

// portableCron lets gocron retain ownership of scheduling, overlap control,
// and lifecycle while exposing the exact logical occurrence that caused a
// callback. gocron deliberately records wall-clock execution time as LastRun;
// that value cannot be used for a stable trigger identity. The Cron interface
// is the library-supported extension point for retaining its calculated
// occurrence without reimplementing a scheduler.
type portableCron struct {
	mu           sync.Mutex
	location     *time.Location
	schedule     robfigcron.Schedule
	lastReturned time.Time
	lastAdvanced time.Time
	due          []time.Time
}

func (c *portableCron) IsValid(crontab string, _ *time.Location, now time.Time) error {
	schedule, err := robfigcron.ParseStandard(cronExpression(crontab, c.location.String()))
	if err != nil {
		return err
	}
	if schedule.Next(now).IsZero() {
		return fmt.Errorf("cron expression has no next occurrence")
	}
	c.mu.Lock()
	c.schedule = schedule
	c.lastReturned = time.Time{}
	c.lastAdvanced = time.Time{}
	c.due = nil
	c.mu.Unlock()
	return nil
}

func (c *portableCron) Next(lastRun time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schedule == nil {
		return time.Time{}
	}
	if !c.lastReturned.IsZero() && lastRun.Equal(c.lastReturned) && !lastRun.Equal(c.lastAdvanced) {
		c.due = append(c.due, c.lastReturned.UTC())
		c.lastAdvanced = lastRun
	}
	next := c.schedule.Next(lastRun)
	for !next.IsZero() && !portableScheduledTime(next, c.location) {
		next = c.schedule.Next(next)
	}
	c.lastReturned = next
	return next
}

func (c *portableCron) consumeDue() (time.Time, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.due) == 0 {
		return time.Time{}, 0
	}
	latest := c.due[len(c.due)-1]
	count := len(c.due)
	c.due = nil
	return latest, count
}

// portableScheduledTime rejects the second instant of an ambiguous local wall
// time. gocron/robfig already skips nonexistent spring-forward wall times, so
// this small adapter establishes the shared "skip gap, first fold once"
// contract without parsing or calculating cron expressions itself.
func portableScheduledTime(value time.Time, location *time.Location) bool {
	local := value.In(location)
	_, beforeOffset := value.Add(-24 * time.Hour).In(location).Zone()
	_, afterOffset := value.Add(24 * time.Hour).In(location).Zone()
	if beforeOffset <= afterOffset {
		return true
	}
	earlier := value.Add(-time.Duration(beforeOffset-afterOffset) * time.Second).In(location)
	return local.Year() != earlier.Year() ||
		local.Month() != earlier.Month() ||
		local.Day() != earlier.Day() ||
		local.Hour() != earlier.Hour() ||
		local.Minute() != earlier.Minute() ||
		local.Second() != earlier.Second()
}

func (ds *dataSource) Start(context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.started {
		return nil
	}
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("create cron scheduler %q: %w", ds.GetName(), err)
	}
	endpoints := ds.GetEndpoints()
	for index := 0; index < endpoints.Len(); index++ {
		if err := endpoints.At(index).(inputEndpoint).register(scheduler); err != nil {
			if shutdownErr := scheduler.Shutdown(); shutdownErr != nil {
				return errors.Join(err, fmt.Errorf("shutdown cron scheduler after registration failure: %w", shutdownErr))
			}
			return err
		}
	}
	scheduler.Start()
	ds.scheduler = scheduler
	ds.started = true
	return nil
}

func (ds *dataSource) Stop(ctx context.Context) {
	ds.mu.Lock()
	if !ds.started {
		ds.mu.Unlock()
		return
	}
	scheduler := ds.scheduler
	ds.scheduler = nil
	ds.started = false
	ds.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- scheduler.Shutdown() }()
	select {
	case <-done:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

func getOrCreateDataSource(id int, environment runtime.RuntimeEnvironment) (runtime.DataSource, error) {
	if existing := environment.GetDataSource(id); existing != nil {
		if existing.GetConfig().GetType() != api.DataConnectorTypeCron {
			return nil, fmt.Errorf("data source id=%d is not a Cron connector", id)
		}
		return existing, nil
	}
	connectorConfig := environment.RuntimeConfig().GetDataConnectorByID(id)
	if connectorConfig == nil {
		return nil, fmt.Errorf("config for cron data source with id=%d not found", id)
	}
	if _, ok := connectorConfig.(*config.CronDataConnectorConfig); !ok {
		return nil, fmt.Errorf("invalid config type for cron data source %q", connectorConfig.GetName())
	}
	base, err := runtime.MakeInputDataSource(connectorConfig, environment)
	if err != nil {
		return nil, err
	}
	ds := &dataSource{InputDataSource: base}
	environment.AddDataSource(ds)
	return ds, nil
}

// MakeGocronEndpointConsumer binds one scheduled endpoint function to the
// existing typed input stream.
func MakeGocronEndpointConsumer[T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	function runtime.ScheduleEndpointFunction[T],
) (runtime.Consumer[T], error) {
	if function == nil {
		return nil, fmt.Errorf("cron endpoint function is nil for stream %q", stream.GetName())
	}
	environment := stream.GetRuntimeEnvironment()
	endpointConfig := environment.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId())
	cfg, ok := endpointConfig.(*config.CronEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for cron endpoint id=%d", stream.GetEndpointId())
	}
	ds, err := getOrCreateDataSource(cfg.IdDataConnector, environment)
	if err != nil {
		return nil, err
	}
	if ds.GetEndpoint(cfg.ID) != nil {
		return nil, fmt.Errorf("cron endpoint %q already exists", cfg.Name)
	}
	baseEndpoint, err := runtime.MakeDataSourceEndpoint(ds, cfg.ID, environment)
	if err != nil {
		return nil, err
	}
	consumer := &endpointConsumer[T, R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](baseEndpoint, stream),
		function:                   function,
		hasResult:                  stream.GetResultStream() != nil,
		pending:                    make(map[string]chan struct{}),
	}
	consumer.out = runtime.CollectFunc[T](consumer.Consume)
	if consumer.hasResult {
		stream.SetResultConsumer(&resultConsumer[R]{consume: consumer.consumeResult})
	}
	ep := &endpoint[T, R, E]{DataSourceEndpoint: baseEndpoint, consumer: consumer}
	ds.AddEndpoint(ep)
	environment.RegisterEndpointConsumer(consumer)
	return consumer, nil
}
