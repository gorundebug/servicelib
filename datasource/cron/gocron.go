/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package cron

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"

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

type endpoint[R, E any] struct {
	*runtime.DataSourceEndpoint
	consumer *endpointConsumer[R, E]
	job      gocron.Job
}

type endpointConsumer[R, E any] struct {
	*runtime.DataSourceEndpointConsumer[runtime.ScheduleTrigger, R, E]
}

func (ec *endpointConsumer[R, E]) GetID() int { return ec.Endpoint().GetID() }

func (ec *endpointConsumer[R, E]) FunctionImplementation() interface{} { return nil }

func (ep *endpoint[R, E]) register(scheduler gocron.Scheduler) error {
	cfg, ok := ep.GetConfig().(*config.CronEndpointConfig)
	if !ok {
		return fmt.Errorf("invalid config type for cron endpoint %q", ep.GetName())
	}
	if !cfg.Enabled {
		return nil
	}
	definition := gocron.CronJob(cronExpression(cfg.Schedule, cfg.Timezone), false)
	options := []gocron.JobOption{gocron.WithName(ep.GetName())}
	if cfg.OverlapPolicy == api.ScheduleOverlapPolicySkip {
		options = append(options, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	var err error
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

func (ep *endpoint[R, E]) fire(ctx context.Context) {
	firedAt := time.Now().UTC()
	scheduledAt := firedAt
	if ep.job != nil {
		if lastRun, err := ep.job.LastRun(); err == nil && !lastRun.IsZero() {
			scheduledAt = lastRun.UTC()
		}
	}
	ctx = runtime.WithStreamId(ctx, runtime.NewStreamID())
	start := ep.OnRequestStart(ctx)
	ep.consumer.Consume(ctx, runtime.NewScheduleTrigger(
		ep.GetID(), ep.GetName(), scheduledAt, firedAt, runtime.ScheduleBackendLocal,
	))
	ep.OnRequestEnd(ctx, start, nil)
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
			_ = scheduler.Shutdown()
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

// MakeGocronEndpointConsumer attaches one scheduled endpoint directly to the
// existing input stream. No transport-specific business function is inserted.
func MakeGocronEndpointConsumer[R, E any](
	stream runtime.TypedInputStream[runtime.ScheduleTrigger, R, E],
) (runtime.Consumer[runtime.ScheduleTrigger], error) {
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
	consumer := &endpointConsumer[R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[runtime.ScheduleTrigger, R, E](baseEndpoint, stream),
	}
	ep := &endpoint[R, E]{DataSourceEndpoint: baseEndpoint, consumer: consumer}
	ds.AddEndpoint(ep)
	environment.RegisterEndpointConsumer(consumer)
	return consumer, nil
}
