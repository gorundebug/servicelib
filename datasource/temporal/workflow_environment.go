/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
)

// WorkflowEnvironment executes the ordinary ServiceLib graph inside one
// Temporal Workflow isolate. It owns no process resources: no sockets, config
// watcher, exporter, OS goroutine pool, or connector is started here.
type WorkflowEnvironment struct {
	runtime.ServiceApp

	workflowCtx workflow.Context
	metrics     metrics.Metrics
	logger      log.Logger
	tracing     tracing.Tracing
	taskPools   map[string]*workflowPool
	priority    map[string]*workflowPool
	parallel    int
	started     bool
}

func NewWorkflowEnvironment(
	ctx workflow.Context,
	runtimeConfig *config.RuntimeConfig,
	serviceID int,
) (*WorkflowEnvironment, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Temporal Workflow context is nil")
	}
	env := &WorkflowEnvironment{
		workflowCtx: ctx,
		metrics:     newWorkflowMetrics(ctx),
		logger:      newWorkflowLogger(ctx),
		tracing:     newWorkflowTracing(ctx),
		taskPools:   make(map[string]*workflowPool),
		priority:    make(map[string]*workflowPool),
	}
	if err := env.ServiceApp.InitIsolatedGraphRuntime(runtimeConfig, env, serviceID); err != nil {
		return nil, err
	}
	return env, nil
}

// BindRuntimeEnvironment lets a generated service wrapper provide its custom
// serde implementation while preserving this Workflow-owned runtime core. It
// must be called before graph construction.
func (env *WorkflowEnvironment) BindRuntimeEnvironment(actual runtime.RuntimeEnvironment) error {
	if env.started {
		return fmt.Errorf("cannot rebind a started Temporal Workflow environment")
	}
	serviceID := env.ServiceConfig().ID
	return env.ServiceApp.InitIsolatedGraphRuntime(env.RuntimeConfig(), actual, serviceID)
}

func (env *WorkflowEnvironment) Metrics() metrics.Metrics { return env.metrics }
func (env *WorkflowEnvironment) Tracing() tracing.Tracing { return env.tracing }
func (env *WorkflowEnvironment) Log() log.Logger          { return env.logger }
func (env *WorkflowEnvironment) ServiceDependencies() environment.ServiceDependencies {
	return nil
}
func (env *WorkflowEnvironment) MetricsEngine() metrics.MetricsEngine { return nil }
func (env *WorkflowEnvironment) TracingEngine() tracing.TracingEngine { return nil }
func (env *WorkflowEnvironment) HasCustomHTTPServer() bool            { return true }
func (env *WorkflowEnvironment) ServiceInit() error                   { return nil }
func (env *WorkflowEnvironment) ReloadConfig()                        {}
func (env *WorkflowEnvironment) Release()                             {}

func (env *WorkflowEnvironment) RegisterHTTPHandler(string, http.Handler) {
	panic("HTTP handlers are unavailable in a Temporal Workflow")
}

func (env *WorkflowEnvironment) GetSerde(reflect.Type) (serde.Serializer, error) {
	return nil, nil
}

func (env *WorkflowEnvironment) Delay(
	_ context.Context,
	duration time.Duration,
	fn func(),
) error {
	if err := workflow.Sleep(env.workflowCtx, duration); err != nil {
		return err
	}
	fn()
	return nil
}

func (env *WorkflowEnvironment) RunParallel(_ context.Context, fn func()) {
	env.parallel++
	workflow.Go(env.workflowCtx, func(workflow.Context) {
		defer func() { env.parallel-- }()
		fn()
	})
}

func (env *WorkflowEnvironment) GetTaskPool(name string) pool.TaskPool {
	if existing := env.taskPools[name]; existing != nil {
		return existing
	}
	created := env.makePool(name, false)
	env.taskPools[name] = created
	return created
}

func (env *WorkflowEnvironment) GetPriorityTaskPool(name string) pool.PriorityTaskPool {
	if existing := env.priority[name]; existing != nil {
		return workflowPriorityPool{existing}
	}
	created := env.makePool(name, true)
	env.priority[name] = created
	return workflowPriorityPool{created}
}

func (env *WorkflowEnvironment) makePool(name string, priority bool) *workflowPool {
	cfg := env.RuntimeConfig().GetPoolByName(name)
	if cfg == nil {
		panic(fmt.Sprintf("Temporal Workflow pool %q not found", name))
	}
	return &workflowPool{
		ctx: env.workflowCtx, name: name,
		executors: max(1, cfg.ExecutorsCount), priority: priority,
		metrics: makeWorkflowPoolMetrics(env.metrics, env.ServiceConfig().Name, name, priority),
	}
}

func makeWorkflowPoolMetrics(
	metricSet metrics.Metrics,
	service string,
	name string,
	priority bool,
) workflowPoolMetrics {
	prefix := "task_pool"
	description := "task pool"
	if priority {
		prefix = "priority_task_pool"
		description = "priority task pool"
	}
	scope := metricSet.Scope(prefix, metrics.Labels{"service": service, "name": name})
	return workflowPoolMetrics{
		queueLength:        mustWorkflowGauge(scope, "queue_length", description+" wait queue length"),
		executorsTarget:    mustWorkflowGauge(scope, "executors_target", "Desired number of "+description+" executors"),
		executorsAllocated: mustWorkflowGauge(scope, "executors_allocated", "Number of live "+description+" executors"),
		executorsBusy:      mustWorkflowGauge(scope, "executors_busy", "Number of "+description+" executors running callbacks"),
		tasksTotal:         mustWorkflowCounter(scope, "tasks_total", "Total number of tasks executed by "+description, nil),
		executionDuration:  mustWorkflowHistogram(scope, "task_execution_duration_seconds", "Task execution duration in seconds"),
		taskRejected:       mustWorkflowCounter(scope, "events_total", "Total number of events in "+description, metrics.Labels{"event": "task_rejected"}),
	}
}

func mustWorkflowGauge(scope metrics.MetricsScope, name, help string) metrics.Int64Gauge {
	value, err := scope.Gauge(name, help, nil)
	if err != nil {
		panic(fmt.Errorf("register Temporal Workflow gauge %q: %w", name, err))
	}
	return value
}

func mustWorkflowCounter(
	scope metrics.MetricsScope,
	name, help string,
	labels metrics.Labels,
) metrics.Int64Counter {
	value, err := scope.Counter(name, help, labels)
	if err != nil {
		panic(fmt.Errorf("register Temporal Workflow counter %q: %w", name, err))
	}
	return value
}

func mustWorkflowHistogram(
	scope metrics.MetricsScope,
	name, help string,
) metrics.Float64Histogram {
	value, err := scope.Histogram(name, help, nil)
	if err != nil {
		panic(fmt.Errorf("register Temporal Workflow histogram %q: %w", name, err))
	}
	return value
}

func (env *WorkflowEnvironment) Start(context.Context) error {
	if env.started {
		return nil
	}
	info := workflow.GetInfo(env.workflowCtx)
	env.logger.Info(
		context.Background(),
		"temporal workflow graph started",
		log.Str("workflow_id", info.WorkflowExecution.ID),
		log.Str("workflow_type", info.WorkflowType.Name),
	)
	if err := env.ServiceApp.BuildRegisteredStreams(); err != nil {
		return err
	}
	for _, item := range sortedWorkflowPools(env.taskPools) {
		if err := item.Start(context.Background()); err != nil {
			return err
		}
	}
	for _, item := range sortedWorkflowPools(env.priority) {
		if err := item.Start(context.Background()); err != nil {
			return err
		}
	}
	env.started = true
	return nil
}

func (env *WorkflowEnvironment) Stop(context.Context) {
	if !env.started {
		return
	}
	if err := workflow.Await(env.workflowCtx, func() bool {
		if env.parallel != 0 {
			return false
		}
		for _, item := range env.taskPools {
			if item.pending != 0 {
				return false
			}
		}
		for _, item := range env.priority {
			if item.pending != 0 {
				return false
			}
		}
		return true
	}); err != nil {
		panic(fmt.Errorf("wait for Temporal Workflow graph quiescence: %w", err))
	}
	for _, item := range sortedWorkflowPools(env.taskPools) {
		item.Stop(context.Background())
	}
	for _, item := range sortedWorkflowPools(env.priority) {
		item.Stop(context.Background())
	}
	env.started = false
}

func sortedWorkflowPools(values map[string]*workflowPool) []*workflowPool {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]*workflowPool, 0, len(names))
	for _, name := range names {
		result = append(result, values[name])
	}
	return result
}

type workflowTask struct {
	ctx      context.Context
	priority int
	sequence uint64
	fn       func()
}

type workflowPoolMetrics struct {
	queueLength        metrics.Int64Gauge
	executorsTarget    metrics.Int64Gauge
	executorsAllocated metrics.Int64Gauge
	executorsBusy      metrics.Int64Gauge
	tasksTotal         metrics.Int64Counter
	executionDuration  metrics.Float64Histogram
	taskRejected       metrics.Int64Counter
}

type workflowPool struct {
	ctx       workflow.Context
	name      string
	executors int
	priority  bool
	metrics   workflowPoolMetrics
	queue     []workflowTask
	sequence  uint64
	pending   int
	workers   int
	started   bool
	stopped   bool
}

func (p *workflowPool) GetName() string        { return p.name }
func (p *workflowPool) GetExecutorsCount() int { return p.executors }

func (p *workflowPool) Start(context.Context) error {
	if p.started {
		return pool.ErrPoolAlreadyStarted
	}
	if p.stopped {
		return pool.ErrPoolStopped
	}
	p.started = true
	p.workers = p.executors
	p.metrics.executorsTarget.Set(int64(p.executors))
	p.metrics.executorsAllocated.Set(int64(p.executors))
	for range p.executors {
		workflow.Go(p.ctx, p.run)
	}
	return nil
}

func (p *workflowPool) run(ctx workflow.Context) {
	defer func() {
		p.workers--
		p.metrics.executorsAllocated.Dec()
	}()
	for {
		if err := workflow.Await(ctx, func() bool {
			return len(p.queue) != 0 || p.stopped
		}); err != nil {
			return
		}
		if len(p.queue) == 0 && p.stopped {
			return
		}
		task := p.queue[0]
		p.queue = p.queue[1:]
		p.metrics.queueLength.Dec()
		p.metrics.executorsBusy.Inc()
		started := workflow.Now(ctx)
		task.fn()
		p.metrics.executorsBusy.Dec()
		p.metrics.tasksTotal.Inc(task.ctx)
		p.metrics.executionDuration.Observe(task.ctx, workflow.Now(ctx).Sub(started).Seconds())
		p.pending--
	}
}

func (p *workflowPool) Stop(context.Context) {
	if p.stopped {
		return
	}
	if err := workflow.Await(p.ctx, func() bool { return p.pending == 0 }); err != nil {
		panic(fmt.Errorf("wait for Temporal Workflow pool %q: %w", p.name, err))
	}
	p.stopped = true
	if err := workflow.Await(p.ctx, func() bool { return p.workers == 0 }); err != nil {
		panic(fmt.Errorf("stop Temporal Workflow pool %q: %w", p.name, err))
	}
}

func (p *workflowPool) AddTask(ctx context.Context, fn func()) error {
	return p.add(ctx, 0, fn)
}

func (p *workflowPool) AddTaskWithPriority(
	ctx context.Context,
	priority int,
	fn func(),
) error {
	return p.add(ctx, priority, fn)
}

func (p *workflowPool) add(ctx context.Context, priority int, fn func()) error {
	if err := ctx.Err(); err != nil {
		p.metrics.taskRejected.Inc(ctx)
		return err
	}
	if !p.started || p.stopped {
		p.metrics.taskRejected.Inc(ctx)
		return pool.ErrPoolStopped
	}
	task := workflowTask{ctx: ctx, priority: priority, sequence: p.sequence, fn: fn}
	p.sequence++
	p.queue = append(p.queue, task)
	if p.priority {
		sort.SliceStable(p.queue, func(i, j int) bool {
			if p.queue[i].priority != p.queue[j].priority {
				return p.queue[i].priority < p.queue[j].priority
			}
			return p.queue[i].sequence < p.queue[j].sequence
		})
	}
	p.pending++
	p.metrics.queueLength.Inc()
	return nil
}

type workflowPriorityPool struct{ *workflowPool }

func (p workflowPriorityPool) AddTask(ctx context.Context, priority int, fn func()) error {
	return p.AddTaskWithPriority(ctx, priority, fn)
}

var _ runtime.RuntimeEnvironment = (*WorkflowEnvironment)(nil)
var _ pool.TaskPool = (*workflowPool)(nil)
var _ pool.PriorityTaskPool = workflowPriorityPool{}
