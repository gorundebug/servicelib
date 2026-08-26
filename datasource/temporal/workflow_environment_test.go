package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
)

func workflowW3CParent(ctx workflow.Context) (string, error) {
	_, span := newWorkflowTracing(ctx).Tracer("workflow-test").Start(
		context.Background(), "workflow-test",
	)
	defer span.End()
	return span.SpanContext().TraceID, nil
}

func TestWorkflowTracingContinuesW3CHeader(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	previousProvider := otel.GetTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(trace.NewTracerProvider(trace.WithSampler(trace.AlwaysSample())))
	t.Cleanup(func() {
		otel.SetTextMapPropagator(previousPropagator)
		otel.SetTracerProvider(previousProvider)
	})

	headers := temporalHeaderMap{}
	require.NoError(t, writeTemporalCarrier(headers, map[string]string{
		"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
	}))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetHeader(&commonpb.Header{Fields: headers})
	env.SetContextPropagators([]workflow.ContextPropagator{temporalContextPropagator{}})
	env.RegisterWorkflow(workflowW3CParent)
	env.ExecuteWorkflow(workflowW3CParent)
	require.NoError(t, env.GetWorkflowError())
	var traceID string
	require.NoError(t, env.GetWorkflowResult(&traceID))
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", traceID)
}

func workflowPoolSemantics(ctx workflow.Context) ([]int, error) {
	_ = newWorkflowLogger(ctx)
	scope := newWorkflowMetrics(ctx).Scope("workflow_test", nil)
	counter, err := scope.Counter("tasks_total", "test tasks", nil)
	if err != nil {
		return nil, err
	}

	underlying := &workflowPool{
		ctx: ctx, name: "priority", executors: 1, priority: true,
		metrics: makeWorkflowPoolMetrics(newWorkflowMetrics(ctx), "workflow-service", "priority", true),
	}
	priority := workflowPriorityPool{underlying}
	if err := priority.Start(context.Background()); err != nil {
		return nil, err
	}
	result := make([]int, 0, 3)
	for _, item := range []struct {
		priority int
		value    int
	}{{7, 7}, {2, 2}, {2, 3}} {
		item := item
		if err := priority.AddTask(context.Background(), item.priority, func() {
			result = append(result, item.value)
			counter.Inc(context.Background())
		}); err != nil {
			return nil, err
		}
	}
	priority.Stop(context.Background())
	return result, nil
}

func TestWorkflowPoolUsesDeterministicPriorityThenFIFO(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowPoolSemantics)
	env.ExecuteWorkflow(workflowPoolSemantics)
	require.NoError(t, env.GetWorkflowError())
	var result []int
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, []int{2, 3, 7}, result)
}

func workflowUnboundedPool(ctx workflow.Context) error {
	pool := &workflowPool{
		ctx: ctx, name: "unbounded", executors: 1,
		metrics: makeWorkflowPoolMetrics(newWorkflowMetrics(ctx), "workflow-service", "unbounded", false),
	}
	if err := pool.Start(context.Background()); err != nil {
		return err
	}
	if err := pool.AddTask(context.Background(), func() {}); err != nil {
		return err
	}
	if err := pool.AddTask(context.Background(), func() {}); err != nil {
		return err
	}
	pool.Stop(context.Background())
	return nil
}

func TestWorkflowTaskPoolKeepsCanonicalUnboundedQueue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowUnboundedPool)
	env.ExecuteWorkflow(workflowUnboundedPool)
	require.NoError(t, env.GetWorkflowError())
}

func workflowPoolLifecycle(ctx workflow.Context) ([]int, error) {
	pool := &workflowPool{
		ctx: ctx, name: "lifecycle", executors: 2,
		metrics: makeWorkflowPoolMetrics(newWorkflowMetrics(ctx), "workflow-service", "lifecycle", false),
	}
	if err := pool.Start(context.Background()); err != nil {
		return nil, err
	}
	active := 0
	maximumActive := 0
	completed := 0
	for range 5 {
		if err := pool.AddTaskWithContext(context.Background(), func(taskCtx context.Context) {
			active++
			maximumActive = max(maximumActive, active)
			_ = workflow.Sleep(workflowExecutionContext(taskCtx, ctx), time.Second)
			completed++
			active--
		}); err != nil {
			return nil, err
		}
	}
	pool.Stop(context.Background())
	pool.Stop(context.Background())
	return []int{maximumActive, completed, pool.pending, pool.workers}, nil
}

func TestWorkflowPoolLimitsLogicalExecutorsAndDrains(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowPoolLifecycle)
	env.ExecuteWorkflow(workflowPoolLifecycle)
	require.NoError(t, env.GetWorkflowError())
	var result []int
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, []int{2, 5, 0, 0}, result)
}

func workflowPoolRejectsCanceledAdmission(ctx workflow.Context) error {
	pool := &workflowPool{
		ctx: ctx, name: "cancel", executors: 1,
		metrics: makeWorkflowPoolMetrics(newWorkflowMetrics(ctx), "workflow-service", "cancel", false),
	}
	if err := pool.Start(context.Background()); err != nil {
		return err
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := pool.AddTask(canceled, func() {})
	pool.Stop(context.Background())
	return err
}

func TestWorkflowPoolRejectsCanceledAdmission(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowPoolRejectsCanceledAdmission)
	env.ExecuteWorkflow(workflowPoolRejectsCanceledAdmission)
	require.ErrorContains(t, env.GetWorkflowError(), context.Canceled.Error())
}

func workflowPoolTaskFailure(ctx workflow.Context) error {
	pool := &workflowPool{
		ctx: ctx, name: "failure", executors: 1,
		metrics: makeWorkflowPoolMetrics(newWorkflowMetrics(ctx), "workflow-service", "failure", false),
	}
	if err := pool.Start(context.Background()); err != nil {
		return err
	}
	if err := pool.AddTask(context.Background(), func() { panic("expected workflow pool failure") }); err != nil {
		return err
	}
	pool.Stop(context.Background())
	return nil
}

func TestWorkflowPoolTaskFailureFailsWorkflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowPoolTaskFailure)
	env.ExecuteWorkflow(workflowPoolTaskFailure)
	require.ErrorContains(t, env.GetWorkflowError(), "expected workflow pool failure")
}

func workflowPoolPropagatesContinueAsNew(ctx workflow.Context) (string, error) {
	env := &WorkflowEnvironment{
		workflowCtx: ctx,
		metrics:     newWorkflowMetrics(ctx),
		failureCh:   workflow.NewBufferedChannel(ctx, 1),
		taskPools:   make(map[string]*workflowPool),
		priority:    make(map[string]*workflowPool),
	}
	pool := &workflowPool{
		ctx: ctx, name: "continue-as-new", executors: 1,
		metrics:       makeWorkflowPoolMetrics(env.metrics, "workflow-service", "continue-as-new", false),
		recordFailure: env.recordFailure,
	}
	env.taskPools[pool.name] = pool
	if err := pool.Start(context.Background()); err != nil {
		return "", err
	}
	durable := runtime.NewDurableWorkflowContext("workflow-id", nil, nil)
	graphCtx := runtime.WithDurableCallContext(context.Background(), durable)
	if err := pool.AddTaskWithContext(graphCtx, func(taskCtx context.Context) {
		runtime.TemporalContinueAsNew(taskCtx, "next-run")
	}); err != nil {
		return "", err
	}
	err := env.AwaitWorkflowGraph(ctx)
	var continuation *runtime.TemporalContinueAsNewRequest
	if !errors.As(err, &continuation) {
		return "", err
	}
	return continuation.NextInput.(string), nil
}

func TestWorkflowPoolPropagatesContinueAsNewToWorkflowBoundary(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowPoolPropagatesContinueAsNew)
	env.ExecuteWorkflow(workflowPoolPropagatesContinueAsNew)
	require.NoError(t, env.GetWorkflowError())
	var next string
	require.NoError(t, env.GetWorkflowResult(&next))
	require.Equal(t, "next-run", next)
}
