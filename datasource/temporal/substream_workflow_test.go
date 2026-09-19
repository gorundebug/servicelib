package temporal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/operators"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workflowSubStreamTestConfig struct {
	config.Config
	entry config.SubStreamConfig
	body  config.MapStreamConfig
}

func (*workflowSubStreamTestConfig) GetServices() []*config.ServiceConfig {
	return []*config.ServiceConfig{{ID: 1, Name: "substream-workflow"}}
}
func (c *workflowSubStreamTestConfig) GetStreams() []config.StreamConfig {
	return []config.StreamConfig{&c.entry, &c.body}
}
func (*workflowSubStreamTestConfig) GetDataConnectors() []config.DataConnectorConfig { return nil }
func (*workflowSubStreamTestConfig) GetEndpoints() []config.EndpointConfig           { return nil }
func (*workflowSubStreamTestConfig) GetPools() []*config.PoolConfig                  { return nil }
func (*workflowSubStreamTestConfig) GetLinks() []*config.LinkConfig                  { return nil }
func (*workflowSubStreamTestConfig) GetModules() []*config.ModuleConfig              { return nil }
func (*workflowSubStreamTestConfig) GetTypes() []*config.TypeConfig                  { return nil }

type workflowSubStreamGraph struct {
	env   *WorkflowEnvironment
	entry runtime.TypedSubStream[int, int]
	ctx   context.Context
}

type workflowSubStreamCallerKey struct{}

func newWorkflowSubStreamGraph(
	ctx workflow.Context,
	body func(*WorkflowEnvironment, context.Context, int, runtime.Collect[int]),
) (*workflowSubStreamGraph, error) {
	cfg := &workflowSubStreamTestConfig{
		entry: config.SubStreamConfig{ID: 1, Name: "Calculate", IdService: 1, IdSource: 2},
		body:  config.MapStreamConfig{ID: 2, Name: "CalculateBody", IdService: 1, IdSource: 1},
	}
	rc, err := config.NewRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	env, err := NewWorkflowEnvironment(ctx, rc, 1)
	if err != nil {
		return nil, err
	}
	entry, err := operators.MakeSubStream[int, int](&cfg.entry, env)
	if err != nil {
		return nil, err
	}
	result, err := operators.MakeMapStream[int, int](&cfg.body, entry,
		operators.MapHandler[int, int](func(callCtx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
			body(env, callCtx, value, out)
		}))
	if err != nil {
		return nil, err
	}
	if err := entry.SetSource(result); err != nil {
		return nil, err
	}
	if err := entry.Build(); err != nil {
		return nil, err
	}
	durable := runtime.NewDurableWorkflowContext("substream-request", func(callCtx context.Context, delay time.Duration) error {
		return workflow.Sleep(workflowExecutionContext(callCtx, ctx), delay)
	}, func() bool { return workflow.IsReplaying(ctx) })
	graphCtx := runtime.WithDurableCallContext(context.Background(), durable)
	graphCtx = context.WithValue(graphCtx, workflowSubStreamCallerKey{}, "outer-caller")
	return &workflowSubStreamGraph{env: env, entry: entry, ctx: withWorkflowExecutionContext(graphCtx, ctx)}, nil
}

func workflowSubStreamParallelCollectors(ctx workflow.Context) ([]int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, callCtx context.Context, _ int, out runtime.Collect[int]) {
		for value := 1; value <= 3; value++ {
			env.RunParallelWithContext(callCtx, func(resultCtx context.Context) { out.Out(resultCtx, value) })
		}
	})
	if err != nil {
		return nil, err
	}
	active, maximum, calls, sum := 0, 0, 0, 0
	err = g.entry.Consume(g.ctx, 0, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
		if returnCtx.Value(workflowSubStreamCallerKey{}) != "outer-caller" {
			panic("collector lost caller values")
		}
		active++
		maximum = max(maximum, active)
		defer func() { active-- }()
		if err := workflow.Sleep(workflowExecutionContext(returnCtx, ctx), time.Second); err != nil {
			panic(err)
		}
		calls++
		sum += value
		return calls == 2
	}))
	if err != nil {
		return nil, err
	}
	if err := g.env.AwaitWorkflowGraph(ctx); err != nil {
		return nil, err
	}
	return []int{calls, maximum, sum}, nil
}

func workflowSubStreamRecursive(ctx workflow.Context) (int, error) {
	var g *workflowSubStreamGraph
	var err error
	g, err = newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, callCtx context.Context, value int, out runtime.Collect[int]) {
		env.RunParallelWithContext(callCtx, func(resultCtx context.Context) {
			if value == 0 {
				out.Out(resultCtx, 1)
				return
			}
			if err := g.entry.Consume(resultCtx, value-1, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, result int) bool {
				out.Out(returnCtx, result+1)
				return true
			})); err != nil {
				panic(err)
			}
		})
	})
	if err != nil {
		return 0, err
	}
	got := 0
	err = g.entry.Consume(g.ctx, 4, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, result int) bool {
		if err := workflow.Sleep(workflowExecutionContext(returnCtx, ctx), time.Second); err != nil {
			panic(err)
		}
		got = result
		return true
	}))
	if err != nil {
		return 0, err
	}
	return got, g.env.AwaitWorkflowGraph(ctx)
}

func workflowSubStreamFromCollector(ctx workflow.Context) (int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, callCtx context.Context, value int, out runtime.Collect[int]) {
		env.RunParallelWithContext(callCtx, func(resultCtx context.Context) { out.Out(resultCtx, value*2) })
	})
	if err != nil {
		return 0, err
	}
	got := 0
	err = g.entry.Consume(g.ctx, 10, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, result int) bool {
		if err := g.entry.Consume(returnCtx, result+1, runtime.SubStreamCollectorFunc[int](func(innerCtx context.Context, value int) bool {
			if err := workflow.Sleep(workflowExecutionContext(innerCtx, ctx), time.Second); err != nil {
				panic(err)
			}
			got = value
			return true
		})); err != nil {
			panic(err)
		}
		return true
	}))
	if err != nil {
		return 0, err
	}
	return got, g.env.AwaitWorkflowGraph(ctx)
}

func workflowSubStreamConcurrentCalls(ctx workflow.Context) ([]int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, callCtx context.Context, value int, out runtime.Collect[int]) {
		env.RunParallelWithContext(callCtx, func(resultCtx context.Context) {
			if err := workflow.Sleep(workflowExecutionContext(resultCtx, ctx), time.Second); err != nil {
				panic(err)
			}
			out.Out(resultCtx, value*2)
		})
	})
	if err != nil {
		return nil, err
	}
	results := make([]int, 8)
	for i := range results {
		g.env.RunParallelWithContext(g.ctx, func(callCtx context.Context) {
			if err := g.entry.Consume(callCtx, i+1, runtime.SubStreamCollectorFunc[int](func(_ context.Context, value int) bool {
				results[i] = value
				return true
			})); err != nil {
				panic(err)
			}
		})
	}
	return results, g.env.AwaitWorkflowGraph(ctx)
}

func workflowSubStreamCanceled(ctx workflow.Context) error {
	callCtx, cancel := workflow.WithCancel(ctx)
	g, err := newWorkflowSubStreamGraph(callCtx, func(*WorkflowEnvironment, context.Context, int, runtime.Collect[int]) {})
	if err != nil {
		return err
	}
	workflow.Go(ctx, func(timerCtx workflow.Context) {
		_ = workflow.Sleep(timerCtx, time.Second)
		cancel()
	})
	return g.entry.Consume(g.ctx, 1, runtime.SubStreamCollectorFunc[int](func(context.Context, int) bool {
		panic("canceled call must not invoke collector")
	}))
}

func workflowSubStreamCancelActiveCollector(ctx workflow.Context, complete bool) ([]bool, error) {
	callCtx, cancel := workflow.WithCancel(ctx)
	g, err := newWorkflowSubStreamGraph(callCtx, func(env *WorkflowEnvironment, graphCtx context.Context, value int, out runtime.Collect[int]) {
		env.RunParallelWithContext(graphCtx, func(resultCtx context.Context) { out.Out(resultCtx, value) })
	})
	if err != nil {
		return nil, err
	}
	started, finished, callbackCanceled := false, false, false
	workflow.Go(ctx, func(timerCtx workflow.Context) {
		_ = workflow.Await(timerCtx, func() bool { return started })
		_ = workflow.Sleep(timerCtx, time.Second)
		cancel()
	})
	err = g.entry.Consume(g.ctx, 1, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, _ int) bool {
		started = true
		err := workflow.Sleep(workflowExecutionContext(returnCtx, callCtx), time.Hour)
		callbackCanceled = sdktemporal.IsCanceledError(err)
		finished = true
		return complete
	}))
	return []bool{finished, callbackCanceled, err == nil, sdktemporal.IsCanceledError(err)}, nil
}

func workflowSubStreamDeadline(ctx workflow.Context) (int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(*WorkflowEnvironment, context.Context, int, runtime.Collect[int]) {})
	if err != nil {
		return 0, err
	}
	start := workflow.Now(ctx)
	deadlineCtx := workflowDeadlineContext{Context: g.ctx, deadline: start.Add(3 * time.Second)}
	err = g.entry.Consume(deadlineCtx, 1, runtime.SubStreamCollectorFunc[int](func(context.Context, int) bool { return true }))
	if !errors.Is(err, context.DeadlineExceeded) {
		return 0, fmt.Errorf("expected deadline exceeded, got %v", err)
	}
	return int(workflow.Now(ctx).Sub(start) / time.Second), nil
}

func workflowSubStreamFailure(ctx workflow.Context, continueAsNew bool) (int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, graphCtx context.Context, _ int, _ runtime.Collect[int]) {
		env.RunParallelWithContext(graphCtx, func(resultCtx context.Context) {
			if continueAsNew {
				runtime.TemporalContinueAsNew(resultCtx, 23)
			}
			panic(errors.New("substream graph failure"))
		})
	})
	if err != nil {
		return 0, err
	}
	err = g.entry.Consume(g.ctx, 1, runtime.SubStreamCollectorFunc[int](func(context.Context, int) bool { return true }))
	if continueAsNew {
		var continuation *runtime.TemporalContinueAsNewRequest
		if !errors.As(err, &continuation) {
			return 0, fmt.Errorf("expected Continue-As-New, got %v", err)
		}
		return continuation.NextInput.(int), nil
	}
	return 0, err
}

func TestWorkflowSubStreamParallelCollectors(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamParallelCollectors)
	require.NoError(t, env.GetWorkflowError())
	var got []int
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, []int{2, 1, 3}, got)
}

func TestWorkflowSubStreamNestedCalls(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   func(workflow.Context) (int, error)
		want int
	}{{"recursive_body", workflowSubStreamRecursive, 5}, {"nested_collector", workflowSubStreamFromCollector, 42}} {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.ExecuteWorkflow(test.fn)
			require.NoError(t, env.GetWorkflowError())
			var got int
			require.NoError(t, env.GetWorkflowResult(&got))
			require.Equal(t, test.want, got)
		})
	}
}

func TestWorkflowSubStreamConcurrentCalls(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamConcurrentCalls)
	require.NoError(t, env.GetWorkflowError())
	var got []int
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, []int{2, 4, 6, 8, 10, 12, 14, 16}, got)
}

func TestWorkflowSubStreamCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamCanceled)
	require.True(t, sdktemporal.IsCanceledError(env.GetWorkflowError()), "%v", env.GetWorkflowError())
}

func TestWorkflowSubStreamCancellationDrainsActiveCollector(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.ExecuteWorkflow(workflowSubStreamCancelActiveCollector, complete)
			require.NoError(t, env.GetWorkflowError())
			var got []bool
			require.NoError(t, env.GetWorkflowResult(&got))
			require.Equal(t, []bool{true, true, complete, !complete}, got)
		})
	}
}

func TestWorkflowSubStreamDeadline(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamDeadline)
	require.NoError(t, env.GetWorkflowError())
	var elapsed int
	require.NoError(t, env.GetWorkflowResult(&elapsed))
	require.Equal(t, 3, elapsed)
}

func TestWorkflowSubStreamGraphFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamFailure, false)
	require.ErrorContains(t, env.GetWorkflowError(), "substream graph failure")
}

func TestWorkflowSubStreamContinueAsNew(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowSubStreamFailure, true)
	require.NoError(t, env.GetWorkflowError())
	var next int
	require.NoError(t, env.GetWorkflowResult(&next))
	require.Equal(t, 23, next)
}

const workflowSubStreamActivityName = "substream-double"
const workflowSubStreamReplayName = "substream-replay"

func workflowSubStreamActivity(ctx workflow.Context) (int, error) {
	g, err := newWorkflowSubStreamGraph(ctx, func(env *WorkflowEnvironment, graphCtx context.Context, value int, out runtime.Collect[int]) {
		env.RunParallelWithContext(graphCtx, func(resultCtx context.Context) {
			activityCtx := workflow.WithActivityOptions(workflowExecutionContext(resultCtx, ctx), workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
			})
			var result int
			if err := workflow.ExecuteActivity(activityCtx, workflowSubStreamActivityName, value).Get(activityCtx, &result); err != nil {
				panic(err)
			}
			out.Out(resultCtx, result)
		})
	})
	if err != nil {
		return 0, err
	}
	got := 0
	err = g.entry.Consume(g.ctx, 7, runtime.SubStreamCollectorFunc[int](func(_ context.Context, value int) bool {
		got = value
		return true
	}))
	if err != nil {
		return 0, err
	}
	return got, g.env.AwaitWorkflowGraph(ctx)
}

// A fixed complete history checks replay through an actual Activity boundary,
// not merely two executions of the testsuite's simulated Workflow environment.
func TestWorkflowSubStreamReplay(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	input, err := dc.ToPayloads(7)
	require.NoError(t, err)
	output, err := dc.ToPayloads(14)
	require.NoError(t, err)
	events := []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: workflowSubStreamReplayName},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "substream-test"},
			}}},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}}},
		{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: 2}}},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: 2, StartedEventId: 3}}},
		{EventId: 5, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId: "5", ActivityType: &commonpb.ActivityType{Name: workflowSubStreamActivityName},
				TaskQueue: &taskqueuepb.TaskQueue{Name: "substream-test"}, Input: input,
				StartToCloseTimeout: durationpb.New(time.Minute), WorkflowTaskCompletedEventId: 4,
			}}},
		{EventId: 6, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: 5}}},
		{EventId: 7, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: 5, StartedEventId: 6, Result: output}}},
		{EventId: 8, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}}},
		{EventId: 9, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: 8}}},
		{EventId: 10, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: 8, StartedEventId: 9}}},
		{EventId: 11, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{WorkflowTaskCompletedEventId: 10, Result: output}}},
	}
	for i, event := range events {
		event.EventTime = timestamppb.New(time.Date(2026, 9, 18, 12, 0, i, 0, time.UTC))
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(workflowSubStreamActivity, workflow.RegisterOptions{Name: workflowSubStreamReplayName})
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: events}))
	// Prove the fixture actually checks commands against recorded history.
	events[4].GetActivityTaskScheduledEventAttributes().ActivityType.Name = "unexpected-activity"
	require.Error(t, replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: events}))
}
