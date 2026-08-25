package temporal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func workflowPoolSemantics(ctx workflow.Context) ([]int, error) {
	_ = newWorkflowLogger(ctx)
	scope := newWorkflowMetrics(ctx).Scope("workflow_test", nil)
	counter, err := scope.Counter("tasks_total", "test tasks", nil)
	if err != nil {
		return nil, err
	}

	underlying := &workflowPool{
		ctx: ctx, name: "priority", executors: 1, capacity: 8, priority: true,
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

func workflowBoundedPool(ctx workflow.Context) error {
	pool := &workflowPool{
		ctx: ctx, name: "bounded", executors: 1, capacity: 1,
	}
	if err := pool.Start(context.Background()); err != nil {
		return err
	}
	if err := pool.AddTask(context.Background(), func() {}); err != nil {
		return err
	}
	return pool.AddTask(context.Background(), func() {})
}

func TestWorkflowPoolRejectsQueueOverflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowBoundedPool)
	env.ExecuteWorkflow(workflowBoundedPool)
	require.ErrorContains(t, env.GetWorkflowError(), "queue is full")
}
