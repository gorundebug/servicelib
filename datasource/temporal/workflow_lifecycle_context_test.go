package temporal

import (
	"context"
	"fmt"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type lifecycleWorkflowKey struct{}
type lifecycleWorkflowLogger struct {
	log.Logger
	received context.Context
}

func (l *lifecycleWorkflowLogger) Info(ctx context.Context, _ string, _ ...log.Field) {
	l.received = ctx
}

func workflowLifecycleContext(ctx workflow.Context) error {
	parent := workflow.WithValue(ctx, lifecycleWorkflowKey{}, "workflow-owner")
	child, cancel := workflow.WithCancel(parent)
	graph := WorkflowGraphContext(child)
	if graph.Value(lifecycleWorkflowKey{}) != "workflow-owner" || graph.Done() != nil || graph.Err() != nil || workflowExecutionContext(graph, nil) != child {
		return fmt.Errorf("lost workflow owner or introduced native wait")
	}
	wantDeadline, wantOK := child.Deadline()
	gotDeadline, gotOK := graph.Deadline()
	if gotOK != wantOK || !gotDeadline.Equal(wantDeadline) {
		return fmt.Errorf("lost workflow deadline")
	}
	cfg := &workflowSubStreamTestConfig{entry: config.SubStreamConfig{ID: 1, Name: "entry", IdService: 1, IdSource: 2}, body: config.MapStreamConfig{ID: 2, Name: "body", IdService: 1, IdSource: 1}}
	runtimeConfig, err := config.NewRuntimeConfig(cfg)
	if err != nil {
		return err
	}
	env, err := NewWorkflowEnvironment(child, runtimeConfig, 1, WorkflowTelemetryPolicy{NoopMetrics: true, NoopTracing: true})
	if err != nil {
		return err
	}
	logger := &lifecycleWorkflowLogger{}
	env.logger = logger
	if err := env.Start(graph); err != nil {
		return err
	}
	if logger.received != graph {
		return fmt.Errorf("lifecycle logger replaced caller context")
	}
	env.Stop(graph)
	cancel()
	if graph.Err() != child.Err() || graph.Err() == nil {
		return fmt.Errorf("workflow cancellation was detached")
	}
	if err := env.Start(graph); err != child.Err() {
		return fmt.Errorf("canceled lifecycle accepted: %v", err)
	}
	return nil
}

func TestWorkflowLifecycleUsesSuppliedOwner(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowLifecycleContext)
	require.NoError(t, env.GetWorkflowError())
}

func TestWorkflowLifecycleRejectsNilContext(t *testing.T) {
	require.Panics(t, func() { WorkflowGraphContext(nil) })
	require.Error(t, (&WorkflowEnvironment{}).Start(nil))
	require.Error(t, (&workflowPool{}).Start(nil))
	require.Panics(t, func() { (&WorkflowEnvironment{}).Stop(nil) })
	require.Panics(t, func() { (&workflowPool{}).Stop(nil) })
}
