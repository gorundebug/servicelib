package temporal

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
)

func TestSubmissionOptionsResolve(t *testing.T) {
	for _, test := range []struct {
		name      string
		options   SubmissionOptions
		execution api.TemporalExecutionType
		queue     string
		invalid   bool
	}{
		{name: "unchanged default", execution: api.Activity, queue: "configured"},
		{name: "explicit activity queue", execution: api.Activity, options: SubmissionOptions{TaskQueue: "selected", ScheduleToStartTimeout: time.Second}, queue: "selected"},
		{name: "explicit workflow queue", execution: api.Workflow, options: SubmissionOptions{TaskQueue: "selected"}, queue: "selected"},
		{name: "negative timeout", execution: api.Activity, options: SubmissionOptions{ScheduleToStartTimeout: -time.Second}, invalid: true},
		{name: "workflow cannot use activity timeout", execution: api.Workflow, options: SubmissionOptions{ScheduleToStartTimeout: time.Second}, invalid: true},
		{name: "blank override", execution: api.Activity, options: SubmissionOptions{TaskQueue: "  "}, invalid: true},
		{name: "nul override", execution: api.Activity, options: SubmissionOptions{TaskQueue: "queue\x00"}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, err := test.options.resolve("configured", test.execution)
			if test.invalid {
				if err == nil {
					t.Fatal("invalid explicit options were accepted")
				}
				return
			}
			if err != nil || queue != test.queue {
				t.Fatalf("queue=%q err=%v", queue, err)
			}
		})
	}
}

func TestWorkflowSubmissionOptionsSelectQueueWithoutMutatingEndpoint(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	const activityName = "submission-options-activity"
	env.RegisterActivityWithOptions(func(ctx context.Context, _ EndpointEnvelope) (EndpointResult, error) {
		return EndpointResult{Payload: []byte(activity.GetInfo(ctx).TaskQueue)}, nil
	}, activity.RegisterOptions{Name: activityName})
	parent := func(ctx workflow.Context) ([]string, error) {
		cfg := testWorkflowActivityConfig(1, "routed", activityName)
		cfg.TaskQueue = "configured"
		state := workflowSubmissionContext{workflowCtx: ctx, connector: "temporal", endpoints: map[int]workflowEndpointConfig{1: cfg}}
		envelope := EndpointEnvelope{MessageID: "routed", StreamID: "parent"}
		selected, err := submitEndpointFromWorkflowWithOptions(state, 1, envelope, SubmissionOptions{TaskQueue: "selected", ScheduleToStartTimeout: time.Second})
		if err != nil {
			return nil, err
		}
		ordinary, err := submitEndpointFromWorkflow(state, 1, envelope)
		return []string{string(selected.Payload), string(ordinary.Payload), state.endpoints[1].TaskQueue}, err
	}
	env.ExecuteWorkflow(parent)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result []string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 || result[0] != "selected" || result[1] != "configured" || result[2] != "configured" {
		t.Fatalf("unexpected actual Activity queues/config: %v", result)
	}
}
