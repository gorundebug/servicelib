package temporal

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
)

func TestDurableWorkflowInvokesRegisteredActivityWithUnchangedEnvelope(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType},
	)
	const activityType = "servicegen.durable.1.2.3.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope runtime.DurableEnvelope) error {
			if envelope.CallID != "logical-call" || envelope.From != 2 || envelope.To != 3 {
				t.Fatalf("unexpected durable envelope: %+v", envelope)
			}
			return nil
		},
		activity.RegisterOptions{Name: activityType},
	)
	environment.ExecuteWorkflow(durableWorkflowType, durableWorkflowRequest{
		ActivityType: activityType, ActivityStartToCloseMillis: 1_000,
		MaximumAttempts: 3, Priority: 3,
		Envelope: runtime.DurableEnvelope{
			Version: 1, From: 2, To: 3, CallID: "logical-call", Payload: []byte("value"),
		},
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}
