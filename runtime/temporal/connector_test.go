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

func TestTemporalEndpointWorkflowPreservesOnDemandEnvelopeAndResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType},
	)
	const activityType = "servicegen.endpoint.1.7.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if envelope.EndpointID != 7 || envelope.ExecutionID != "job-1" || envelope.StreamID != "request-1" || envelope.Scheduled {
				t.Fatalf("unexpected endpoint envelope: %+v", envelope)
			}
			return EndpointResult{Payload: []byte("result")}, nil
		},
		activity.RegisterOptions{Name: activityType},
	)
	environment.ExecuteWorkflow(endpointWorkflowType, endpointWorkflowRequest{
		ActivityType: activityType, ActivityStartToCloseMillis: 1_000,
		MaximumAttempts: 3, Priority: 3,
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: 7, ExecutionID: "job-1", StreamID: "request-1", Payload: []byte("value"),
		},
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result EndpointResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if string(result.Payload) != "result" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestTemporalScheduleWorkflowCreatesExecutionIdentity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType},
	)
	const activityType = "servicegen.endpoint.1.8.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if !envelope.Scheduled || envelope.ScheduleID != "schedule-8" || envelope.ExecutionID == "" || envelope.StreamID != envelope.ExecutionID || envelope.ScheduledAtNano == 0 {
				t.Fatalf("unexpected scheduled envelope: %+v", envelope)
			}
			return EndpointResult{}, nil
		},
		activity.RegisterOptions{Name: activityType},
	)
	environment.ExecuteWorkflow(endpointWorkflowType, endpointWorkflowRequest{
		ActivityType: activityType, ActivityStartToCloseMillis: 1_000,
		MaximumAttempts: 3, Priority: 3,
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: 8, Scheduled: true, ScheduleID: "schedule-8",
		},
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}
