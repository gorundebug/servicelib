package temporal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
)

func TestTemporalEndpointActivityProvidesDurableContextAndHeartbeat(t *testing.T) {
	heartbeats := make(chan any, 1)
	registration := endpointRegistration{
		id: 7,
		handler: func(ctx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if envelope.FiredAtNano == 0 {
				t.Fatal("endpoint Activity did not record its actual fire time")
			}
			if _, ok := runtime.DurableCallContextFromContext(ctx); !ok {
				t.Fatal("endpoint handler did not receive a processing-side durable context")
			}
			if err := runtime.DurableCallHeartbeat(ctx, "halfway"); err != nil {
				return EndpointResult{}, err
			}
			return EndpointResult{Payload: []byte("accepted")}, nil
		},
	}
	result, err := executeEndpointActivity(
		context.Background(),
		EndpointEnvelope{Version: 1, EndpointID: 7, MessageID: "job-1"},
		registration,
		func(_ context.Context, details any) error {
			heartbeats <- details
			return nil
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Payload) != "accepted" {
		t.Fatalf("endpoint result = %q", result.Payload)
	}
	if heartbeat := <-heartbeats; heartbeat != "halfway" {
		t.Fatalf("heartbeat = %#v", heartbeat)
	}
}

func TestTemporalEndpointActivityPropagatesEndpointError(t *testing.T) {
	want := errors.New("business failure")
	registration := endpointRegistration{
		id: 7,
		handler: func(context.Context, EndpointEnvelope) (EndpointResult, error) {
			return EndpointResult{}, want
		},
	}
	_, err := executeEndpointActivity(
		context.Background(),
		EndpointEnvelope{Version: 1, EndpointID: 7, MessageID: "job-2"},
		registration, nil, nil,
	)
	if !errors.Is(err, want) {
		t.Fatalf("endpoint error = %v, want %v", err, want)
	}
}

func TestTemporalRuntimeIdentityUsesEndpointContract(t *testing.T) {
	if got := temporalEndpointActivityType("Temporal", "Durable Job"); got != "temporal.endpoint.durable_job.v1" {
		t.Fatalf("endpoint activity type = %q", got)
	}
	if got := temporalEndpointWorkflowID("Temporal", "Durable Job", "job-1"); got != "temporal/endpoint/durable_job/job-1" {
		t.Fatalf("endpoint workflow id = %q", got)
	}
	if got := temporalDirectWorkflowType("Temporal", "Durable Job"); got != "temporal.endpoint.durable_job.workflow.v1" {
		t.Fatalf("direct endpoint workflow type = %q", got)
	}
	if got := temporalScheduleWorkflowID("Temporal Connector", "Durable Job"); got != "temporal_connector/schedule/durable_job" {
		t.Fatalf("schedule workflow id = %q", got)
	}
}

func TestScheduledTimeUsesTemporalScheduleWorkflowIDSuffix(t *testing.T) {
	fallback := time.Date(2026, 8, 24, 12, 35, 1, 0, time.UTC)
	got := scheduledTimeFromWorkflowID(
		"Temporal/schedule/DurableJob-2026-08-24T12:30:00.123456789Z", fallback,
	)
	want := time.Date(2026, 8, 24, 12, 30, 0, 123456789, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("scheduled time = %s, want %s", got, want)
	}
	if got := scheduledTimeFromWorkflowID("manual-workflow", fallback); !got.Equal(fallback) {
		t.Fatalf("fallback scheduled time = %s, want %s", got, fallback)
	}
}

func TestTemporalCronExpressionPreservesPortableMinuteSemantics(t *testing.T) {
	if got, want := temporalCronExpression("  */5   * * * * "), "0 */5 * * * *"; got != want {
		t.Fatalf("temporal cron expression = %q, want %q", got, want)
	}
}

func TestTemporalEndpointWorkflowPreservesOnDemandEnvelopeAndResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType},
	)
	const activityType = "Temporal.endpoint.DurableJob.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if envelope.EndpointID != 7 || envelope.MessageID != "job-1" || envelope.StreamID != "request-1" || envelope.Scheduled {
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
			Version: 1, EndpointID: 7, MessageID: "job-1", StreamID: "request-1", Payload: []byte("value"),
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
	const activityType = "Temporal.endpoint.TemporalSchedule.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if !envelope.Scheduled || envelope.ScheduleID != "schedule-8" || envelope.MessageID == "" || envelope.StreamID != envelope.MessageID || envelope.ScheduledAtNano == 0 {
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

func TestTemporalWorkflowEndpointRunsGraphContractWithDurableTimer(t *testing.T) {
	registration := endpointRegistration{
		id: 9, workflowType: "temporal.endpoint.workflow_job.v1",
		handler: func(ctx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if envelope.MessageID != "workflow-1" || envelope.StreamID != "request-1" {
				t.Fatalf("unexpected workflow endpoint envelope: %+v", envelope)
			}
			if !runtime.IsDurableWorkflowContext(ctx) {
				t.Fatal("workflow endpoint did not receive durable Workflow context")
			}
			if err := runtime.DurableCallHeartbeat(ctx, "ignored outside Activity"); err != nil {
				return EndpointResult{}, err
			}
			resumed := false
			handled, err := runtime.RunDurableCallDelay(ctx, time.Hour, func() { resumed = true })
			if err != nil || !handled || !resumed {
				return EndpointResult{}, fmt.Errorf("durable delay handled=%v resumed=%v: %w", handled, resumed, err)
			}
			return EndpointResult{Payload: []byte("workflow-result")}, nil
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			return executeEndpointWorkflow(ctx, envelope, registration, temporalContextPropagator{})
		},
		workflow.RegisterOptions{Name: registration.workflowType},
	)
	environment.ExecuteWorkflow(registration.workflowType, EndpointEnvelope{
		Version: 1, EndpointID: 9, MessageID: "workflow-1", StreamID: "request-1",
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result EndpointResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if string(result.Payload) != "workflow-result" {
		t.Fatalf("unexpected workflow result: %+v", result)
	}
}

func TestScheduledTemporalWorkflowEndpointCreatesTriggerIdentity(t *testing.T) {
	registration := endpointRegistration{
		id: 10, workflowType: "temporal.endpoint.scheduled_workflow.v1",
		handler: func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if !envelope.Scheduled || envelope.ScheduleID != "workflow-schedule" ||
				envelope.MessageID == "" || envelope.StreamID != envelope.MessageID ||
				envelope.ScheduledAtNano == 0 || envelope.FiredAtNano == 0 {
				return EndpointResult{}, fmt.Errorf("unexpected scheduled Workflow envelope: %+v", envelope)
			}
			return EndpointResult{}, nil
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			return executeEndpointWorkflow(ctx, envelope, registration, temporalContextPropagator{})
		},
		workflow.RegisterOptions{Name: registration.workflowType},
	)
	environment.ExecuteWorkflow(registration.workflowType, EndpointEnvelope{
		Version: 1, EndpointID: 10, Scheduled: true, ScheduleID: "workflow-schedule",
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}
