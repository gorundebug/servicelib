package temporal

// Connector behavior is tested at its owning data-source boundary.

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
)

func TestTemporalEndpointActivityCompletesExplicitlyWithoutDurableLink(t *testing.T) {
	heartbeats := make(chan any, 1)
	registration := endpointRegistration{
		id: 7,
		handler: func(ctx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if envelope.FiredAtNano == 0 {
				t.Fatal("endpoint Activity did not record its actual fire time")
			}
			if _, ok := runtime.DurableCallContextFromContext(ctx); !ok {
				t.Fatal("endpoint handler did not receive a processing-side DurableCallContext")
			}
			if err := runtime.DurableCallHeartbeat(ctx, "halfway"); err != nil {
				return EndpointResult{}, err
			}
			if err := runtime.DurableCallSuccess(ctx); err != nil {
				return EndpointResult{}, err
			}
			return EndpointResult{Payload: []byte("accepted")}, nil
		},
	}
	result, err := executeEndpointActivity(
		context.Background(),
		EndpointEnvelope{Version: 1, EndpointID: 7, ExecutionID: "job-1"},
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
	if string(result.Result.Payload) != "accepted" {
		t.Fatalf("endpoint result = %q", result.Result.Payload)
	}
	if heartbeat := <-heartbeats; heartbeat != "halfway" {
		t.Fatalf("heartbeat = %#v", heartbeat)
	}
}

func TestTemporalEndpointActivityPropagatesExplicitErrorWithoutDurableLink(t *testing.T) {
	want := errors.New("business failure")
	registration := endpointRegistration{
		id: 7,
		handler: func(ctx context.Context, _ EndpointEnvelope) (EndpointResult, error) {
			if err := runtime.DurableCallError(ctx, want); err != nil {
				return EndpointResult{}, err
			}
			return EndpointResult{}, nil
		},
	}
	_, err := executeEndpointActivity(
		context.Background(),
		EndpointEnvelope{Version: 1, EndpointID: 7, ExecutionID: "job-2"},
		registration, nil, nil,
	)
	if !errors.Is(err, want) {
		t.Fatalf("endpoint error = %v, want %v", err, want)
	}
}

func TestTemporalEndpointActivityWithoutOutcomeWaitsForCancellation(t *testing.T) {
	started := make(chan struct{})
	registration := endpointRegistration{
		id: 7,
		handler: func(context.Context, EndpointEnvelope) (EndpointResult, error) {
			close(started)
			return EndpointResult{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := executeEndpointActivity(
			ctx,
			EndpointEnvelope{Version: 1, EndpointID: 7, ExecutionID: "job-3"},
			registration, nil, nil,
		)
		result <- err
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("endpoint completed without an explicit outcome: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, runtime.ErrDurableCallOutcomeMissing) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("endpoint Activity did not finish after cancellation")
	}
}

func TestTemporalRuntimeIdentityUsesServiceForLinksAndContractForEndpoints(t *testing.T) {
	if got := durableLinkWorkflowID(
		"Automation Service", "Consume Durable Job", "Process/Durable Job", "call-1",
	); got != "automation_service/durable/consume_durable_job/process_durable_job/call-1" {
		t.Fatalf("durable workflow id = %q", got)
	}
	if got := durableLinkOwner(
		"Automation Service", "Consume Durable Job", "Process/Durable Job",
	); got != "automation_service/link/consume_durable_job/process_durable_job/v1" {
		t.Fatalf("durable owner = %q", got)
	}
	if got := temporalEndpointActivityType("Temporal", "Durable Job"); got != "temporal.endpoint.durable_job.v1" {
		t.Fatalf("endpoint activity type = %q", got)
	}
	if got := temporalEndpointWorkflowID("Temporal", "Durable Job", "job-1"); got != "temporal/endpoint/durable_job/job-1" {
		t.Fatalf("endpoint workflow id = %q", got)
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

func TestDurableWorkflowInvokesRegisteredActivityWithUnchangedEnvelope(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType},
	)
	const activityType = "automation_service.durable.source.target.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope runtime.DurableEnvelope) (runtime.DurableActivityResult, error) {
			if envelope.CallID != "logical-call" || envelope.From != 2 || envelope.To != 3 {
				t.Fatalf("unexpected durable envelope: %+v", envelope)
			}
			return runtime.DurableActivityResult{}, nil
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
	const activityType = "Temporal.endpoint.DurableJob.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (endpointActivityResult, error) {
			if envelope.EndpointID != 7 || envelope.ExecutionID != "job-1" || envelope.StreamID != "request-1" || envelope.Scheduled {
				t.Fatalf("unexpected endpoint envelope: %+v", envelope)
			}
			return endpointActivityResult{Result: EndpointResult{Payload: []byte("result")}}, nil
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
	const activityType = "Temporal.endpoint.TemporalSchedule.v1"
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (endpointActivityResult, error) {
			if !envelope.Scheduled || envelope.ScheduleID != "schedule-8" || envelope.ExecutionID == "" || envelope.StreamID != envelope.ExecutionID || envelope.ScheduledAtNano == 0 {
				t.Fatalf("unexpected scheduled envelope: %+v", envelope)
			}
			return endpointActivityResult{}, nil
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

func TestDurableWorkflowResumesAfterTemporalTimer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType},
	)
	const initialActivity = "automation_service.durable.source.delay.v1"
	const continuationActivity = "automation_service.durable_continuation.temporal.v1"
	environment.RegisterActivityWithOptions(
		func(context.Context, runtime.DurableEnvelope) (runtime.DurableActivityResult, error) {
			return runtime.DurableActivityResult{Continuation: &runtime.DurableContinuation{
				Version: 1, FromName: "Delay", ToName: "After Delay", CallID: "call-1/delay",
				WakeAtUnixNano: time.Now().UTC().Add(time.Hour).UnixNano(), Payload: []byte("value"),
			}}, nil
		},
		activity.RegisterOptions{Name: initialActivity},
	)
	resumed := false
	environment.RegisterActivityWithOptions(
		func(_ context.Context, continuation runtime.DurableContinuation) (runtime.DurableActivityResult, error) {
			resumed = true
			if continuation.FromName != "Delay" || continuation.ToName != "After Delay" || string(continuation.Payload) != "value" {
				t.Fatalf("unexpected continuation: %+v", continuation)
			}
			return runtime.DurableActivityResult{}, nil
		},
		activity.RegisterOptions{Name: continuationActivity},
	)
	environment.ExecuteWorkflow(durableWorkflowType, durableWorkflowRequest{
		ActivityType: initialActivity, ContinuationActivityType: continuationActivity,
		ActivityStartToCloseMillis: 1_000, MaximumAttempts: 1,
		Envelope: runtime.DurableEnvelope{Version: 1, From: 1, To: 2, CallID: "call-1"},
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !resumed {
		t.Fatal("continuation Activity was not executed")
	}
}
