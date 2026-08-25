package temporal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
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
		func(ctx workflow.Context, request directEndpointWorkflowRequest) (EndpointResult, error) {
			return executeEndpointWorkflow(ctx, request, registration, temporalContextPropagator{})
		},
		workflow.RegisterOptions{Name: registration.workflowType},
	)
	environment.ExecuteWorkflow(registration.workflowType, directEndpointWorkflowRequest{
		ConnectorName: "temporal",
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: 9, MessageID: "workflow-1", StreamID: "request-1",
		},
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
		func(ctx workflow.Context, request directEndpointWorkflowRequest) (EndpointResult, error) {
			return executeEndpointWorkflow(ctx, request, registration, temporalContextPropagator{})
		},
		workflow.RegisterOptions{Name: registration.workflowType},
	)
	environment.ExecuteWorkflow(registration.workflowType, directEndpointWorkflowRequest{
		ConnectorName: "temporal",
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: 10, Scheduled: true, ScheduleID: "workflow-schedule",
		},
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestTemporalWorkflowEndpointContinuesAsNewWithTypedInput(t *testing.T) {
	registration := endpointRegistration{
		id: 11, workflowType: "temporal.endpoint.continue_job.v1",
		encodeInput: func(value any) ([]byte, error) {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected continuation input %T", value)
			}
			return []byte(text), nil
		},
		handler: func(ctx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			if string(envelope.Payload) == "first" {
				runtime.TemporalContinueAsNew(ctx, "second")
			}
			if envelope.Scheduled || string(envelope.Payload) != "second" {
				return EndpointResult{}, fmt.Errorf("unexpected continued envelope: %+v", envelope)
			}
			return EndpointResult{Payload: []byte("complete")}, nil
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request directEndpointWorkflowRequest) (EndpointResult, error) {
			return executeEndpointWorkflow(ctx, request, registration, temporalContextPropagator{})
		},
		workflow.RegisterOptions{Name: registration.workflowType},
	)
	environment.ExecuteWorkflow(registration.workflowType, directEndpointWorkflowRequest{
		ConnectorName: "temporal",
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: 11, MessageID: "continue-1", StreamID: "continue-1", Payload: []byte("first"),
		},
	})
	err := environment.GetWorkflowError()
	var continuation *workflow.ContinueAsNewError
	if !errors.As(err, &continuation) {
		t.Fatalf("expected Continue-As-New, got %v", err)
	}
	var next directEndpointWorkflowRequest
	if err := converter.GetDefaultDataConverter().FromPayloads(continuation.Input, &next); err != nil {
		t.Fatal(err)
	}
	if next.Envelope.Scheduled || string(next.Envelope.Payload) != "second" || next.Envelope.MessageID != "continue-1" {
		t.Fatalf("unexpected continued input: %+v", next)
	}
}

func TestWorkflowTemporalSinksAwaitSequentialActivityResults(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			return EndpointResult{Payload: append(envelope.Payload, []byte("-a")...)}, nil
		},
		activity.RegisterOptions{Name: "temporal.endpoint.activity_a.v1"},
	)
	environment.RegisterActivityWithOptions(
		func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
			return EndpointResult{Payload: append(envelope.Payload, []byte("-b")...)}, nil
		},
		activity.RegisterOptions{Name: "temporal.endpoint.activity_b.v1"},
	)
	workflowFunction := func(ctx workflow.Context) (string, error) {
		state := workflowSubmissionContext{
			workflowCtx: ctx,
			connector:   "temporal",
			endpoints: map[int]workflowEndpointConfig{
				1: testWorkflowActivityConfig(1, "activityA", "temporal.endpoint.activity_a.v1"),
				2: testWorkflowActivityConfig(2, "activityB", "temporal.endpoint.activity_b.v1"),
			},
		}
		first, err := submitEndpointFromWorkflow(state, 1, EndpointEnvelope{
			MessageID: "sequence-a", StreamID: "sequence", Payload: []byte("start"),
		})
		if err != nil {
			return "", err
		}
		second, err := submitEndpointFromWorkflow(state, 2, EndpointEnvelope{
			MessageID: "sequence-b", StreamID: "sequence", Payload: first.Payload,
		})
		return string(second.Payload), err
	}
	environment.ExecuteWorkflow(workflowFunction)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "start-a-b" {
		t.Fatalf("sequential activity result = %q", result)
	}
}

func TestWorkflowTemporalSinkResultCanFanOutToTwoActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	for _, target := range []struct {
		name   string
		suffix string
	}{
		{name: "temporal.endpoint.activity_a.v1", suffix: "-a"},
		{name: "temporal.endpoint.activity_b.v1", suffix: "-b"},
		{name: "temporal.endpoint.activity_c.v1", suffix: "-c"},
	} {
		target := target
		environment.RegisterActivityWithOptions(
			func(_ context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
				return EndpointResult{Payload: append(envelope.Payload, []byte(target.suffix)...)}, nil
			},
			activity.RegisterOptions{Name: target.name},
		)
	}
	workflowFunction := func(ctx workflow.Context) ([]string, error) {
		state := workflowSubmissionContext{
			workflowCtx: ctx,
			connector:   "temporal",
			endpoints: map[int]workflowEndpointConfig{
				1: testWorkflowActivityConfig(1, "activityA", "temporal.endpoint.activity_a.v1"),
				2: testWorkflowActivityConfig(2, "activityB", "temporal.endpoint.activity_b.v1"),
				3: testWorkflowActivityConfig(3, "activityC", "temporal.endpoint.activity_c.v1"),
			},
		}
		first, err := submitEndpointFromWorkflow(state, 1, EndpointEnvelope{
			MessageID: "fanout-a", StreamID: "fanout", Payload: []byte("start"),
		})
		if err != nil {
			return nil, err
		}
		results := make([]string, 0, 2)
		for endpointID, messageID := range []string{"fanout-b", "fanout-c"} {
			result, submitErr := submitEndpointFromWorkflow(state, endpointID+2, EndpointEnvelope{
				MessageID: messageID, StreamID: "fanout", Payload: first.Payload,
			})
			if submitErr != nil {
				return nil, submitErr
			}
			results = append(results, string(result.Payload))
		}
		return results, nil
	}
	environment.ExecuteWorkflow(workflowFunction)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result []string
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(result) != "[start-a-b start-a-c]" {
		t.Fatalf("fan-out activity results = %v", result)
	}
}

func testWorkflowActivityConfig(id int, name, activityType string) workflowEndpointConfig {
	return workflowEndpointConfig{
		ID: id, Name: name, TaskQueue: "temporal-test", ExecutionType: api.Activity,
		ActivityType: activityType, ActivityStartToCloseMillis: 1000, MaximumAttempts: 1,
	}
}
