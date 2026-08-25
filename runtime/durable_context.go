/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

var (
	// ErrDurableCallHeartbeatAfterCompletion reports progress after the endpoint
	// Activity boundary has already closed.
	ErrDurableCallHeartbeatAfterCompletion = errors.New("durable call heartbeat after completion")
	// ErrTemporalContinueAsNewOutsideWorkflow reports a terminal Workflow
	// operation attempted from ordinary or Activity execution.
	ErrTemporalContinueAsNewOutsideWorkflow = errors.New("Temporal Continue-As-New requires a Workflow endpoint")
)

// DurableCallEvent is a stable diagnostics classification shared by metrics,
// tracing and structured logging adapters. The name is retained together with
// the public heartbeat API; it does not represent a graph call semantics.
type DurableCallEvent string

const (
	DurableCallEventHeartbeat     DurableCallEvent = "heartbeat"
	DurableCallEventSuccess       DurableCallEvent = "success"
	DurableCallEventError         DurableCallEvent = "error"
	DurableCallEventLateHeartbeat DurableCallEvent = "late_heartbeat"
)

// DurableCallDiagnostics receives Activity lifecycle transitions without
// coupling the portable runtime context to Temporal, logging, metrics or a
// tracing SDK.
type DurableCallDiagnostics func(context.Context, DurableCallEvent, error)

// DurableCallHeartbeatRecorder bridges the portable runtime API to the
// official Temporal Activity heartbeat operation on the processing side.
type DurableCallHeartbeatRecorder func(context.Context, any) error

// DurableCallDelay waits through the durable Workflow scheduler. It is set
// only for Workflow endpoint execution; Activity endpoints retain the normal
// language-runtime Delay implementation.
type DurableCallDelay func(time.Duration) error

type durableCallContextKeyType struct{}

var durableCallContextKey = durableCallContextKeyType{}

// DurableCallContext is local Temporal Activity execution state. It never
// crosses a Workflow/process boundary and is safe for concurrent graph branches.
// Its presence means only that processing entered through a Temporal Source.
type DurableCallContext struct {
	messageID string

	mu          sync.Mutex
	closed      bool
	heartbeat   DurableCallHeartbeatRecorder
	delay       DurableCallDelay
	diagnostics DurableCallDiagnostics
	workflow    bool
	replaying   func() bool
	span        tracing.Span
	spanEnded   bool
}

// NewDurableWorkflowContext constructs processing-side Workflow state. A
// Workflow has no Activity heartbeat, but the same context marker lets the
// unchanged Delay operator select the official durable timer backend.
func NewDurableWorkflowContext(
	messageID string,
	delay DurableCallDelay,
	replaying func() bool,
) *DurableCallContext {
	return &DurableCallContext{
		messageID: messageID, delay: delay, workflow: true,
		replaying: replaying,
	}
}

// TemporalContinueAsNewRequest is the terminal, in-process control outcome
// consumed by the Temporal Workflow adapter. Business code should create it
// only through TemporalContinueAsNew.
type TemporalContinueAsNewRequest struct{ NextInput any }

func (*TemporalContinueAsNewRequest) Error() string { return "Temporal Continue-As-New" }

// TemporalContinueAsNew terminates the current Workflow run and starts its
// next run with nextInput. It never returns. Calling it outside a Workflow
// endpoint panics with ErrTemporalContinueAsNewOutsideWorkflow so an invalid
// history boundary cannot be ignored accidentally.
func TemporalContinueAsNew(ctx context.Context, nextInput any) {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		panic(ErrTemporalContinueAsNewOutsideWorkflow)
	}
	durable.mu.Lock()
	workflow, closed := durable.workflow, durable.closed
	durable.mu.Unlock()
	if !workflow {
		panic(ErrTemporalContinueAsNewOutsideWorkflow)
	}
	if closed {
		panic(errors.New("Temporal Continue-As-New after Workflow completion"))
	}
	panic(&TemporalContinueAsNewRequest{NextInput: nextInput})
}

// NewDurableCallContext constructs processing-side Activity state. Temporal
// endpoint adapters call it after receiving a serialized endpoint envelope.
func NewDurableCallContext(
	messageID string,
	heartbeat DurableCallHeartbeatRecorder,
	diagnostics DurableCallDiagnostics,
) *DurableCallContext {
	return &DurableCallContext{
		messageID:   messageID,
		heartbeat:   heartbeat,
		diagnostics: diagnostics,
	}
}

// WithDurableCallContext attaches processing-side Activity state to the
// ordinary context propagated through the graph.
func WithDurableCallContext(ctx context.Context, durable *DurableCallContext) context.Context {
	if durable == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, durableCallContextKey, durable)
	if durable.workflow {
		policy := func() bool { return durable.replaying == nil || !durable.replaying() }
		ctx = metrics.WithRecordingPolicy(ctx, policy)
		ctx = tracing.WithRecordingPolicy(ctx, policy)
	}
	return ctx
}

// DurableCallContextFromContext returns the current processing-side scope.
func DurableCallContextFromContext(ctx context.Context) (*DurableCallContext, bool) {
	if ctx == nil {
		return nil, false
	}
	durable, ok := ctx.Value(durableCallContextKey).(*DurableCallContext)
	return durable, ok && durable != nil
}

// IsDurableWorkflowContext reports whether execution entered through a
// Workflow endpoint rather than an Activity endpoint.
func IsDurableWorkflowContext(ctx context.Context) bool {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return false
	}
	durable.mu.Lock()
	defer durable.mu.Unlock()
	return durable.workflow
}

func (d *DurableCallContext) report(ctx context.Context, event DurableCallEvent, err error) {
	d.mu.Lock()
	span := d.span
	d.mu.Unlock()
	if span != nil {
		var attributes []tracing.Attribute
		if err != nil {
			attributes = append(attributes, tracing.StringAttr("error", err.Error()))
		}
		span.AddEvent("temporal.activity."+string(event), attributes...)
		if event == DurableCallEventError {
			tracing.SpanError(span, err)
		}
	}
	if d.diagnostics != nil {
		d.diagnostics(ctx, event, err)
	}
}

// BindDurableCallSpan gives the Activity scope ownership of its input span.
// It is a transport-adapter API, not a business API.
func BindDurableCallSpan(ctx context.Context, span tracing.Span) bool {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok || span == nil {
		return false
	}
	durable.mu.Lock()
	durable.span = span
	durable.mu.Unlock()
	return true
}

func (d *DurableCallContext) close(ctx context.Context, outcome error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	span := d.span
	shouldEndSpan := span != nil && !d.spanEnded
	if shouldEndSpan {
		d.spanEnded = true
	}
	d.mu.Unlock()

	event := DurableCallEventSuccess
	if outcome != nil {
		event = DurableCallEventError
	}
	d.report(ctx, event, outcome)
	if shouldEndSpan {
		if outcome == nil {
			span.SetStatus(tracing.StatusOK, "")
		}
		span.End()
	}
}

// DurableCallHeartbeat records user-declared progress for the current Temporal
// Activity. It remains available throughout the ordinary downstream pipeline.
func DurableCallHeartbeat(ctx context.Context, message any) error {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return nil
	}
	durable.mu.Lock()
	if durable.closed {
		durable.mu.Unlock()
		err := ErrDurableCallHeartbeatAfterCompletion
		durable.report(ctx, DurableCallEventLateHeartbeat, err)
		return err
	}
	heartbeat := durable.heartbeat
	workflow := durable.workflow
	if workflow {
		durable.mu.Unlock()
		return nil
	}
	durable.mu.Unlock()
	if heartbeat != nil {
		if err := heartbeat(ctx, message); err != nil {
			durable.report(ctx, DurableCallEventHeartbeat, err)
			return err
		}
	}
	durable.report(ctx, DurableCallEventHeartbeat, nil)
	return nil
}

// RunDurableCallDelay executes fn after a durable Workflow timer when ctx came
// from a Workflow endpoint. handled=false means the caller must use its normal
// local timer backend.
func RunDurableCallDelay(
	ctx context.Context,
	duration time.Duration,
	fn func(),
) (handled bool, err error) {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return false, nil
	}
	durable.mu.Lock()
	delay := durable.delay
	durable.mu.Unlock()
	if delay == nil {
		return false, nil
	}
	if err := delay(duration); err != nil {
		return true, err
	}
	fn()
	return true, nil
}

// RunDurableActivity attaches the processing-side context for exactly one
// Temporal endpoint Activity. The existing endpoint handler owns the result and
// error boundary; returning from invoke closes the context exactly once.
func RunDurableActivity(
	ctx context.Context,
	durable *DurableCallContext,
	invoke func(context.Context) error,
) error {
	if durable == nil {
		return errors.New("durable call context is nil")
	}
	activityCtx := WithDurableCallContext(ctx, durable)
	err := invoke(activityCtx)
	durable.close(activityCtx, err)
	return err
}

// RunDurableWorkflow attaches processing-side state for one Workflow endpoint.
// The adapter closes the execution scope when the ordinary endpoint contract
// returns; heartbeat remains a silent no-op in this domain.
func RunDurableWorkflow(
	ctx context.Context,
	durable *DurableCallContext,
	invoke func(context.Context) error,
) (err error) {
	if durable == nil {
		return errors.New("durable workflow context is nil")
	}
	workflowCtx := WithDurableCallContext(ctx, durable)
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		control, ok := recovered.(*TemporalContinueAsNewRequest)
		if !ok {
			panic(recovered)
		}
		durable.close(workflowCtx, nil)
		err = control
	}()
	err = invoke(workflowCtx)
	durable.close(workflowCtx, err)
	return err
}
