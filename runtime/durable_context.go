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
	"fmt"
	"log/slog"
	"sync"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

var (
	// ErrNoDurableCallContext reports a DurableCall operation outside a
	// processing-side Temporal Activity scope.
	ErrNoDurableCallContext = errors.New("durable call context is not present")
	// ErrDurableCallAlreadyCompleted reports a second terminal operation.
	ErrDurableCallAlreadyCompleted = errors.New("durable call is already completed")
	// ErrDurableCallHeartbeatAfterCompletion reports progress after a terminal
	// outcome has already won.
	ErrDurableCallHeartbeatAfterCompletion = errors.New("durable call heartbeat after completion")
	// ErrDurableCallOutcomeMissing identifies cancellation/deadline fallback.
	ErrDurableCallOutcomeMissing = errors.New("durable call completed without explicit outcome")
)

// DurableCallEvent is a stable diagnostics classification shared by metrics,
// tracing and structured logging adapters.
type DurableCallEvent string

const (
	DurableCallEventHeartbeat       DurableCallEvent = "heartbeat"
	DurableCallEventSuccess         DurableCallEvent = "success"
	DurableCallEventError           DurableCallEvent = "error"
	DurableCallEventMissingOutcome  DurableCallEvent = "missing_outcome"
	DurableCallEventDuplicateResult DurableCallEvent = "duplicate_terminal"
	DurableCallEventLateHeartbeat   DurableCallEvent = "late_heartbeat"
)

// DurableCallDiagnostics receives state transitions without coupling the
// portable runtime context to Temporal, a logger, metrics or a tracing SDK.
type DurableCallDiagnostics func(context.Context, DurableCallEvent, error)

// DurableCallHeartbeatRecorder bridges the portable runtime API to the
// official Temporal Activity heartbeat operation on the processing side.
type DurableCallHeartbeatRecorder func(context.Context, any) error

type durableCallContextKeyType struct{}

var durableCallContextKey = durableCallContextKeyType{}

// DurableCallContext is local Activity execution state. It never crosses the
// Temporal boundary and is safe for concurrent graph branches.
type DurableCallContext struct {
	parentID string

	mu        sync.Mutex
	counts    map[string]uint64
	completed bool
	outcome   error
	done      chan struct{}

	heartbeat   DurableCallHeartbeatRecorder
	diagnostics DurableCallDiagnostics
	span        tracing.Span
	spanEnded   bool
}

// NewDurableCallContext constructs processing-side Activity state. Transport
// adapters call it after receiving a serialized durable envelope.
func NewDurableCallContext(
	parentID string,
	heartbeat DurableCallHeartbeatRecorder,
	diagnostics DurableCallDiagnostics,
) *DurableCallContext {
	return &DurableCallContext{
		parentID:    parentID,
		counts:      make(map[string]uint64),
		done:        make(chan struct{}),
		heartbeat:   heartbeat,
		diagnostics: diagnostics,
	}
}

// WithDurableCallContext attaches processing-side Activity state to the
// ordinary context that is propagated through the graph.
func WithDurableCallContext(ctx context.Context, durable *DurableCallContext) context.Context {
	if durable == nil {
		return ctx
	}
	return context.WithValue(ctx, durableCallContextKey, durable)
}

// DurableCallContextFromContext returns the current processing-side scope.
func DurableCallContextFromContext(ctx context.Context) (*DurableCallContext, bool) {
	if ctx == nil {
		return nil, false
	}
	durable, ok := ctx.Value(durableCallContextKey).(*DurableCallContext)
	return durable, ok && durable != nil
}

func reportMissingDurableCallContext(ctx context.Context, operation string) error {
	err := fmt.Errorf("%w: %s", ErrNoDurableCallContext, operation)
	slog.WarnContext(ctx, "DurableCall operation invoked outside an Activity", "operation", operation, "error", err)
	return err
}

func (d *DurableCallContext) report(ctx context.Context, event DurableCallEvent, err error) {
	d.mu.Lock()
	span := d.span
	d.mu.Unlock()
	if span != nil {
		attributes := []tracing.Attribute{tracing.StringAttr("event", string(event))}
		if err != nil {
			attributes = append(attributes, tracing.StringAttr("error", err.Error()))
		}
		span.AddEvent("durable_call."+string(event), attributes...)
		if event == DurableCallEventError || event == DurableCallEventMissingOutcome {
			tracing.SpanError(span, err)
		}
	}
	if d.diagnostics != nil {
		d.diagnostics(ctx, event, err)
	}
}

// BindDurableCallSpan gives the lifecycle scope ownership of an Activity span.
// It is intentionally a transport-adapter API, not a business API.
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

func (d *DurableCallContext) finishSpan() {
	d.mu.Lock()
	if d.span == nil || d.spanEnded {
		d.mu.Unlock()
		return
	}
	d.spanEnded = true
	span := d.span
	outcome := d.outcome
	d.mu.Unlock()
	if outcome == nil {
		span.SetStatus(tracing.StatusOK, "")
	}
	span.End()
}

func (d *DurableCallContext) complete(ctx context.Context, event DurableCallEvent, outcome error) error {
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		err := fmt.Errorf("%w: attempted %s", ErrDurableCallAlreadyCompleted, event)
		d.report(ctx, DurableCallEventDuplicateResult, err)
		return err
	}
	d.completed = true
	d.outcome = outcome
	d.mu.Unlock()
	d.report(ctx, event, outcome)
	close(d.done)
	return nil
}

// DurableCallHeartbeat records user-declared progress for the current Activity.
func DurableCallHeartbeat(ctx context.Context, message any) error {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return reportMissingDurableCallContext(ctx, "heartbeat")
	}
	durable.mu.Lock()
	if durable.completed {
		durable.mu.Unlock()
		err := ErrDurableCallHeartbeatAfterCompletion
		durable.report(ctx, DurableCallEventLateHeartbeat, err)
		return err
	}
	if durable.heartbeat != nil {
		if err := durable.heartbeat(ctx, message); err != nil {
			durable.mu.Unlock()
			durable.report(ctx, DurableCallEventHeartbeat, err)
			return err
		}
	}
	durable.mu.Unlock()
	durable.report(ctx, DurableCallEventHeartbeat, nil)
	return nil
}

// DurableCallSuccess explicitly completes the current Activity successfully.
func DurableCallSuccess(ctx context.Context) error {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return reportMissingDurableCallContext(ctx, "success")
	}
	return durable.complete(ctx, DurableCallEventSuccess, nil)
}

// DurableCallError explicitly completes the current Activity with an error.
func DurableCallError(ctx context.Context, outcome error) error {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return reportMissingDurableCallContext(ctx, "error")
	}
	if outcome == nil {
		outcome = errors.New("DurableCallError requires a non-nil error")
	}
	return durable.complete(ctx, DurableCallEventError, outcome)
}

// cancelWithoutOutcome is the cancellation/deadline safety net. There is no
// corresponding normal-completion inference.
func (d *DurableCallContext) cancelWithoutOutcome(ctx context.Context, cause error) {
	missing := ErrDurableCallOutcomeMissing
	if cause != nil {
		missing = fmt.Errorf("%w: %w", ErrDurableCallOutcomeMissing, cause)
	}
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		return
	}
	d.completed = true
	d.outcome = missing
	d.mu.Unlock()
	d.report(ctx, DurableCallEventMissingOutcome, missing)
	close(d.done)
}

// RunDurableCallActivity installs the processing-side context and cancellation
// hook, dispatches the existing consumer, then waits for an explicit terminal
// result or Activity cancellation. A nil return from invoke is not success.
func RunDurableCallActivity(
	ctx context.Context,
	durable *DurableCallContext,
	invoke func(context.Context) error,
) error {
	if durable == nil {
		return errors.New("durable call context is nil")
	}
	activityCtx := WithDurableCallContext(ctx, durable)
	defer durable.finishSpan()
	stopCancellationHook := context.AfterFunc(activityCtx, func() {
		durable.cancelWithoutOutcome(activityCtx, context.Cause(activityCtx))
	})
	defer stopCancellationHook()

	if err := invoke(activityCtx); err != nil {
		if cause := context.Cause(activityCtx); cause != nil {
			durable.cancelWithoutOutcome(activityCtx, cause)
		} else {
			_ = durable.complete(activityCtx, DurableCallEventError, err)
		}
	}
	<-durable.done
	durable.mu.Lock()
	outcome := durable.outcome
	durable.mu.Unlock()
	return outcome
}
