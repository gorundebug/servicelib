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
	"time"

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
	DurableCallEventSuspended       DurableCallEvent = "suspended"
)

// DurableContinuation is the serialized boundary between two Activity
// executions separated by a durable timer. Stream names are immutable graph
// identities; generated numeric IDs are deliberately not persisted in
// Temporal history.
type DurableContinuation struct {
	Version          int               `json:"version"`
	FromName         string            `json:"fromName"`
	ToName           string            `json:"toName"`
	CallID           string            `json:"callId"`
	StreamID         string            `json:"streamId,omitempty"`
	Priority         int               `json:"priority"`
	DeadlineUnixNano int64             `json:"deadlineUnixNano,omitempty"`
	WakeAtUnixNano   int64             `json:"wakeAtUnixNano"`
	TraceCarrier     map[string]string `json:"traceCarrier,omitempty"`
	Payload          []byte            `json:"payload"`
}

// DurableActivityResult is returned to the durable transport when an
// Activity either completes normally or suspends at a Delay node.
type DurableActivityResult struct {
	Continuation *DurableContinuation `json:"continuation,omitempty"`
}

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

	mu           sync.Mutex
	counts       map[string]uint64
	completed    bool
	outcome      error
	done         chan struct{}
	delayAt      time.Time
	continuation *DurableContinuation

	heartbeat   DurableCallHeartbeatRecorder
	diagnostics DurableCallDiagnostics
	span        tracing.Span
	spanEnded   bool
}

// BeginDurableDelay marks the current Activity for durable suspension. It
// returns false outside a DurableCall Activity, preserving ordinary Delay
// behavior without a transport/config lookup on the local path.
func BeginDurableDelay(ctx context.Context, duration time.Duration) (bool, error) {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return false, nil
	}
	if duration <= 0 {
		return false, nil
	}
	durable.mu.Lock()
	defer durable.mu.Unlock()
	if durable.completed {
		return true, ErrDurableCallAlreadyCompleted
	}
	if !durable.delayAt.IsZero() {
		return true, errors.New("durable delay is already pending")
	}
	durable.delayAt = time.Now().UTC().Add(duration)
	return true, nil
}

// CaptureDurableContinuation is called by the outgoing caller of a Delay
// stream. The caller owns the serde and target identity, while Delay itself
// remains independent of both.
func CaptureDurableContinuation(
	ctx context.Context,
	fromName string,
	toName string,
	payload []byte,
	traceCarrier map[string]string,
) (bool, error) {
	durable, ok := DurableCallContextFromContext(ctx)
	if !ok {
		return false, nil
	}
	durable.mu.Lock()
	if durable.delayAt.IsZero() {
		durable.mu.Unlock()
		return false, nil
	}
	if durable.completed {
		durable.mu.Unlock()
		return true, ErrDurableCallAlreadyCompleted
	}
	streamID, _ := StreamIdFromContext(ctx)
	priority, _ := PriorityFromContext(ctx)
	continuation := &DurableContinuation{
		Version: 1, FromName: fromName, ToName: toName,
		CallID:   durable.parentID + "/delay",
		Priority: priority, WakeAtUnixNano: durable.delayAt.UnixNano(),
		TraceCarrier: cloneStringMap(traceCarrier),
		Payload:      append([]byte(nil), payload...),
	}
	if streamID != nil {
		continuation.StreamID = streamID.GetID()
	}
	if deadline, present := ctx.Deadline(); present {
		continuation.DeadlineUnixNano = deadline.UTC().UnixNano()
	}
	durable.completed = true
	durable.continuation = continuation
	durable.mu.Unlock()
	durable.report(ctx, DurableCallEventSuspended, nil)
	close(durable.done)
	return true, nil
}

func cloneStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
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

func (d *DurableCallContext) beginCompletion(outcome error) bool {
	d.mu.Lock()
	if d.completed {
		d.mu.Unlock()
		return false
	}
	d.completed = true
	d.outcome = outcome
	d.mu.Unlock()
	return true
}

func (d *DurableCallContext) complete(ctx context.Context, event DurableCallEvent, outcome error) error {
	if !d.beginCompletion(outcome) {
		err := fmt.Errorf("%w: attempted %s", ErrDurableCallAlreadyCompleted, event)
		d.report(ctx, DurableCallEventDuplicateResult, err)
		return err
	}
	d.report(ctx, event, outcome)
	close(d.done)
	return nil
}

// completeInvocationFailure is the Activity-adapter path for an error returned
// by the graph invocation itself. It deliberately has no discarded error:
// first-terminal-wins is preserved, while a conflicting terminal outcome is
// emitted through the configured diagnostics callback (metrics, log and span).
func (d *DurableCallContext) completeInvocationFailure(ctx context.Context, outcome error) {
	if !d.beginCompletion(outcome) {
		err := fmt.Errorf("%w: Activity invocation returned error: %v", ErrDurableCallAlreadyCompleted, outcome)
		d.report(ctx, DurableCallEventDuplicateResult, err)
		return
	}
	d.report(ctx, DurableCallEventError, outcome)
	close(d.done)
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
	_, err := RunDurableCallActivityWithResult(ctx, durable, invoke)
	return err
}

// RunDurableCallActivityWithResult additionally exposes a durable Delay
// continuation to transport adapters. Existing callers retain the original
// error-only API above.
func RunDurableCallActivityWithResult(
	ctx context.Context,
	durable *DurableCallContext,
	invoke func(context.Context) error,
) (DurableActivityResult, error) {
	if durable == nil {
		return DurableActivityResult{}, errors.New("durable call context is nil")
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
			durable.completeInvocationFailure(activityCtx, err)
		}
	}
	<-durable.done
	durable.mu.Lock()
	outcome := durable.outcome
	continuation := durable.continuation
	durable.mu.Unlock()
	return DurableActivityResult{Continuation: continuation}, outcome
}
