package temporal

import (
	"context"
	"go.temporal.io/sdk/workflow"
)

// WorkflowGraphContext adapts the supplied Temporal context for replay-safe
// graph construction, lifecycle and cooperative handlers. Values, deadlines and
// Err come from that workflow, never from a new process/background context.
//
// Done intentionally returns nil: workflow code must wait using Temporal
// channels/selectors, not native Go channels, goroutines or time-based contexts.
// Cancellation is observable through Err and workflowExecutionContext. This
// adapter is only for a workflow isolate, never for activities or process I/O.
func WorkflowGraphContext(ctx workflow.Context) context.Context {
	if ctx == nil {
		panic("Temporal Workflow context is nil")
	}
	return workflowGraphContext{Context: ctx}
}

type workflowGraphContext struct{ workflow.Context }

func (workflowGraphContext) Done() <-chan struct{} { return nil }
func (ctx workflowGraphContext) Value(key any) any {
	if _, ok := key.(workflowExecutionContextKey); ok {
		return ctx.Context
	}
	return ctx.Context.Value(key)
}

var _ context.Context = workflowGraphContext{}
