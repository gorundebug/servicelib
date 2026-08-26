/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// NewWorkflowReplayer constructs a history replayer with the same Workflow
// serializable context propagation used by a live Connector. Worker
// interceptors are deliberately absent: history replay validates the Workflow
// commands below that boundary, while production replay exercises the SDK's
// replay-aware tracing interceptor. Registering a new tracing interceptor on an
// already traced history changes the inbound interceptor chain and can prevent
// the Workflow body from producing its recorded first command.
func NewWorkflowReplayer() (worker.WorkflowReplayer, error) {
	return worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		ContextPropagators: []workflow.ContextPropagator{
			temporalContextPropagator{},
		},
	})
}
