/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"fmt"

	"go.opentelemetry.io/otel"
	sdkotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// NewWorkflowReplayer constructs a history replayer with the same Workflow
// interceptors and serializable context propagation used by a live Connector.
// Process-owned metrics and exporters are deliberately absent: the SDK's
// replay-aware Workflow logger, metric handler and tracing interceptor suppress
// duplicate telemetry while replaying recorded commands.
func NewWorkflowReplayer() (worker.WorkflowReplayer, error) {
	tracingInterceptor, err := sdkotel.NewTracingInterceptor(sdkotel.TracerOptions{
		TextMapPropagator: otel.GetTextMapPropagator(),
	})
	if err != nil {
		return nil, fmt.Errorf("create Temporal replay tracing interceptor: %w", err)
	}
	return worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{tracingInterceptor},
		ContextPropagators: []workflow.ContextPropagator{
			temporalContextPropagator{},
		},
	})
}
