/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package runtime

import (
	"context"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// ApplyDataSourceEndpointTracing force-enables sampling for one inbound event
// when the endpoint's current reloadable configuration requests it. Callers
// retain any sampling already restored from an incoming transport carrier.
// DataSink adapters deliberately never call this function.
func ApplyDataSourceEndpointTracing(
	ctx context.Context,
	environment RuntimeEnvironment,
	endpointID int,
) context.Context {
	if environment == nil || environment.RuntimeConfig() == nil {
		return ctx
	}
	endpoint := environment.RuntimeConfig().GetEndpointConfigByID(endpointID)
	if endpoint != nil && endpoint.GetTracingEnabled() {
		return tracing.EnableSampling(ctx)
	}
	return ctx
}
