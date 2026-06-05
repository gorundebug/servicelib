/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package environment

import (
	"context"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type ServiceDependencies interface {
	MetricsEngine(ctx context.Context, env ServiceEnvironment) (metrics.MetricsEngine, error)
	TracingEngine(ctx context.Context, env ServiceEnvironment) (tracing.TracingEngine, error)
	LogsEngine(ctx context.Context, env ServiceEnvironment) (log.LogsEngine, error)
}

type Lifecycle interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

type ServiceEnvironment interface {
	RuntimeConfig() *config.RuntimeConfig
	ServiceConfig() *config.ServiceConfig
	Metrics() metrics.Metrics
	Tracing() tracing.Tracing
	Log() log.Logger
	ServiceDependencies() ServiceDependencies
	ServiceContext() interface{}
}
