/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package environment

import (
	"context"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// DelayPool is the interface for a delay task scheduler.
// Implement it in ServiceDependencies.DelayPool to replace the default goroutine-per-task pool.
type DelayPool interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
	Delay(ctx context.Context, deadline time.Duration, fn func()) error
}

type ServiceDependencies interface {
	MetricsEngine(ctx context.Context, env ServiceEnvironment) (metrics.MetricsEngine, error)
	TracingEngine(ctx context.Context, env ServiceEnvironment) (tracing.TracingEngine, error)
	LogsEngine(ctx context.Context, env ServiceEnvironment) (log.LogsEngine, error)
	// DelayPool returns a custom delay pool, or nil to use the default goroutine-per-task pool.
	DelayPool(ctx context.Context, env ServiceEnvironment) (DelayPool, error)
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
