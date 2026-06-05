/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package telemetry

import (
    "context"

    "github.com/gorundebug/servicelib/runtime/environment"
    envlog "github.com/gorundebug/servicelib/runtime/environment/log"
    "github.com/gorundebug/servicelib/runtime/environment/metrics"
    "github.com/gorundebug/servicelib/runtime/environment/tracing"
    "github.com/gorundebug/servicelib/runtime/telemetry/opentelemetry"
)

func CreatePrometheusMetricsEngine(env environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
    return opentelemetry.CreatePrometheusMetricsEngine(env)
}

func CreateOTLPMetricsEngine(ctx context.Context, env environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
    return opentelemetry.CreateOTLPMetricsEngine(ctx, env)
}

func WithContextSampler() opentelemetry.TracingOption {
    return opentelemetry.WithContextSampler()
}

func CreateStdoutTracingEngine(env environment.ServiceEnvironment, opts ...opentelemetry.TracingOption) (tracing.TracingEngine, error) {
    return opentelemetry.CreateStdoutTracingEngine(env, opts...)
}

func CreateOTLPTracingEngine(ctx context.Context, env environment.ServiceEnvironment, opts ...opentelemetry.TracingOption) (tracing.TracingEngine, error) {
    return opentelemetry.CreateOTLPTracingEngine(ctx, env, opts...)
}

func CreatePrettyTracingEngine(env environment.ServiceEnvironment, opts ...opentelemetry.TracingOption) (tracing.TracingEngine, error) {
    return opentelemetry.CreatePrettyTracingEngine(env, opts...)
}

func CreateStdoutLogsEngine(env environment.ServiceEnvironment) (envlog.LogsEngine, error) {
    return opentelemetry.CreateStdoutLogsEngine(env)
}

func CreateOTLPLogsEngine(ctx context.Context, env environment.ServiceEnvironment) (envlog.LogsEngine, error) {
    return opentelemetry.CreateOTLPLogsEngine(ctx, env)
}
