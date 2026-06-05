/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package opentelemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// ── otelLogger ────────────────────────────────────────────────────────────────

type otelLogger struct {
	l otellog.Logger
}

func (l *otelLogger) emit(ctx context.Context, severity otellog.Severity, severityText, msg string) {
	var r otellog.Record
	r.SetTimestamp(time.Now())
	r.SetSeverity(severity)
	r.SetSeverityText(severityText)
	r.SetBody(otellog.StringValue(msg))
	l.l.Emit(ctx, r)
}

func (l *otelLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.emit(ctx, otellog.SeverityDebug, "DEBUG", fmt.Sprintf(format, args...))
}
func (l *otelLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.emit(ctx, otellog.SeverityInfo, "INFO", fmt.Sprintf(format, args...))
}
func (l *otelLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.emit(ctx, otellog.SeverityWarn, "WARN", fmt.Sprintf(format, args...))
}
func (l *otelLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.emit(ctx, otellog.SeverityError, "ERROR", fmt.Sprintf(format, args...))
}

// ── OtelLogsEngine ────────────────────────────────────────────────────────────

type OtelLogsEngine struct {
	logger   *otelLogger
	provider *sdklog.LoggerProvider
}

func (e *OtelLogsEngine) DefaultLogger(_ *envlog.Config) envlog.Logger {
	return e.logger
}

func (e *OtelLogsEngine) Shutdown(ctx context.Context) error {
	return e.provider.Shutdown(ctx)
}

func newOtelLogsEngine(env environment.ServiceEnvironment, processor sdklog.Processor) *OtelLogsEngine {
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(processor),
		sdklog.WithResource(serviceResource(env)),
	)
	logger := &otelLogger{
		l: provider.Logger("github.com/gorundebug/servicelib"),
	}
	return &OtelLogsEngine{logger: logger, provider: provider}
}

func CreateOTLPLogsEngine(ctx context.Context, env environment.ServiceEnvironment) (envlog.LogsEngine, error) {
	exporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	return newOtelLogsEngine(env, sdklog.NewBatchProcessor(exporter)), nil
}

func CreateStdoutLogsEngine(env environment.ServiceEnvironment) (envlog.LogsEngine, error) {
	exporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}
	return newOtelLogsEngine(env, sdklog.NewSimpleProcessor(exporter)), nil
}
