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

func fieldToOTel(f envlog.Field) otellog.KeyValue {
	switch f.Type {
	case envlog.FieldTypeString:
		return otellog.String(f.Key, f.StrVal())
	case envlog.FieldTypeInt64:
		return otellog.Int64(f.Key, f.Int64Val())
	case envlog.FieldTypeFloat64:
		return otellog.Float64(f.Key, f.Float64Val())
	case envlog.FieldTypeBool:
		return otellog.Bool(f.Key, f.BoolVal())
	case envlog.FieldTypeError:
		if err := f.ErrorVal(); err != nil {
			return otellog.String(f.Key, err.Error())
		}
		return otellog.String(f.Key, "")
	default: // FieldTypeAny
		v := f.AnyVal()
		if v == nil {
			return otellog.String(f.Key, "")
		}
		if s, ok := v.(fmt.Stringer); ok {
			return otellog.String(f.Key, s.String())
		}
		return otellog.String(f.Key, fmt.Sprint(v))
	}
}

func (l *otelLogger) emit(ctx context.Context, severity otellog.Severity, msg string, fields []envlog.Field) {
	if !l.l.Enabled(ctx, otellog.EnabledParameters{Severity: severity}) {
		return
	}
	var r otellog.Record
	r.SetTimestamp(time.Now())
	r.SetSeverity(severity)
	r.SetBody(otellog.StringValue(msg))
	if len(fields) > 0 {
		attrs := make([]otellog.KeyValue, len(fields))
		for i, f := range fields {
			attrs[i] = fieldToOTel(f)
		}
		r.AddAttributes(attrs...)
	}
	l.l.Emit(ctx, r)
}

func (l *otelLogger) Debug(ctx context.Context, msg string, fields ...envlog.Field) {
	l.emit(ctx, otellog.SeverityDebug, msg, fields)
}
func (l *otelLogger) Info(ctx context.Context, msg string, fields ...envlog.Field) {
	l.emit(ctx, otellog.SeverityInfo, msg, fields)
}
func (l *otelLogger) Warn(ctx context.Context, msg string, fields ...envlog.Field) {
	l.emit(ctx, otellog.SeverityWarn, msg, fields)
}
func (l *otelLogger) Error(ctx context.Context, msg string, fields ...envlog.Field) {
	l.emit(ctx, otellog.SeverityError, msg, fields)
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
		l: provider.Logger(env.ServiceConfig().Name),
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
