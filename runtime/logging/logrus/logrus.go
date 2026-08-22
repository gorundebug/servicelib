/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package logrus

import (
	"context"
	"sync"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/sirupsen/logrus"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var logsEngine *LogEngine
var once sync.Once

type LogEngine struct {
	environment environment.ServiceEnvironment
}

type Logger struct {
	*logrus.Logger
}

func (l *Logger) withTraceFields(ctx context.Context) *logrus.Entry {
	span := oteltrace.SpanFromContext(ctx)
	if sc := span.SpanContext(); sc.IsValid() {
		return l.WithFields(logrus.Fields{
			"trace_id": sc.TraceID().String(),
			"span_id":  sc.SpanID().String(),
		})
	}
	return logrus.NewEntry(l.Logger)
}

func fieldsToLogrus(fields []log.Field) logrus.Fields {
	if len(fields) == 0 {
		return nil
	}
	lf := make(logrus.Fields, len(fields))
	for _, f := range fields {
		lf[f.Key] = f.Value()
	}
	return lf
}

func (l *Logger) emit(ctx context.Context, level logrus.Level, msg string, fields []log.Field) {
	if !l.IsLevelEnabled(level) {
		return
	}
	entry := l.withTraceFields(ctx)
	if lf := fieldsToLogrus(fields); lf != nil {
		entry = entry.WithFields(lf)
	}
	entry.Log(level, msg)
}

func (l *Logger) Debug(ctx context.Context, msg string, fields ...log.Field) {
	l.emit(ctx, logrus.DebugLevel, msg, fields)
}

func (l *Logger) Info(ctx context.Context, msg string, fields ...log.Field) {
	l.emit(ctx, logrus.InfoLevel, msg, fields)
}

func (l *Logger) Warn(ctx context.Context, msg string, fields ...log.Field) {
	l.emit(ctx, logrus.WarnLevel, msg, fields)
}

func (l *Logger) Error(ctx context.Context, msg string, fields ...log.Field) {
	l.emit(ctx, logrus.ErrorLevel, msg, fields)
}

func (e *LogEngine) DefaultLogger(_ *log.Config) log.Logger {
	return &Logger{Logger: logrus.StandardLogger()}
}

func (e *LogEngine) Shutdown(_ context.Context) error {
	return nil
}

func CreateLogsEngine(env environment.ServiceEnvironment) log.LogsEngine {
	once.Do(func() {
		logsEngine = &LogEngine{environment: env}
	})
	return logsEngine
}
