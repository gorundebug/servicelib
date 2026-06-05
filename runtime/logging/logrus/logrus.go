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

func (l *Logger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.withTraceFields(ctx).Debugf(format, args...)
}

func (l *Logger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.withTraceFields(ctx).Infof(format, args...)
}

func (l *Logger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.withTraceFields(ctx).Warnf(format, args...)
}

func (l *Logger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.withTraceFields(ctx).Errorf(format, args...)
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
