/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the LICENSE file for details.
 */

package log

import "context"

// NoopLogsEngine disables application logging without installing a backend.
type NoopLogsEngine struct{}

func NewNoopLogsEngine() LogsEngine { return NoopLogsEngine{} }

func (NoopLogsEngine) DefaultLogger(_ *Config) Logger { return NoopLogger{} }

func (NoopLogsEngine) Shutdown(_ context.Context) error { return nil }

// NoopLogger discards log records.
type NoopLogger struct{}

func (NoopLogger) Debug(context.Context, string, ...Field) {}
func (NoopLogger) Info(context.Context, string, ...Field)  {}
func (NoopLogger) Warn(context.Context, string, ...Field)  {}
func (NoopLogger) Error(context.Context, string, ...Field) {}
