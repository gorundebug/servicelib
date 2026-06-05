/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// Package testlog provides an in-memory logging engine for use in automated
// tests. It implements log.LogsEngine and log.Logger, capturing every log
// call so assertions can be made on what was logged.
//
// Usage:
//
//	engine := testlog.New()
//	// pass engine via ServiceDependencies.LogsEngine()
//
//	doWork(ctx)
//
//	entries := engine.Entries()
//	require.Equal(t, log.LevelError, entries[0].Level)
//	require.Contains(t, entries[0].Message, "connection refused")
package testlog

import (
	"context"
	"sync"

	"github.com/gorundebug/servicelib/runtime/environment/log"
)

// Entry holds one captured log call.
type Entry struct {
	Level   log.Level
	Message string
	Fields  []log.Field
}

// ── testLogger ────────────────────────────────────────────────────────────────

type testLogger struct {
	engine *TestLog
}

func (l *testLogger) record(level log.Level, msg string, fields []log.Field) {
	cp := make([]log.Field, len(fields))
	copy(cp, fields)
	l.engine.record(Entry{Level: level, Message: msg, Fields: cp})
}

func (l *testLogger) Debug(_ context.Context, msg string, fields ...log.Field) {
	l.record(log.LevelDebug, msg, fields)
}
func (l *testLogger) Info(_ context.Context, msg string, fields ...log.Field) {
	l.record(log.LevelInfo, msg, fields)
}
func (l *testLogger) Warn(_ context.Context, msg string, fields ...log.Field) {
	l.record(log.LevelWarn, msg, fields)
}
func (l *testLogger) Error(_ context.Context, msg string, fields ...log.Field) {
	l.record(log.LevelError, msg, fields)
}

// ── TestLog ───────────────────────────────────────────────────────────────────

// TestLog implements log.LogsEngine and log.Logger.
// All log calls are recorded in memory and accessible via Entries().
type TestLog struct {
	mu      sync.Mutex
	entries []Entry
}

func New() *TestLog {
	return &TestLog{}
}

func (l *TestLog) record(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
}

// Entries returns a snapshot of all recorded log entries.
func (l *TestLog) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// EntriesAtLevel returns a snapshot filtered to a specific level.
func (l *TestLog) EntriesAtLevel(level log.Level) []Entry {
	all := l.Entries()
	var out []Entry
	for _, e := range all {
		if e.Level == level {
			out = append(out, e)
		}
	}
	return out
}

// Reset clears all recorded entries.
func (l *TestLog) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = l.entries[:0]
}

// ── log.LogsEngine ────────────────────────────────────────────────────────────

func (l *TestLog) DefaultLogger(_ *log.Config) log.Logger {
	return &testLogger{engine: l}
}

func (l *TestLog) Shutdown(_ context.Context) error {
	return nil
}
