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
//	require.Equal(t, testlog.LevelError, entries[0].Level)
//	require.Contains(t, entries[0].Message, "connection refused")
package testlog

import (
	"fmt"
	"sync"

	"github.com/gorundebug/servicelib/runtime/environment/log"
)

// Level mirrors logrus levels as a simple enum.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelPanic
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	case LevelPanic:
		return "panic"
	default:
		return "unknown"
	}
}

// Entry holds one captured log call.
type Entry struct {
	Level   Level
	Message string
}

// ── testLogger ────────────────────────────────────────────────────────────────

type testLogger struct {
	engine *TestLog
}

func (l *testLogger) record(level Level, msg string) {
	l.engine.record(Entry{Level: level, Message: msg})
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	l.record(LevelDebug, fmt.Sprintf(format, args...))
}
func (l *testLogger) Infof(format string, args ...interface{}) {
	l.record(LevelInfo, fmt.Sprintf(format, args...))
}
func (l *testLogger) Printf(format string, args ...interface{}) {
	l.record(LevelInfo, fmt.Sprintf(format, args...))
}
func (l *testLogger) Warnf(format string, args ...interface{}) {
	l.record(LevelWarn, fmt.Sprintf(format, args...))
}
func (l *testLogger) Warningf(format string, args ...interface{}) {
	l.record(LevelWarn, fmt.Sprintf(format, args...))
}
func (l *testLogger) Errorf(format string, args ...interface{}) {
	l.record(LevelError, fmt.Sprintf(format, args...))
}
func (l *testLogger) Fatalf(format string, args ...interface{}) {
	l.record(LevelFatal, fmt.Sprintf(format, args...))
}
func (l *testLogger) Panicf(format string, args ...interface{}) {
	l.record(LevelPanic, fmt.Sprintf(format, args...))
}

func (l *testLogger) Debug(args ...interface{}) { l.record(LevelDebug, fmt.Sprint(args...)) }
func (l *testLogger) Info(args ...interface{})  { l.record(LevelInfo, fmt.Sprint(args...)) }
func (l *testLogger) Print(args ...interface{}) { l.record(LevelInfo, fmt.Sprint(args...)) }
func (l *testLogger) Warn(args ...interface{})  { l.record(LevelWarn, fmt.Sprint(args...)) }
func (l *testLogger) Warning(args ...interface{}) {
	l.record(LevelWarn, fmt.Sprint(args...))
}
func (l *testLogger) Error(args ...interface{}) { l.record(LevelError, fmt.Sprint(args...)) }
func (l *testLogger) Fatal(args ...interface{}) { l.record(LevelFatal, fmt.Sprint(args...)) }
func (l *testLogger) Panic(args ...interface{}) { l.record(LevelPanic, fmt.Sprint(args...)) }

func (l *testLogger) Debugln(args ...interface{}) { l.record(LevelDebug, fmt.Sprintln(args...)) }
func (l *testLogger) Infoln(args ...interface{})  { l.record(LevelInfo, fmt.Sprintln(args...)) }
func (l *testLogger) Println(args ...interface{}) { l.record(LevelInfo, fmt.Sprintln(args...)) }
func (l *testLogger) Warnln(args ...interface{})  { l.record(LevelWarn, fmt.Sprintln(args...)) }
func (l *testLogger) Warningln(args ...interface{}) {
	l.record(LevelWarn, fmt.Sprintln(args...))
}
func (l *testLogger) Errorln(args ...interface{}) { l.record(LevelError, fmt.Sprintln(args...)) }
func (l *testLogger) Fatalln(args ...interface{}) { l.record(LevelFatal, fmt.Sprintln(args...)) }
func (l *testLogger) Panicln(args ...interface{}) { l.record(LevelPanic, fmt.Sprintln(args...)) }

func (l *testLogger) NativeLogger() interface{} { return l }

// ── TestLog ───────────────────────────────────────────────────────────────────

// TestLog implements log.LogsEngine and log.Logger.
// All log calls are recorded in memory and accessible via Entries().
// Fatal/Panic variants record the entry but do NOT exit or panic, making them
// safe to call in test code.
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
func (l *TestLog) EntriesAtLevel(level Level) []Entry {
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
