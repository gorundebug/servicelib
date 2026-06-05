/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package log

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Level represents a log severity level.
type Level uint8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
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
	default:
		return "unknown"
	}
}

// FieldType discriminates the inline storage type within Field.
type FieldType uint8

const (
	FieldTypeString  FieldType = iota
	FieldTypeInt64             // also used for int
	FieldTypeFloat64
	FieldTypeBool
	FieldTypeError   // anyVal holds the error interface — no boxing
	FieldTypeAny     // anyVal holds an arbitrary value — may box
)

// Field is a structured log field. Primitive types are stored inline
// (numVal / strVal) to avoid interface boxing at call sites.
type Field struct {
	Key    string
	Type   FieldType
	numVal uint64 // int64 (cast), float64 (bits), bool (0|1)
	strVal string
	anyVal any
}

// Typed accessors — no allocations.
func (f Field) StrVal() string      { return f.strVal }
func (f Field) Int64Val() int64     { return int64(f.numVal) }
func (f Field) Float64Val() float64 { return math.Float64frombits(f.numVal) }
func (f Field) BoolVal() bool       { return f.numVal != 0 }
func (f Field) AnyVal() any         { return f.anyVal }
func (f Field) ErrorVal() error     { err, _ := f.anyVal.(error); return err }

// Value returns the field value as any. Primitive types are boxed here —
// call this only inside logger implementations, not at call sites.
func (f Field) Value() any {
	switch f.Type {
	case FieldTypeString:
		return f.strVal
	case FieldTypeInt64:
		return int64(f.numVal)
	case FieldTypeFloat64:
		return math.Float64frombits(f.numVal)
	case FieldTypeBool:
		return f.numVal != 0
	default:
		return f.anyVal
	}
}

// StringValue formats the field value as a string (for text-only backends).
func (f Field) StringValue() string {
	switch f.Type {
	case FieldTypeString:
		return f.strVal
	case FieldTypeInt64:
		return strconv.FormatInt(int64(f.numVal), 10)
	case FieldTypeFloat64:
		return strconv.FormatFloat(math.Float64frombits(f.numVal), 'f', -1, 64)
	case FieldTypeBool:
		if f.numVal != 0 {
			return "true"
		}
		return "false"
	case FieldTypeError:
		if err := f.ErrorVal(); err != nil {
			return err.Error()
		}
		return ""
	default:
		if f.anyVal != nil {
			return fmt.Sprintf("%v", f.anyVal)
		}
		return ""
	}
}

// ── Field constructors ────────────────────────────────────────────────────────

func Str(key, val string) Field {
	return Field{Key: key, Type: FieldTypeString, strVal: val}
}

func Int(key string, val int) Field {
	return Field{Key: key, Type: FieldTypeInt64, numVal: uint64(val)}
}

func Int64(key string, val int64) Field {
	return Field{Key: key, Type: FieldTypeInt64, numVal: uint64(val)}
}

func Float64(key string, val float64) Field {
	return Field{Key: key, Type: FieldTypeFloat64, numVal: math.Float64bits(val)}
}

func Bool(key string, val bool) Field {
	var n uint64
	if val {
		n = 1
	}
	return Field{Key: key, Type: FieldTypeBool, numVal: n}
}

// Err stores an error. Since error is already an interface, no boxing occurs.
func Err(err error) Field {
	return Field{Key: "error", Type: FieldTypeError, anyVal: err}
}

// Any stores an arbitrary value. May box if val is a concrete type.
func Any(key string, val any) Field {
	return Field{Key: key, Type: FieldTypeAny, anyVal: val}
}

// ── Event ─────────────────────────────────────────────────────────────────────

// Event is the canonical representation of a single log record.
// Implementations may use it internally; it is not required by the Logger interface.
type Event struct {
	Level   Level
	Msg     string
	Time    time.Time
	Fields  []Field
	Context context.Context
}

// ── Engine / Logger ───────────────────────────────────────────────────────────

type Config struct{}

type LogsEngine interface {
	DefaultLogger(cfg *Config) Logger
	Shutdown(ctx context.Context) error
}

// Logger is a structured logger. Pass key-value pairs as Field values using
// the constructors (Str, Int, Err, …). No format strings, no eager Sprintf.
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
}
