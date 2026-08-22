package testlog

import (
	"context"
	"errors"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/log"
)

func TestStructuredLogLevelAndTypedFieldContract(t *testing.T) {
	engine := New()
	logger := engine.DefaultLogger(nil)
	logger.Debug(context.Background(), "debug event")
	logger.Info(context.Background(), "info event")
	logger.Warn(
		context.Background(), "request failed",
		log.Str("endpoint", "orders"),
		log.Int64("attempt", 2),
		log.Float64("ratio", 1.5),
		log.Bool("retry", true),
	)
	logger.Error(context.Background(), "shutdown failed", log.Err(errors.New("timeout")))

	entries := engine.Entries()
	if len(entries) != 4 {
		t.Fatalf("entries = %d, want 4", len(entries))
	}
	levels := []log.Level{log.LevelDebug, log.LevelInfo, log.LevelWarn, log.LevelError}
	for index, level := range levels {
		if entries[index].Level != level {
			t.Fatalf("entry %d level = %s, want %s", index, entries[index].Level, level)
		}
	}
	fields := entries[2].Fields
	if len(fields) != 4 ||
		fields[0].Key != "endpoint" || fields[0].Type != log.FieldTypeString || fields[0].StrVal() != "orders" ||
		fields[1].Key != "attempt" || fields[1].Type != log.FieldTypeInt64 || fields[1].Int64Val() != 2 ||
		fields[2].Key != "ratio" || fields[2].Type != log.FieldTypeFloat64 || fields[2].Float64Val() != 1.5 ||
		fields[3].Key != "retry" || fields[3].Type != log.FieldTypeBool || !fields[3].BoolVal() {
		t.Fatalf("typed fields differ: %#v", fields)
	}
	errorField := entries[3].Fields[0]
	if errorField.Key != "error" || errorField.Type != log.FieldTypeError || errorField.StringValue() != "timeout" {
		t.Fatalf("error field differs: %#v", errorField)
	}
	if len(engine.EntriesAtLevel(log.LevelError)) != 1 {
		t.Fatal("error level filter differs")
	}
	engine.Reset()
	if len(engine.Entries()) != 0 {
		t.Fatal("reset retained entries")
	}
}
