/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the LICENSE file for details.
 */

package log

import (
	"context"
	"testing"
)

func TestNoopLogsEngine(t *testing.T) {
	engine := NewNoopLogsEngine()
	logger := engine.DefaultLogger(nil)
	logger.Debug(context.Background(), "debug", Str("key", "value"))
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.Error(context.Background(), "error")
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}
