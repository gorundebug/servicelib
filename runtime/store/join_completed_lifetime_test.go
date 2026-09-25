package store

import (
	"context"
	"testing"
	"time"
)

func TestCompletedJoinCancelsItsPendingExpiry(t *testing.T) {
	for _, useContextDeadline := range []bool{false, true} {
		name := "configured_ttl"
		if useContextDeadline {
			name = "context_deadline"
		}
		t.Run(name, func(t *testing.T) {
			s, gauge := makeStorage(t, time.Hour, false)
			defer s.Stop(context.Background())
			ctx := context.Background()
			if useContextDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				defer cancel()
			}
			s.JoinValue(ctx, "pending", 0, "first", func(_ [][]interface{}) bool { return false })
			s.rotateLock.RLock()
			s.lock.RLock()
			original := s.storage1["pending"]
			s.lock.RUnlock()
			s.rotateLock.RUnlock()
			if original == nil || original.stopAfterFunc == nil {
				t.Fatal("test requires an armed pending expiry")
			}
			s.JoinValue(ctx, "pending", 1, "second", func(values [][]interface{}) bool {
				if len(values) != 2 || len(values[0]) != 1 || len(values[1]) != 1 {
					t.Errorf("wrong join inputs: %v", values)
				}
				return true
			})
			if original.stopAfterFunc() {
				t.Fatal("completed Join left its one-hour expiry callback registered")
			}
			if got := gauge.Value(); got != 0 {
				t.Fatalf("completed Join count = %d, want 0", got)
			}
		})
	}
}
