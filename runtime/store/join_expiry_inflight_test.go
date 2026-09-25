package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestActiveJoinExpiryFinishesAcrossStopOrReplacement(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "replacement"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := makeStorage(t, 20*time.Millisecond, false)
			defer s.Stop(context.Background())
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer close(release)
			var calls atomic.Int32
			s.JoinValue(context.Background(), "key", 0, 10, func(values [][]interface{}) bool {
				if calls.Add(1) == 1 {
					return false
				}
				close(entered)
				<-release
				if values[0][0] != 10 {
					t.Errorf("active callback lost its original value: %v", values)
				}
				close(finished)
				return false
			})
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("expiry callback did not start")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if stop {
				s.Stop(context.Background())
			} else {
				s.JoinValue(ctx, "key", 0, 20, func(values [][]interface{}) bool {
					if len(values[0]) != 1 || values[0][0] != 20 {
						t.Errorf("replacement included the expired generation: %v", values)
					}
					return false
				})
			}
			// Release through a single sender rather than closing twice on failure.
			release <- struct{}{}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("active callback did not finish")
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("old generation callback count = %d, want 2", got)
			}
			if !stop {
				s.JoinValue(ctx, "key", 1, 30, func(values [][]interface{}) bool {
					if len(values) != 2 || len(values[0]) != 1 || values[0][0] != 20 || values[1][0] != 30 {
						t.Errorf("replacement was lost: %v", values)
					}
					return true
				})
			}
		})
	}
}
