package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcceptedJoinExpirySurvivesStorageStop(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var calls atomic.Int32
	expired := make(chan int, 1)
	s.JoinValue(ctx, "accepted", 0, 42, func(values [][]interface{}) bool {
		if calls.Add(1) > 1 {
			expired <- values[0][0].(int)
		}
		return false
	})
	if calls.Load() != 1 {
		t.Fatal("initial join callback did not run")
	}
	s.Stop(context.Background())
	cancel()
	select {
	case value := <-expired:
		if value != 42 {
			t.Fatalf("expiry lost accepted input: %d", value)
		}
	case <-time.After(time.Second):
		t.Fatal("storage stop suppressed an already accepted expiry callback")
	}
	if calls.Load() != 2 {
		t.Fatalf("callback count = %d, want 2", calls.Load())
	}
}

func TestStoppedJoinStoragePreservesCompletionAndReuse(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Hour} {
		t.Run(ttl.String(), func(t *testing.T) {
			s, _ := makeStorage(t, ttl, false)
			defer s.Stop(context.Background())
			calls := 0
			callback := func(values [][]interface{}) bool {
				calls++
				if values[0][0].(int) != 10 {
					t.Fatal("first input lost")
				}
				if len(values) == 1 {
					return false
				}
				if values[1][0].(int) != 20 {
					t.Fatal("second input lost")
				}
				return true
			}
			s.JoinValue(context.Background(), "accepted", 0, 10, callback)
			s.Stop(context.Background())
			s.Stop(context.Background())
			if err := s.Start(context.Background()); err == nil {
				t.Fatal("stopped storage restarted")
			}
			s.JoinValue(context.Background(), "accepted", 1, 20, callback)
			for _, key := range []string{"accepted", "new"} {
				s.JoinValue(context.Background(), key, 0, 10, callback)
				s.JoinValue(context.Background(), key, 1, 20, callback)
			}
			if calls != 6 {
				t.Fatalf("callbacks = %d, want 6", calls)
			}
		})
	}
}

func TestConfiguredJoinTTLSurvivesStopWithoutExternalOwner(t *testing.T) {
	var calls atomic.Int32
	expired := make(chan int, 1)
	func() {
		s, _ := makeStorage(t, 100*time.Millisecond, false)
		s.JoinValue(context.Background(), "accepted", 0, 42, func(values [][]interface{}) bool {
			if calls.Add(1) > 1 {
				expired <- values[0][0].(int)
			}
			return false
		})
		s.Stop(context.Background())
	}()
	select {
	case value := <-expired:
		if value != 42 {
			t.Fatalf("expiry lost input: %d", value)
		}
	case <-time.After(time.Second):
		t.Fatal("configured TTL callback suppressed after stop")
	}
	if calls.Load() != 2 {
		t.Fatalf("callbacks = %d, want 2", calls.Load())
	}
}
