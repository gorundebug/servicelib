package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinRenewalPreservesContextCancellation(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, true)
	defer s.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var calls atomic.Int32
	expired := make(chan struct{}, 1)
	callback := func(values [][]interface{}) bool {
		if calls.Add(1) == 3 {
			expired <- struct{}{}
		}
		return false
	}
	s.JoinValue(ctx, "key", 0, 10, callback)
	s.JoinValue(ctx, "key", 1, 20, callback)
	cancel()
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("renewal suppressed context cancellation")
	}
	if calls.Load() != 3 {
		t.Fatalf("callback count = %d", calls.Load())
	}
}
