package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinTTLIncludesInitialCallbackTime(t *testing.T) {
	const ttl = 300 * time.Millisecond
	s, _ := makeStorage(t, ttl, false)
	defer s.Stop(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	expired := make(chan struct{}, 1)
	done := make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	var calls atomic.Int32
	go func() {
		defer close(done)
		s.JoinValue(context.Background(), "key", 0, 42, func(values [][]interface{}) bool {
			switch calls.Add(1) {
			case 1:
				close(entered)
				<-release
			case 2:
				expired <- struct{}{}
			}
			return false
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial callback did not start")
	}
	time.Sleep(ttl * 2)
	if calls.Load() != 1 {
		t.Fatal("expiry ran concurrently with the initial callback")
	}
	released.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("initial JoinValue did not finish")
	}
	select {
	case <-expired:
	case <-time.After(ttl / 2):
		t.Fatal("TTL was restarted after the initial callback")
	}
	if calls.Load() != 2 {
		t.Fatalf("callback count = %d", calls.Load())
	}
}

func TestJoinRenewedTTLDoesNotExpireAtOriginalDeadline(t *testing.T) {
	const ttl = 500 * time.Millisecond
	s, _ := makeStorage(t, ttl, true)
	defer s.Stop(context.Background())
	var calls atomic.Int32
	expired := make(chan time.Time, 1)
	callback := func(values [][]interface{}) bool {
		if calls.Add(1) == 3 {
			expired <- time.Now()
		}
		return false
	}
	s.JoinValue(context.Background(), "key", 0, 10, callback)
	time.Sleep(100 * time.Millisecond)
	renewedAt := time.Now()
	s.JoinValue(context.Background(), "key", 1, 20, callback)
	select {
	case expiredAt := <-expired:
		if expiredAt.Before(renewedAt.Add(ttl)) {
			t.Fatalf("original timer bypassed renewed TTL: expired %s after renewal, TTL=%s", expiredAt.Sub(renewedAt), ttl)
		}
	case <-time.After(ttl * 3):
		t.Fatal("renewed group did not expire")
	}
	if calls.Load() != 3 {
		t.Fatalf("callback count = %d", calls.Load())
	}
}
