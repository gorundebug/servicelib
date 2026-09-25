package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinRenewalKeepsFirstExpiryCallback(t *testing.T) {
	s, _ := makeStorage(t, 40*time.Millisecond, true)
	defer s.Stop(context.Background())
	expired := make(chan string, 4)
	var first, second atomic.Int32
	s.JoinValue(context.Background(), "key", 0, 10, func(values [][]interface{}) bool {
		if first.Add(1) > 1 {
			if len(values) != 2 || values[0][0] != 10 || values[1][0] != 20 {
				t.Errorf("expiry lost accumulated values: %v", values)
			}
			expired <- "first"
		}
		return false
	})
	s.JoinValue(context.Background(), "key", 1, 20, func([][]interface{}) bool {
		if second.Add(1) > 1 {
			expired <- "second"
		}
		return false
	})
	select {
	case callback := <-expired:
		if callback != "first" {
			t.Fatalf("expiry callback = %q, want first", callback)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("renewed entry did not expire")
	}
}

func TestJoinRenewalKeepsFirstContextCancellation(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, true)
	defer s.Stop(context.Background())
	firstContext, cancelFirst := context.WithTimeout(context.Background(), time.Hour)
	defer cancelFirst()
	secondContext, cancelSecond := context.WithTimeout(context.Background(), time.Hour)
	defer cancelSecond()
	expired := make(chan string, 4)
	var first, second atomic.Int32
	s.JoinValue(firstContext, "key", 0, 10, func([][]interface{}) bool {
		if first.Add(1) > 1 {
			expired <- "first"
		}
		return false
	})
	s.JoinValue(secondContext, "key", 1, 20, func([][]interface{}) bool {
		if second.Add(1) > 1 {
			expired <- "second"
		}
		return false
	})
	cancelFirst()
	select {
	case callback := <-expired:
		if callback != "first" {
			t.Fatalf("expiry callback = %q, want first", callback)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("renewal detached expiry from the first context")
	}
}
