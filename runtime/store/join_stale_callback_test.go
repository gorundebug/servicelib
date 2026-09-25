package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStaleJoinCallbackCannotDeleteOrRestoreReplacement(t *testing.T) {
	for _, completes := range []bool{false, true} {
		name := "renew"
		if completes { name = "complete" }
		t.Run(name, func(t *testing.T) {
			s, gauge := makeStorage(t, time.Hour, true)
			defer s.Stop(context.Background())
			oldContext, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var calls atomic.Int32
			go func() {
				s.JoinValue(oldContext, "key", 0, 10, func([][]interface{}) bool {
					if calls.Add(1) == 1 {
						close(entered)
						<-release
					}
					return completes
				})
				close(returned)
			}()
			select { case <-entered: case <-time.After(time.Second): t.Fatal("old callback did not start") }
			<-oldContext.Done()
			s.JoinValue(context.Background(), "key", 0, 20, func(values [][]interface{}) bool {
				if len(values) != 1 || len(values[0]) != 1 || values[0][0] != 20 {
					t.Errorf("replacement contains old values: %v", values)
				}
				return false
			})
			unblock()
			select { case <-returned: case <-time.After(time.Second): t.Fatal("old callback did not return") }
			s.JoinValue(context.Background(), "key", 1, 30, func(values [][]interface{}) bool {
				if len(values) != 2 || len(values[0]) != 1 || values[0][0] != 20 || len(values[1]) != 1 || values[1][0] != 30 {
					t.Errorf("stale callback deleted or resurrected a generation: %v", values)
				}
				return true
			})
			if count := gauge.Value(); count != 0 {
				t.Errorf("completed generations left incorrect gauge: %d", count)
			}
		})
	}
}
