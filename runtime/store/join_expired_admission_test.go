package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExpiredJoinAdmitsNewGenerationWhileOldCallbackRuns(t *testing.T) {
	s, _ := makeStorage(t, time.Hour, false)
	defer s.Stop(context.Background())
	firstContext, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	go func() {
		s.JoinValue(firstContext, "key", 0, 10, func([][]interface{}) bool {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
			return false
		})
		close(returned)
	}()
	select { case <-entered: case <-time.After(time.Second): t.Fatal("initial callback did not start") }
	<-firstContext.Done()
	values := make(chan [][]interface{}, 1)
	secondReturned := make(chan struct{})
	go func() {
		s.JoinValue(context.Background(), "key", 0, 20, func(v [][]interface{}) bool { values <- v; return true })
		close(secondReturned)
	}()
	var fresh [][]interface{}
	select { case fresh = <-values: case <-time.After(time.Second): }
	unblock()
	select { case <-returned: case <-time.After(time.Second): t.Fatal("initial callback did not finish") }
	select { case <-secondReturned: case <-time.After(time.Second): t.Fatal("replacement did not finish") }
	if len(fresh) != 1 || len(fresh[0]) != 1 || fresh[0][0] != 20 {
		t.Fatalf("expired generation blocked or contaminated its replacement: %v", fresh)
	}
}
