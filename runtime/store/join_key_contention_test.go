package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestJoinConcurrentCompletionDoesNotDiscardWaitingValues(t *testing.T) {
	s, _ := makeStorage(t, 0, false)
	defer s.Stop(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	var workers sync.WaitGroup
	var mu sync.Mutex
	observed := make(map[int]int)
	callback := func(values [][]interface{}) bool {
		value := values[0][0].(int)
		if value == 0 {
			close(entered)
			<-release
		}
		mu.Lock()
		observed[value]++
		mu.Unlock()
		return true
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		s.JoinValue(context.Background(), "shared", 0, 0, callback)
	}()
	<-entered
	started := make(chan struct{}, 64)
	for value := 1; value <= 64; value++ {
		workers.Add(1)
		go func(value int) {
			defer workers.Done()
			started <- struct{}{}
			s.JoinValue(context.Background(), "shared", 0, value, callback)
		}(value)
	}
	for range 64 {
		<-started
	}
	close(release)
	workers.Wait()
	for value := 0; value <= 64; value++ {
		if observed[value] != 1 {
			t.Fatalf("value %d executed %d times, want exactly once", value, observed[value])
		}
	}
}

func TestJoinIndependentKeyProgressesWhileCallbackWaits(t *testing.T) {
	s, _ := makeStorage(t, 0, false)
	defer s.Stop(context.Background())
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		s.JoinValue(context.Background(), "first", 0, 1, func(_ [][]interface{}) bool {
			close(entered)
			<-release
			return true
		})
		close(finished)
	}()
	<-entered
	second := make(chan struct{})
	go func() {
		s.JoinValue(context.Background(), "second", 0, 2, func(_ [][]interface{}) bool { return true })
		close(second)
	}()
	var blocked bool
	select {
	case <-second:
	case <-time.After(time.Second):
		blocked = true
	}
	close(release)
	<-finished
	<-second
	if blocked {
		t.Fatal("unrelated key blocked behind the first callback")
	}
}
