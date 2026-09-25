package store

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func newTimerLifecycleStorage(t *testing.T, ttl time.Duration) *HashMapJoinStorage[string] {
	t.Helper()
	storage, err := MakeHashMapJoinStorage[string](
		newMockEnv("timer-lifecycle", newMockMetrics()),
		&mockJoinStorageConfig{name: "timer-lifecycle", ttl: ttl},
	)
	if err != nil {
		t.Fatal(err)
	}
	s := storage.(*HashMapJoinStorage[string])
	t.Cleanup(func() { s.Stop(context.Background()) })
	return s
}

func TestJoinTimerPublicationWithImmediateExpiry(t *testing.T) {
	for round := 0; round < 200; round++ {
		s := newTimerLifecycleStorage(t, time.Nanosecond)
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
		s.Stop(context.Background())
		s.rotateLock.Lock()
		active := s.timer.Stop()
		s.rotateLock.Unlock()
		if active {
			t.Fatal("maintenance restarted after Stop")
		}
	}
}

func TestJoinTimerQueuedRotationDoesNotRestartAfterStop(t *testing.T) {
	s := newTimerLifecycleStorage(t, time.Hour)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.rotateLock.Lock()
	s.highWaterMark = 17
	item := &Item{}
	s.storage1["pending"] = item
	s.rotateLock.Unlock()
	s.Stop(context.Background())

	// Model an AfterFunc callback that was queued before Stop but only entered
	// rotate after it. The lifecycle check must precede all rotation work.
	s.rotate(context.Background())
	s.rotateLock.Lock()
	active := s.timer.Stop()
	preserved := s.highWaterMark == 17 && s.storage1["pending"] == item && len(s.storage2) == 0
	s.rotateLock.Unlock()
	if active {
		t.Fatal("late rotation rearmed the stopped timer")
	}
	if !preserved {
		t.Fatal("late rotation changed the stopped storage")
	}
}

func TestJoinTimerConcurrentStartStop(t *testing.T) {
	for round := 0; round < 200; round++ {
		s := newTimerLifecycleStorage(t, time.Nanosecond)
		start := make(chan struct{})
		result := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			result <- s.Start(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-start
			s.Stop(context.Background())
		}()
		close(start)
		wg.Wait()
		if err := <-result; err != nil && !errors.Is(err, ErrStoreStopped) {
			t.Fatalf("unexpected concurrent Start result: %v", err)
		}
		if err := s.Start(context.Background()); !errors.Is(err, ErrStoreStopped) {
			t.Fatalf("Start after Stop = %v", err)
		}
		s.rotateLock.Lock()
		active := s.timer != nil && s.timer.Stop()
		s.rotateLock.Unlock()
		if active {
			t.Fatal("concurrent Start left a timer running after Stop")
		}
	}
}
