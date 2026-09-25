package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRotatedJoinCannotResurrectAfterReplacementCompletes(t *testing.T) {
	cfg := &dynamicJoinTTL{}
	cfg.ttl.Store(int64(time.Hour))
	cfg.renew.Store(true)
	m := newMockMetrics()
	storage, err := MakeHashMapJoinStorage[string](newMockEnv("rotated-generation", m), cfg)
	if err != nil { t.Fatal(err) }
	s := storage.(*HashMapJoinStorage[string])
	if err := s.Start(context.Background()); err != nil { t.Fatal(err) }
	defer s.Stop(context.Background())
	keep := func([][]interface{}) bool { return false }
	s.JoinValue(context.Background(), "key", 0, 10, keep)
	// Shorten only admission: the accepted expiry timer stays at one hour.
	cfg.ttl.Store(int64(100 * time.Millisecond))
	s.JoinValue(context.Background(), "key", 0, 11, keep)
	cfg.ttl.Store(int64(time.Hour))
	s.rotate(context.Background())
	s.rotateLock.RLock()
	s.lock.RLock()
	old := s.storage2["key"]
	deadline := old.deadline
	s.lock.RUnlock()
	s.rotateLock.RUnlock()
	defer func() {
		old.lock.Lock()
		defer old.lock.Unlock()
		if old.stopAfterFunc != nil { old.stopAfterFunc() }
	}()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		s.JoinValue(context.Background(), "key", 0, 12, func([][]interface{}) bool {
			close(entered)
			<-release
			return false
		})
		close(returned)
	}()
	select { case <-entered: case <-time.After(time.Second): t.Fatal("old callback did not enter") }
	if remaining := time.Until(deadline); remaining > 0 { time.Sleep(remaining) }
	s.JoinValue(context.Background(), "key", 0, 20, func(values [][]interface{}) bool {
		if len(values) != 1 || len(values[0]) != 1 || values[0][0] != 20 {
			t.Errorf("replacement includes expired data: %v", values)
		}
		return true
	})
	unblock()
	select { case <-returned: case <-time.After(time.Second): t.Fatal("old callback did not finish") }
	s.JoinValue(context.Background(), "key", 0, 30, func(values [][]interface{}) bool {
		if len(values) != 1 || len(values[0]) != 1 || values[0][0] != 30 {
			t.Errorf("rotated callback resurrected expired values: %v", values)
		}
		return true
	})
	if count := m.gauge.Value(); count != 0 { t.Errorf("completed map count = %d, want zero", count) }
}
