/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

var (
	ErrStoreAlreadyStarted = errors.New("store already started")
	ErrStoreStopped        = errors.New("store stopped")
)

type Item struct {
	values    [][]interface{}
	lock      sync.Mutex
	deadline  time.Time
	processed bool
}

type HashMapJoinStorage[K comparable] struct {
	storage1       map[K]*Item
	storage2       map[K]*Item
	rotateLock     sync.RWMutex
	lock           sync.RWMutex
	timer          *time.Timer
	config         JoinStorageConfig
	gaugeCount     metrics.Int64Gauge
	evictionsTotal metrics.Int64Counter
	environment    environment.ServiceEnvironment
	stopped        bool
	startOnce      sync.Once
	stopOnce       sync.Once
}

func MakeHashMapJoinStorage[K comparable](env environment.ServiceEnvironment, cfg JoinStorageConfig) (JoinStorage[K], error) {
	joinStorage := &HashMapJoinStorage[K]{
		storage1:    make(map[K]*Item),
		environment: env,
		config:      cfg,
	}
	scope := env.Metrics().Scope("hashmap_join_storage", metrics.Labels{
		"service": env.ServiceConfig().Name,
		"name":    cfg.GetName(),
	})
	var err error
	joinStorage.gaugeCount, err = scope.Gauge("count", "Elements count stored in a join storage", nil)
	if err != nil {
		return nil, err
	}
	joinStorage.evictionsTotal, err = scope.Counter("evictions_total", "Total number of items evicted from join storage by TTL", nil)
	if err != nil {
		return nil, err
	}
	if cfg.GetTTL() > 0 {
		joinStorage.storage2 = make(map[K]*Item)
	}
	return joinStorage, nil
}

func (s *HashMapJoinStorage[K]) rotate(ctx context.Context) {
	newStorage := make(map[K]*Item)
	s.rotateLock.Lock()
	defer s.rotateLock.Unlock()
	evicted := int64(len(s.storage2))
	s.gaugeCount.Sub(evicted)
	s.storage2 = s.storage1
	s.storage1 = newStorage
	if evicted > 0 {
		s.evictionsTotal.Add(ctx, evicted)
	}
	s.timer.Reset(s.config.GetTTL())
}

func (s *HashMapJoinStorage[K]) JoinValue(ctx context.Context, key K, index int, value interface{}, f JoinValueFunc) {
	ttl := s.config.GetTTL()
	renewTTL := s.config.GetRenewTTL()
	if ctxDeadline, ok := ctx.Deadline(); ok {
		ttl = time.Until(ctxDeadline)
	}
	if ttl > 0 {
		s.rotateLock.RLock()
		defer s.rotateLock.RUnlock()
	}
	for {
		item, inStorage2 := func() (*Item, bool) {

			item, inStorage2 := func() (*Item, bool) {
				s.lock.RLock()
				defer s.lock.RUnlock()
				item := s.storage1[key]
				if item != nil && (item.deadline.IsZero() || time.Now().Before(item.deadline)) {
					return item, false
				}
				if item == nil && ttl > 0 {
					item = s.storage2[key]
					if item != nil && (item.deadline.IsZero() || time.Now().Before(item.deadline)) {
						return item, true
					}
				}
				return nil, false
			}()

			if item != nil {
				return item, inStorage2
			}
			newItem := &Item{
				values: make([][]interface{}, index+1, 2),
			}
			s.lock.Lock()
			defer s.lock.Unlock()

			item = s.storage1[key]
			if item != nil && (item.deadline.IsZero() || time.Now().Before(item.deadline)) {
				return item, false
			}
			if ttl > 0 {
				newItem.deadline = time.Now().Add(ttl)
			}
			s.storage1[key] = newItem
			if item == nil {
				s.gaugeCount.Inc()
			}
			return newItem, false
		}()

		if func() bool {
			item.lock.Lock()
			defer item.lock.Unlock()
			if !item.processed && (item.deadline.IsZero() || item.deadline.After(time.Now())) {
				if len(item.values) <= index {
					item.values = append(item.values, make([][]interface{}, index-len(item.values)+1)...)
				}
				item.values[index] = append(item.values[index], value)
				item.processed = f(item.values)
				if item.processed {
					s.lock.Lock()
					defer s.lock.Unlock()
					if inStorage2 {
						delete(s.storage2, key)
					} else {
						delete(s.storage1, key)
					}
					s.gaugeCount.Dec()
				} else if renewTTL { //Depend on logic: should we extend deadline after change or not
					s.lock.Lock()
					defer s.lock.Unlock()
					if inStorage2 {
						delete(s.storage2, key)
					}
					item.deadline = time.Now().Add(ttl)
					s.storage1[key] = item
				}
				return true
			}
			return false
		}() {
			break
		}
	}
}

func (s *HashMapJoinStorage[K]) Start(ctx context.Context) error {
	var called bool
	s.startOnce.Do(func() {
		s.rotateLock.RLock()
		isStopped := s.stopped
		s.rotateLock.RUnlock()
		if isStopped {
			return
		}
		called = true
		if s.config.GetTTL() > 0 {
			s.timer = time.AfterFunc(s.config.GetTTL(), func() { s.rotate(ctx) })
		}
	})
	if !called {
		s.rotateLock.RLock()
		isStopped := s.stopped
		s.rotateLock.RUnlock()
		if isStopped {
			return ErrStoreStopped
		}
		return ErrStoreAlreadyStarted
	}
	return nil
}

func (s *HashMapJoinStorage[K]) Stop(ctx context.Context) {
	s.stopOnce.Do(func() {
		s.rotateLock.Lock()
		defer s.rotateLock.Unlock()
		s.stopped = true
		if s.timer != nil {
			s.timer.Stop()
		}
	})
}
