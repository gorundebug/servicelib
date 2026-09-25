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
    values        [][]interface{}
    lock          sync.Mutex
    deadline      time.Time
    processed     bool
    f             JoinValueFunc // stored for context.AfterFunc
    stopAfterFunc func() bool   // cancels AfterFunc; nil if not registered
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
    metricsEnabled bool
    environment    environment.ServiceEnvironment
    highWaterMark  int
    stopped        bool
    startOnce      sync.Once
    stopOnce       sync.Once
}

func MakeHashMapJoinStorage[K comparable](env environment.ServiceEnvironment, cfg JoinStorageConfig) (JoinStorage[K], error) {
    joinStorage := &HashMapJoinStorage[K]{
        storage1:    make(map[K]*Item),
        environment: env,
        config:      cfg,
        metricsEnabled: !metrics.IsNoop(env.Metrics()),
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
    s.rotateLock.Lock()
    defer s.rotateLock.Unlock()

    // Timer.Stop does not remove an AfterFunc callback that is already queued.
    // A callback arriving after Stop must not restart maintenance.
    if s.stopped {
        return
    }

    total := len(s.storage1) + len(s.storage2)
    shouldRotate := s.highWaterMark == 0 || total*rotatingMapShrinkFactor < s.highWaterMark
    if total > s.highWaterMark {
        s.highWaterMark = total
    }

    if shouldRotate {
        s.highWaterMark = total
        newStorage := make(map[K]*Item)
        rescued := 0
        for k, item := range s.storage2 {
            if _, exists := s.storage1[k]; !exists {
                s.storage1[k] = item
                rescued++
            }
        }
        evicted := int64(len(s.storage2) - rescued)
        if s.metricsEnabled {
            s.gaugeCount.Sub(evicted)
        }
        s.storage2 = s.storage1
        s.storage1 = newStorage
        if s.metricsEnabled && evicted > 0 {
            s.evictionsTotal.Add(ctx, evicted)
        }
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
                values: make([][]interface{}, index+1),
                f:      f,
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
            // Retire the old generation from both lookup maps. Otherwise an
            // in-flight callback from storage2 can republish it after the
            // replacement in storage1 has already completed. Its accepted
            // expiry callback remains alive and uses identity-checked cleanup.
            if _, exists := s.storage2[key]; exists {
                delete(s.storage2, key)
                if s.metricsEnabled {
                    s.gaugeCount.Dec()
                }
            }
            s.storage1[key] = newItem
            if s.metricsEnabled && item == nil {
                s.gaugeCount.Inc()
            }
            if ttl > 0 {
                _, usesContextDeadline := ctx.Deadline()
                var afterFunc func()
                afterFunc = func() {
                    newItem.lock.Lock()
                    if newItem.processed {
                        newItem.lock.Unlock()
                        return
                    }
                    // Renewal updates deadline under this item lock. A timer
                    // already queued for the old deadline must wait for the
                    // renewed one rather than deliver premature expiration.
                    // Context cancellation/deadline is absolute, not renewable.
                    if !usesContextDeadline {
                        if remaining := time.Until(newItem.deadline); remaining > 0 {
                            timer := time.AfterFunc(remaining, afterFunc)
                            newItem.stopAfterFunc = timer.Stop
                            newItem.lock.Unlock()
                            return
                        }
                    }
                    newItem.processed = true
                    newItem.lock.Unlock()
                    newItem.f(newItem.values)
                    s.rotateLock.RLock()
                    s.lock.Lock()
                    if s.storage1[key] == newItem {
                        delete(s.storage1, key)
                        if s.metricsEnabled {
                            s.gaugeCount.Dec()
                        }
                    } else if s.storage2 != nil && s.storage2[key] == newItem {
                        delete(s.storage2, key)
                        if s.metricsEnabled {
                            s.gaugeCount.Dec()
                        }
                    }
                    s.lock.Unlock()
                    s.rotateLock.RUnlock()
                }
                if _, ok := ctx.Deadline(); ok {
                    newItem.stopAfterFunc = context.AfterFunc(ctx, afterFunc)
                } else {
                    timer := time.AfterFunc(ttl, afterFunc)
                    newItem.stopAfterFunc = timer.Stop
                }
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
                    if item.stopAfterFunc != nil {
                        item.stopAfterFunc()
                    }
                    s.lock.Lock()
                    defer s.lock.Unlock()
                    removed := false
                    if inStorage2 {
                        if s.storage2[key] == item {
                            delete(s.storage2, key)
                            removed = true
                        }
                    } else if s.storage1[key] == item {
                        delete(s.storage1, key)
                        removed = true
                    }
                    if removed && s.metricsEnabled {
                        s.gaugeCount.Dec()
                    }
                } else if renewTTL { //Depend on logic: should we extend deadline after change or not
                    s.lock.Lock()
                    defer s.lock.Unlock()
                    if inStorage2 {
                        if s.storage2[key] != item || (s.storage1[key] != nil && s.storage1[key] != item) {
                            return true
                        }
                        delete(s.storage2, key)
                    } else if s.storage1[key] != item {
                        // A callback may outlive its logical deadline. Never
                        // replace the newer group admitted while it ran.
                        return true
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
        // Publish the timer under the same lock used by rotate and Stop.
        // For a short TTL AfterFunc may otherwise run before assignment.
        s.rotateLock.Lock()
        defer s.rotateLock.Unlock()
        if s.stopped {
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

func (s *HashMapJoinStorage[K]) Stop(_ context.Context) {
    s.stopOnce.Do(func() {
        s.rotateLock.Lock()
        defer s.rotateLock.Unlock()
        s.stopped = true
        if s.timer != nil {
            s.timer.Stop()
        }
    })
}
