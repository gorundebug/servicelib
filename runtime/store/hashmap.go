/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"sync"
	"sync/atomic"
	"time"
)

const clearInterval = time.Duration(1) * time.Second

type Item[K comparable] struct {
	values    [][]interface{}
	key       K
	lock      sync.Mutex
	deadline  time.Time
	next      *Item[K]
	processed atomic.Bool
}

func (item *Item[K]) SetValue(index int, value interface{}, f JoinValueFunc) bool {
	item.lock.Lock()
	defer item.lock.Unlock()
	if len(item.values) <= index {
		item.values = append(item.values, make([][]interface{}, index-len(item.values)+1)...)
	}
	item.values[index] = append(item.values[index], value)
	if f(item.values) {
		item.processed.Store(true)
		return true
	}
	return false
}

type HashMapJoinStorage[K comparable] struct {
	storage           map[K]*Item[K]
	ttl               time.Duration
	lock              sync.RWMutex
	deadlineListFirst *Item[K]
	deadlineListLast  *Item[K]
	timer             *time.Timer
}

func (s *HashMapJoinStorage[K]) processDeadline() {
	deadlineReached := func() *Item[K] {
		var deadlineReached *Item[K]
		var deadlineReachedLast *Item[K]
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.deadlineListFirst == nil {
			return deadlineReached
		}
		it := s.deadlineListFirst
		s.deadlineListFirst = nil
		s.deadlineListLast = nil
		before := false
		now := time.Now()
		for it != nil {
			before = before || now.Before(it.deadline)
			processed := it.processed.Load()
			if !before {
				if !processed {
					if deadlineReached == nil {
						deadlineReached = it
						deadlineReachedLast = it
					} else {
						deadlineReachedLast.next = it
						deadlineReachedLast = it
					}
					it = it.next
					deadlineReachedLast.next = nil
				} else {
					it = it.next
				}
			} else {
				if !processed {
					if s.deadlineListFirst == nil {
						s.deadlineListFirst = it
						s.deadlineListLast = it
					} else {
						s.deadlineListLast.next = it
						s.deadlineListLast = it
					}
					it = it.next
					s.deadlineListLast.next = nil
				} else {
					it = it.next
				}
			}
		}
		if s.deadlineListFirst != nil {
			s.timer.Reset(clearInterval)
		}
		return deadlineReached
	}()
	for ; deadlineReached != nil; deadlineReached = deadlineReached.next {
		func(item *Item[K]) {
			if item.processed.CompareAndSwap(false, true) {
				s.lock.Lock()
				defer s.lock.Unlock()
				delete(s.storage, item.key)
			}
		}(deadlineReached)
	}
}

func (s *HashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}, f JoinValueFunc) {
	item := func() *Item[K] {
		s.lock.RLock()
		item := s.storage[key]
		s.lock.RUnlock()
		if item != nil && time.Now().Before(item.deadline) {
			return item
		}
		newItem := &Item[K]{
			values: make([][]interface{}, index+1, 2),
		}
		s.lock.Lock()
		defer s.lock.Unlock()
		if item == nil {
			item = s.storage[key]
			if item != nil && time.Now().Before(item.deadline) {
				return item
			}
		}
		if item != nil {
			item.processed.Store(true)
		}
		s.storage[key] = newItem
		if s.ttl > 0 {
			newItem.deadline = time.Now().Add(s.ttl)
			if s.deadlineListLast == nil {
				s.deadlineListLast = newItem
				s.deadlineListFirst = newItem
				if s.timer == nil {
					s.timer = time.AfterFunc(clearInterval, s.processDeadline)
				} else {
					s.timer.Reset(clearInterval)
				}
			} else {
				s.deadlineListLast.next = newItem
				s.deadlineListLast = newItem
			}
		}
		return newItem
	}()
	if item.SetValue(index, value, f) {
		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.storage, key)
	}
}

func MakeHashMapJoinStorage[K comparable](ttl time.Duration) JoinStorage[K] {
	return &HashMapJoinStorage[K]{
		storage: make(map[K]*Item[K]),
		ttl:     ttl,
	}
}

type RotateHashMapJoinStorage[K comparable] struct {
	storage1   map[K]*Item[K]
	storage2   map[K]*Item[K]
	ttl        time.Duration
	rotateLock sync.RWMutex
	lock       sync.RWMutex
	timer      *time.Timer
}

func MakeRotateHashMapJoinStorage[K comparable](ttl time.Duration) JoinStorage[K] {
	return &RotateHashMapJoinStorage[K]{
		storage1: make(map[K]*Item[K]),
		storage2: make(map[K]*Item[K]),
		ttl:      ttl,
	}
}

func (s *RotateHashMapJoinStorage[K]) rotate() {
	s.rotateLock.Lock()
	defer s.rotateLock.Unlock()
	s.storage2 = s.storage1
	s.storage1 = make(map[K]*Item[K])
	s.timer.Reset(s.ttl)
}

func (s *RotateHashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}, f JoinValueFunc) {
	s.rotateLock.RLock()
	defer s.rotateLock.RUnlock()
	if s.timer == nil && s.ttl > 0 {
		s.timer = time.AfterFunc(s.ttl, s.rotate)
	}
	item, storage := func() (*Item[K], map[K]*Item[K]) {
		item, storage := func() (*Item[K], map[K]*Item[K]) {
			s.lock.RLock()
			defer s.lock.RUnlock()
			if s.ttl > 0 {
				item := s.storage2[key]
				if item != nil {
					return item, s.storage2
				}
			}
			item := s.storage1[key]
			if item != nil {
				return item, s.storage1
			}
			return nil, nil
		}()
		if item == nil || time.Now().Before(item.deadline) {
			s.lock.Lock()
			defer s.lock.Unlock()
			if item == nil {
				item = s.storage1[key]
				if item != nil && !time.Now().Before(item.deadline) {
					return item, s.storage1
				}
			} else if &storage != &s.storage1 {
				delete(storage, key)
			}
			newItem := &Item[K]{
				values: make([][]interface{}, index+1, 2),
			}
			s.storage1[key] = newItem
			return item, s.storage1
		}
		return item, storage
	}()
	if item.SetValue(index, value, f) {
		s.lock.Lock()
		defer s.lock.Unlock()
		delete(storage, key)
	}
}
