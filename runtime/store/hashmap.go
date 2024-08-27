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
	return f(item.values)
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
		if item != nil {
			return item
		}
		newItem := &Item[K]{
			values: make([][]interface{}, index+1, 2),
		}
		s.lock.Lock()
		defer s.lock.Unlock()
		item = s.storage[key]
		if item != nil {
			return item
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
		if item.processed.CompareAndSwap(false, true) {
			s.lock.Lock()
			defer s.lock.Unlock()
			delete(s.storage, key)
		}
	}
}

func MakeHashMapJoinStorage[K comparable](ttl time.Duration) JoinStorage[K] {
	return &HashMapJoinStorage[K]{
		storage: make(map[K]*Item[K]),
		ttl:     ttl,
	}
}
