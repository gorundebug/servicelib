/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type Item[K comparable] struct {
	values    [][]interface{}
	key       K
	lock      sync.Mutex
	deadline  time.Time
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
	storage      map[K]*Item[K]
	ttl          time.Duration
	lock         sync.Mutex
	deadlineList *list.List
	timer        *time.Timer
}

func (s *HashMapJoinStorage[K]) processDeadline() {
	deadlineReached := func() []*Item[K] {
		var deadlineReached []*Item[K]
		s.lock.Lock()
		defer s.lock.Unlock()
		for e := s.deadlineList.Front(); e != nil; e = s.deadlineList.Front() {
			item := e.Value.(*Item[K])
			if !time.Now().Before(item.deadline) {
				deadlineReached = append(deadlineReached, item)
				s.deadlineList.Remove(e)
			} else {
				s.timer.Reset(time.Until(item.deadline))
				break
			}
		}
		if s.deadlineList.Len() == 0 {
			s.timer.Stop()
			s.timer = nil
		}
		return deadlineReached
	}()
	for _, item := range deadlineReached {
		func(item *Item[K]) {
			if item.processed.CompareAndSwap(false, true) {
				s.lock.Lock()
				defer s.lock.Unlock()
				delete(s.storage, item.key)
			}
		}(item)
	}
}

func (s *HashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}, f JoinValueFunc) {
	item := func() *Item[K] {
		s.lock.Lock()
		defer s.lock.Unlock()
		item := s.storage[key]
		if item == nil {
			item = &Item[K]{
				values: make([][]interface{}, index+1, 2),
			}
			s.storage[key] = item
			if s.ttl > 0 {
				e := s.deadlineList.Back()
				item.deadline = time.Now().Add(s.ttl)
				for e != nil {
					if item.deadline.Before(e.Value.(*Item[K]).deadline) {
						e = e.Prev()
					} else {
						s.deadlineList.InsertAfter(item, e)
						break
					}
				}
				if e == nil {
					s.deadlineList.PushFront(item)
				}
				if s.timer == nil {
					s.timer = time.AfterFunc(time.Until(item.deadline), s.processDeadline)
				}
			}
		}
		return item
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
		storage:      make(map[K]*Item[K]),
		deadlineList: list.New(),
		ttl:          ttl,
	}
}
