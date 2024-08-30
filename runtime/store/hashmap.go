/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"sync"
	"time"
)

type Item struct {
	values    [][]interface{}
	lock      sync.Mutex
	deadline  time.Time
	processed bool
}

type HashMapJoinStorage[K comparable] struct {
	storage1   map[K]*Item
	storage2   map[K]*Item
	ttl        time.Duration
	rotateLock sync.RWMutex
	lock       sync.RWMutex
	timer      *time.Timer
	renewTTL   bool
}

func MakeHashMapJoinStorage[K comparable](ttl time.Duration, renewTTL bool) JoinStorage[K] {
	joinStorage := &HashMapJoinStorage[K]{
		storage1: make(map[K]*Item),
		ttl:      ttl,
		renewTTL: renewTTL,
	}
	if ttl > 0 {
		joinStorage.storage2 = make(map[K]*Item)
		joinStorage.timer = time.AfterFunc(ttl, joinStorage.rotate)
	}
	return joinStorage
}

func (s *HashMapJoinStorage[K]) rotate() {
	newStorage := make(map[K]*Item)
	s.rotateLock.Lock()
	defer s.rotateLock.Unlock()
	s.storage2 = s.storage1
	s.storage1 = newStorage
	s.timer.Reset(s.ttl)
}

func (s *HashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}, f JoinValueFunc) {
	if s.ttl > 0 {
		s.rotateLock.RLock()
		defer s.rotateLock.RUnlock()
	}
	for {
		item, storage := func() (*Item, map[K]*Item) {

			item, storage := func() (*Item, map[K]*Item) {
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
				newItem := &Item{
					values: make([][]interface{}, index+1, 2),
				}
				s.lock.Lock()
				defer s.lock.Unlock()
				if s.ttl > 0 {
					newItem.deadline = time.Now().Add(s.ttl)
				}
				if item == nil {
					item = s.storage1[key]
					if item != nil {
						if !time.Now().Before(item.deadline) {
							return item, s.storage1
						}
					}
				} else if &storage != &s.storage1 {
					delete(storage, key)
				}
				s.storage1[key] = newItem
				return item, s.storage1
			}
			return item, storage
		}()

		if func(item *Item) bool {
			item.lock.Lock()
			defer item.lock.Unlock()
			if !item.processed {
				if len(item.values) <= index {
					item.values = append(item.values, make([][]interface{}, index-len(item.values)+1)...)
				}
				item.values[index] = append(item.values[index], value)
				item.processed = f(item.values)
				if item.processed {
					s.lock.Lock()
					defer s.lock.Unlock()
					delete(storage, key)
				} else if s.renewTTL { //Depend on logic: should we extend deadline after change or not
					s.lock.Lock()
					defer s.lock.Unlock()
					if &storage == &s.storage1 {
						delete(storage, key)
					}
					item.deadline = time.Now().Add(s.ttl)
					s.storage1[key] = item
				}
				return true
			}
			return false
		}(item) {
			break
		}
	}
}

func (s *HashMapJoinStorage[K]) Start(ctx context.Context) error {
	return nil
}

func (s *HashMapJoinStorage[K]) Stop(ctx context.Context) {
}
