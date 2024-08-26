/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"sync"
	"time"
)

type Item struct {
	values [][]interface{}
	time   time.Time
	lock   sync.Mutex
}

func (item *Item) SetValue(index int, value interface{}, f JoinValueFunc) bool {
	item.lock.Lock()
	defer item.lock.Unlock()
	if len(item.values) <= index {
		item.values = append(item.values, make([][]interface{}, index-len(item.values)+1)...)
	}
	item.values[index] = append(item.values[index], value)
	return f(item.values)
}

type HashMapJoinStorage[K comparable] struct {
	storage map[K]*Item
	ttl     time.Duration
	lock    sync.Mutex
}

func (s *HashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}, f JoinValueFunc) {
	item := func() *Item {
		s.lock.Lock()
		defer s.lock.Unlock()
		item := s.storage[key]
		if item == nil {
			item = &Item{
				values: make([][]interface{}, index+1, 2),
			}
			s.storage[key] = item
		}
		return item
	}()
	if item.SetValue(index, value, f) {
		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.storage, key)
	}
}

func MakeHashMapJoinStorage[K comparable](ttl time.Duration) JoinStorage[K] {
	return &HashMapJoinStorage[K]{
		storage: make(map[K]*Item),
		ttl:     ttl,
	}
}
