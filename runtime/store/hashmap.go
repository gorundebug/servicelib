/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import "time"

type Item struct {
	values [][]interface{}
	time   time.Time
}

type HashMapJoinStorage[K comparable] struct {
	storage     map[K]*Item
	valuesCount int
	ttl         time.Duration
}

func (s *HashMapJoinStorage[K]) JoinValue(key K, index int, value interface{}) [][]interface{} {
	return [][]interface{}{}
}

func MakeHashMapJoinStorage[K comparable](ttl time.Duration, valuesCount int) JoinStorage[K] {
	return &HashMapJoinStorage[K]{
		storage:     make(map[K]*Item),
		valuesCount: valuesCount,
		ttl:         ttl,
	}
}
