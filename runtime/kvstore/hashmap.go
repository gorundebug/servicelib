/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kvstore

import "time"

type Item[T1, T2 any] struct {
	v1   []T1
	v2   []T2
	time time.Time
}

type HashMapKVStorage[K comparable, T1, T2 any] struct {
	storage map[K]*Item[T1, T2]
}

/*
func MakeHashMapKVStorage[K comparable, T1, T2 any](ttl time.Duration) KVStorage[K, V] {

}
*/
func (s *HashMapKVStorage[K, T1, T2]) Set(key K, v1 T1, v2 T2) {
}
