/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kvstore

type KVStorage[K comparable] interface {
	SetValue1(key K)
	SetValue2(key K)
	Get(key K) bool
}
