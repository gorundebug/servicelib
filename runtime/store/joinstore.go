/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

type JoinStorage[K comparable] interface {
	JoinValue(key K, index int, value interface{}) [][]interface{}
}
