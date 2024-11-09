/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type Collection[T any] struct {
	data []T
}

func NewCollection[T any](data []T) Collection[T] {
	return Collection[T]{data: data}
}

func (s Collection[T]) Len() int {
	return len(s.data)
}

func (s Collection[T]) At(i int) T {
	return s.data[i]
}
