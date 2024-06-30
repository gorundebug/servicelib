/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type StreamFunction[T any] struct {
	context Stream[T]
}

func (f *StreamFunction[T]) BeforeCall() {
}

func (f *StreamFunction[T]) AfterCall() {
}
