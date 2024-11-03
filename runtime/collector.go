/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

type Collect[T any] interface {
	Out(value T)
}

type collector[T any] struct {
	caller Caller[T]
}

func (c *collector[T]) Out(value T) {
	if c.caller != nil {
		c.caller.Consume(value)
	}
}

func makeCollector[T any](
	caller Caller[T]) *collector[T] {
	return &collector[T]{
		caller: caller,
	}
}

type parallelsCollector[T any] struct {
	caller Caller[T]
}

func (c *parallelsCollector[T]) Out(value T) {
	if c.caller != nil {
		go func(value T) {
			c.caller.Consume(value)
		}(value)
	}
}

func makeParallelsCollector[T any](
	caller Caller[T]) *parallelsCollector[T] {
	return &parallelsCollector[T]{
		caller: caller,
	}
}
