/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import "context"

type Collect[T any] interface {
	Out(ctx context.Context, value T)
}

type CollectFunc[T any] func(context.Context, T)

func (f CollectFunc[T]) Out(ctx context.Context, v T) {
	f(ctx, v)
}

type collector[T any] struct {
	caller Caller[T]
}

func (c *collector[T]) Out(ctx context.Context, value T) {
	if c.caller != nil {
		c.caller.Consume(ctx, value)
	}
}

func MakeCollector[T any](
	caller Caller[T],
) Collect[T] {
	return &collector[T]{
		caller: caller,
	}
}
