/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
    "context"

    "github.com/gorundebug/servicelib/runtime"
)

var _ runtime.TypedConsumedStream[any] = (*ErrorStream[any])(nil)

type ErrorStream[T any] struct {
    runtime.ConsumedStream[T]
}

func MakeErrorStream[T any](id int, env runtime.RuntimeEnvironment) *ErrorStream[T] {
    return &ErrorStream[T]{
        ConsumedStream: runtime.MakeConsumedStream[T](id, env, runtime.MakeSerde[T](env)),
    }
}

func (s *ErrorStream[T]) Stream() runtime.Stream {
    return s
}

func (s *ErrorStream[T]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) {
    s.SetDownstream(consumer, s)
}

func (s *ErrorStream[T]) Consume(ctx context.Context, value T) {
    s.Emit(ctx, value)
}
