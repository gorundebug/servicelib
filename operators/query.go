/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"context"
	"reflect"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

var _ runtime.TypedQueryStream[any, any, any] = (*QueryStream[any, any, any])(nil)

type QueryStream[T, R, E any] struct {
	runtime.ConsumedStream[R]
	source        runtime.TypedStream[T]
	queryConsumer runtime.Consumer[T]
	errorConsumer *ErrorStream[E]
}

func MakeQueryStream[T, R, E any](streamConfig *config.QueryStreamConfig, stream runtime.TypedStream[T]) (runtime.TypedQueryStream[T, R, E], error) {
	env := stream.GetRuntimeEnvironment()

	queryStream := &QueryStream[T, R, E]{
		ConsumedStream: runtime.MakeConsumedStream[R](streamConfig.ID, env, runtime.MakeSerde[R](env)),
		source:         stream,
		errorConsumer:  MakeErrorStream[E](streamConfig.ID, env),
	}
	if err := stream.SetConsumer(queryStream); err != nil {
		return nil, err
	}
	env.RegisterStream(queryStream)
	return queryStream, nil
}

func (s *QueryStream[T, R, E]) ConsumeResult(ctx context.Context, value R) {
	s.Emit(ctx, value)
}

func (s *QueryStream[T, R, E]) Consume(ctx context.Context, value T) {
	endpoint := s.GetRuntimeEnvironment().RuntimeConfig().GetEndpointConfigByID(s.GetEndpointId())
	if queryEndpoint, ok := endpoint.(*config.QueryEndpointConfig); ok && !queryEndpoint.Enabled {
		return
	}
	if s.queryConsumer != nil {
		if s.TracingEnabled(ctx) {
			newCtx, span := s.StartSpan(ctx, "stream.query")
			ctx = newCtx
			defer span.End()
		}
		s.queryConsumer.Consume(ctx, value)
	}
}

func (s *QueryStream[T, R, E]) GetErrorStream() runtime.TypedConsumedStream[E] {
	return s.errorConsumer
}

func (s *QueryStream[T, R, E]) FunctionImplementation() interface{} {
	return nil
}

func (s *QueryStream[T, R, E]) GetErrorConsumer() runtime.RuntimeStream {
	return s.errorConsumer
}

func (s *QueryStream[T, R, E]) GetValueType() reflect.Type {
	var r R
	return reflect.TypeOf(&r).Elem()
}

func (s *QueryStream[T, R, E]) GetKeyType() reflect.Type {
	return nil
}

func (s *QueryStream[T, R, E]) Stream() runtime.Stream {
	return s
}

func (s *QueryStream[T, R, E]) SetConsumer(consumer runtime.TypedStreamConsumer[R]) error {
	return s.SetDownstream(consumer, s)
}

func (s *QueryStream[T, R, E]) GetConsumers() []runtime.Stream {
	return s.ConsumedStream.GetConsumers()
}

// SetQueryConsumer binds the endpoint during graph construction, just as
// SetSinkConsumer does for Sink. The endpoint needs this stream to emit results,
// so it is constructed after the stream. This is not a per-request operation.
func (s *QueryStream[T, R, E]) SetQueryConsumer(consumer runtime.Consumer[T]) {
	s.queryConsumer = consumer
}

func (s *QueryStream[T, R, E]) GetEndpointId() int {
	return s.GetConfig().(*config.QueryStreamConfig).IdEndpoint
}
