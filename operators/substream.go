/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package operators

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

var _ runtime.TypedSubStream[any, any] = (*SubStream[any, any])(nil)

// SubStream reuses ordinary graph execution and receives results through source.
// Only result delivery and caller completion are local to an invocation.
type SubStream[T, R any] struct {
	runtime.ConsumedStream[T]
	resultSource runtime.TypedStream[R]
}

type subStreamResult[T, R any] struct {
	streamLink
	entry *SubStream[T, R]
}

func (r *subStreamResult[T, R]) Consume(ctx context.Context, value R) {
	if call, ok := ctx.Value(r.entry).(*subStreamCall[R]); ok {
		call.deliver(value)
	}
}

type subStreamCall[R any] struct {
	mu        sync.Mutex
	ctx       context.Context
	collector runtime.SubStreamCollector[R]
	done      chan struct{}
	closed    bool
	completed bool
}

func (c *subStreamCall[R]) deliver(value R) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.ctx.Err() != nil {
		return
	}
	// Use the caller context, restoring the enclosing invocation for nesting.
	if c.collector.Out(c.ctx, value) {
		c.completed = true
		c.closed = true
		c.collector = nil
		close(c.done)
	}
}

// close also waits for an already-running callback before Consume returns.
func (c *subStreamCall[R]) close() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.collector = nil
	return c.completed
}

func MakeSubStream[T, R any](cfg *config.SubStreamConfig, env runtime.RuntimeEnvironment) (runtime.TypedSubStream[T, R], error) {
	if cfg == nil || env == nil {
		return nil, fmt.Errorf("SubStream requires configuration and a runtime environment")
	}
	if cfg.IdService != env.ServiceConfig().ID {
		return nil, fmt.Errorf("SubStream %q must belong to its runtime service", cfg.Name)
	}
	stream := &SubStream[T, R]{
		ConsumedStream: runtime.MakeConsumedStream[T](cfg.ID, env, runtime.MakeSerde[T](env)),
	}
	env.RegisterStream(stream)
	return stream, nil
}

func (s *SubStream[T, R]) Build() error {
	if s.GetConsumer() == nil {
		return fmt.Errorf("SubStream %q has no body consumer", s.GetName())
	}
	if s.resultSource == nil {
		return fmt.Errorf("SubStream %q has no result source", s.GetName())
	}
	return nil
}

func (s *SubStream[T, R]) Consume(ctx context.Context, value T, collector runtime.SubStreamCollector[R]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if collector == nil {
		return fmt.Errorf("SubStream %q requires a result collector", s.GetName())
	}
	if err := s.Build(); err != nil {
		return err
	}
	call := &subStreamCall[R]{ctx: ctx, collector: collector, done: make(chan struct{})}
	defer call.close()
	callCtx := context.WithValue(ctx, s, call)
	if s.TracingEnabled(callCtx) {
		tracedCtx, span := s.StartSpan(callCtx, "stream.substream")
		callCtx = tracedCtx
		defer span.End()
	}
	// Link semantics alone choose synchronous execution, a pool or parallelism.
	s.Emit(callCtx, value)
	select {
	case <-call.done:
		return nil
	case <-ctx.Done():
		// A callback that completed while cancellation arrived wins, like an
		// endpoint response. Do not return while that callback is still active.
		if call.close() {
			return nil
		}
		return ctx.Err()
	}
}

func (s *SubStream[T, R]) SetSource(source runtime.TypedStream[R]) error {
	if source == nil {
		return fmt.Errorf("SubStream %q requires a result source", s.GetName())
	}
	if s.resultSource != nil {
		return fmt.Errorf("SubStream %q already has a result source", s.GetName())
	}
	if source.GetID() == s.GetID() || source.GetConfig().GetIdService() != s.GetConfig().GetIdService() {
		return fmt.Errorf("SubStream %q requires a different result source in the same service", s.GetName())
	}
	link := &subStreamResult[T, R]{streamLink: streamLink{stream: s}, entry: s}
	if err := source.SetConsumer(link); err != nil {
		return err
	}
	s.resultSource = source
	return nil
}

func (s *SubStream[T, R]) SetConsumer(consumer runtime.TypedStreamConsumer[T]) error {
	return s.SetDownstream(consumer, s)
}

func (s *SubStream[T, R]) FunctionImplementation() interface{} { return nil }

func (s *SubStream[T, R]) GetErrorConsumer() runtime.RuntimeStream { return nil }

func (s *SubStream[T, R]) GetValueType() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

func (s *SubStream[T, R]) GetKeyType() reflect.Type { return nil }

func (s *SubStream[T, R]) Stream() runtime.Stream { return s }
