// Package dataquery implements the request lifecycle shared by storage Query
// endpoints. It is independent of DataSink and creates no delivery queues.
package dataquery

import (
	"context"
	"fmt"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"time"
)

// EndpointHandler owns application state for one invocation. EndRequest is
// called once, including after BeginRequest fails, with the returned state and
// error. It maps errors to the normal error collector and may emit an application
// completion value through out. No automatic completion value is emitted.
//
// Each invocation has independent state. All calls for that invocation are
// sequential. Neither collector may be retained beyond EndRequest.
// A failed BeginRequest must return a usable context and clean up any resources
// not represented by its returned state. Cancellation is never detached.
type EndpointHandler[HandlerState, R, E any] interface {
	BeginRequest(context.Context, runtime.Stream) (context.Context, HandlerState, error)
	EndRequest(ctx context.Context, stream runtime.Stream, state HandlerState, err error, out runtime.Collect[R], errors runtime.Collect[E])
}

// Execute is implemented by a storage adapter. It prepares/executes the native
// request and emits mapped results through out. Before returning it must close
// its rows, cursor or response body, including on cancellation and partial error.
// It must not retry a request after emitting results or enqueue work beyond its
// lifetime. The adapter receives the original request and invocation state.
type Execute[HandlerState, T, R any] func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) error

type collector[T any] struct{ emit func(context.Context, T) }

func (c collector[T]) Out(ctx context.Context, value T) { c.emit(ctx, value) }

// Options configures one complete adapter operation, including native I/O and
// application mapping. Zero adds no deadline. Cancellation remains cooperative;
// user callbacks must honor their supplied context. EndRequest uses the original
// handler context after operation resources have been released.
type Options struct {
	OperationTimeout time.Duration
}

type endpointConsumer[HandlerState, T, R, E any] struct {
	stream    runtime.TypedQueryStream[T, R, E]
	handler   EndpointHandler[HandlerState, R, E]
	execute   Execute[HandlerState, T, R]
	options   Options
	telemetry *endpointMetrics
}

// MakeEndpointConsumer connects a storage adapter to an independent Query
// operator. Resource startup/stop belongs to the connector, not this invocation.
func MakeEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], handler EndpointHandler[HandlerState, R, E], execute Execute[HandlerState, T, R], options ...Options) (runtime.Consumer[T], error) {
	if stream == nil || handler == nil || execute == nil {
		return nil, fmt.Errorf("query endpoint requires stream, handler and execute")
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("query endpoint accepts one options value")
	}
	var settings Options
	if len(options) == 1 {
		settings = options[0]
	}
	if settings.OperationTimeout < 0 {
		return nil, fmt.Errorf("query operation timeout cannot be negative")
	}
	telemetry, err := makeEndpointMetrics(stream.GetRuntimeEnvironment(), stream.GetEndpointId)
	if err != nil {
		return nil, fmt.Errorf("initialize Query metrics: %w", err)
	}
	consumer := &endpointConsumer[HandlerState, T, R, E]{stream: stream, handler: handler, execute: execute, options: settings, telemetry: telemetry}
	stream.SetQueryConsumer(consumer)
	return consumer, nil
}

func (c *endpointConsumer[HandlerState, T, R, E]) Consume(ctx context.Context, value T) {
	started := c.telemetry.begin(ctx)
	var outcome error
	defer func() { c.telemetry.finish(ctx, started, outcome) }()
	out := collector[R]{emit: c.stream.ConsumeResult}
	if !started.IsZero() {
		out.emit = func(resultCtx context.Context, result R) {
			if metrics.RecordingEnabled(resultCtx) {
				c.telemetry.results.Inc(resultCtx)
			}
			c.stream.ConsumeResult(resultCtx, result)
		}
	}
	errors := collector[E]{emit: c.stream.GetErrorStream().Consume}
	handlerCtx, state, err := c.handler.BeginRequest(ctx, c.stream)
	// Invalid application context must not turn cancellation into a fresh root.
	if handlerCtx == nil {
		handlerCtx = ctx
		if err == nil {
			err = fmt.Errorf("query BeginRequest returned a nil context")
		}
	}
	if err == nil {
		err = handlerCtx.Err()
	}
	if err == nil {
		operationCtx := handlerCtx
		cancel := func() {}
		if c.options.OperationTimeout > 0 {
			operationCtx, cancel = context.WithTimeout(handlerCtx, c.options.OperationTimeout)
		}
		// Execute returns only after native resources are released. Never detach
		// a mapper in order to report a timeout before its work has stopped.
		func() {
			defer cancel()
			err = c.execute(operationCtx, state, value, out)
			if err == nil {
				err = operationCtx.Err()
			}
		}()
	}
	outcome = err
	c.handler.EndRequest(handlerCtx, c.stream, state, err, out, errors)
}
