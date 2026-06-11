/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsource

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
)

const pendingRotationInterval = 30 * time.Second

type DataProducer[T any] interface {
	Start(ctx context.Context, consumer runtime.Consumer[T]) error
	Stop(context.Context)
}

// StreamContext bundles the typed stream, result stream, output collector, and error collector
// that are passed to every EndpointHandler lifecycle method.
type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// ResultCallback is the callback type registered via ResultContext.SetResultCallback.
// It is called when a pipeline result R with a matching messageID arrives.
// Return true to deregister the callback after this invocation; false to keep it active.
type ResultCallback[HandlerState, T, R, E any] func(
	ctx context.Context,
	sc StreamContext[T, R, E],
	handlerState HandlerState,
	value R,
) bool

// ResultContext is given to EndpointHandler.ConsumeMessage.
// The handler registers result callbacks keyed by messageID.
// Done() signals that response processing is complete.
type ResultContext[HandlerState, T, R, E any] interface {
	SetResultCallback(messageID string, cb ResultCallback[HandlerState, T, R, E])
	Done()
}

type customResult[HandlerState, T, R, E any] struct {
	once               sync.Once
	handlerState       HandlerState
	span               tracing.Span
	doneCh             chan struct{}
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap map[string]ResultCallback[HandlerState, T, R, E]
}

func (r *customResult[HandlerState, T, R, E]) SetResultCallback(messageID string, cb ResultCallback[HandlerState, T, R, E]) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap[messageID] = cb
}

func (r *customResult[HandlerState, T, R, E]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done_called")
		close(r.doneCh)
	})
}

// EndpointHandler handles custom (local) source messages for a single endpoint.
//
// Pipeline lifecycle (pipeline result expected):
//
//	BeginRequest → ConsumeMessage → [await result] → EndRequest
//	                    │                  ↑
//	                    └─ SetResultCallback → cb(R)
//
// The framework blocks after ConsumeMessage until Done() is called on the
// ResultContext (either directly in ConsumeMessage, or via a registered callback
// when a matching pipeline result R arrives). EndRequest is called only after
// Done() is signalled or the context is cancelled.
//
// Pipeline lifecycle (no pipeline result):
//
//	BeginRequest → ConsumeMessage → EndRequest
//
// Concurrency returns the maximum number of values processed in parallel.
// It is called on every incoming value so the limit can change dynamically.
// Returning 0 means unlimited concurrency.
//
// BeginRequest initialises per-value handler state. If it returns a non-nil
// error the pipeline will not be started: the framework will NOT call
// ConsumeMessage or EndRequest. BeginRequest is therefore responsible for
// releasing any resources it acquired before returning the error.
//
// ConsumeMessage is called once per value T produced by the DataProducer. It may
// push values into the pipeline via sc.Collect, or register a callback via
// resultCtx.SetResultCallback to be notified when a matching pipeline result arrives
// asynchronously. If it returns a non-nil error the framework stops processing:
// EndRequest is called with that error.
//
// EndRequest finalises the value handling. It receives the error that caused the
// pipeline to stop (nil on the happy path). Unlike gRPC handlers, EndRequest does
// not return an error.
//
// GetMessageID correlates an inbound pipeline result value R with the originating
// value, enabling the framework to route the result back via the correct
// ResultContext callback.
//
// Thread safety:
// GetMessageID and the ResultContext callbacks registered via SetResultCallback
// may be called concurrently from multiple goroutines (one per pipeline result
// that arrives for the same value), and may also run concurrently with an
// in-progress ConsumeMessage call. Implementations must synchronise all access
// to HandlerState inside these methods.
// BeginRequest, ConsumeMessage, and EndRequest are called sequentially from a
// single goroutine per value, so no synchronisation is needed among them —
// but access to HandlerState from these methods still requires synchronisation
// if it is shared with GetMessageID or a registered callback.
type EndpointHandler[HandlerState, T, R, E any] interface {
	// Concurrency returns the maximum number of requests processed in parallel.
	// Called on every incoming request so the limit can change dynamically.
	// Returning 0 means unlimited.
	Concurrency(sc StreamContext[T, R, E]) int
	BeginRequest(ctx context.Context, sc StreamContext[T, R, E]) (context.Context, HandlerState, error)
	ConsumeMessage(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, value T, resultCtx ResultContext[HandlerState, T, R, E]) error
	GetMessageID(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, value R) string
	EndRequest(ctx context.Context, sc StreamContext[T, R, E], err error, handlerState HandlerState)
}

type resultConsumer[R any] interface {
	consumeResult(ctx context.Context, value R)
}

type resultConsumerProxy[R any] struct {
	consumer resultConsumer[R]
}

func (c *resultConsumerProxy[R]) Consume(ctx context.Context, value R) {
	c.consumer.consumeResult(ctx, value)
}

type customInputDataSource interface {
	runtime.DataSource
	waitGroup() *sync.WaitGroup
}

type customInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type customInputEndpointConsumer[T any] interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
	EndpointRequest(ctx context.Context, value T)
}

type customDataSource struct {
	*runtime.InputDataSource
	wg sync.WaitGroup
}

type customEndpoint[T any] struct {
	*runtime.DataSourceEndpoint
	consumer     customInputEndpointConsumer[T]
	dataProducer DataProducer[T]
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func (ep *customEndpoint[T]) Consume(ctx context.Context, value T) {
	ep.consumer.EndpointRequest(ctx, value)
}

func (ep *customEndpoint[T]) Start(ctx context.Context) error {
	if err := ep.consumer.Start(ctx); err != nil {
		return err
	}
	var cancelCtx context.Context
	cancelCtx, ep.cancel = context.WithCancel(ctx)
	ep.wg.Add(1)
	go func() {
		defer ep.wg.Done()
		if err := ep.dataProducer.Start(cancelCtx, ep); err != nil {
			ep.GetRuntimeEnvironment().Log().Error(cancelCtx, "data producer error",
				log.Str("endpoint", ep.GetName()), log.Err(err))
		}
	}()
	return nil
}

func (ep *customEndpoint[T]) Stop(ctx context.Context) {
	ep.consumer.Stop(ctx)
	ep.cancel()
	ep.dataProducer.Stop(ctx)
	c := make(chan struct{})
	go func() {
		defer close(c)
		ep.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ep.GetRuntimeEnvironment().Log().Warn(ctx, "custom source endpoint stopped by timeout", log.Str("endpoint", ep.GetName()))
	}
}

type customEndpointConsumer[HandlerState, T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	sc        StreamContext[T, R, E]
	hasResult bool
	handler   EndpointHandler[HandlerState, T, R, E]
	pending   *store.RotatingMap[string, *customResult[HandlerState, T, R, E]]
	concMu    sync.Mutex
	concCond  *sync.Cond
	active    int
	stopped   bool
	tracer    tracing.Tracer
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) Out(ctx context.Context, value T) {
	ec.Consume(ctx, value)
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *customResult[HandlerState, T, R, E]](pendingRotationInterval)
		return ec.pending.Start(ctx)
	}
	return nil
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
	ec.concMu.Lock()
	ec.stopped = true
	ec.concMu.Unlock()
	ec.concCond.Broadcast()
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) acquireConcurrency() bool {
	for {
		limit := ec.handler.Concurrency(ec.sc)

		ec.concMu.Lock()
		if ec.stopped {
			ec.concMu.Unlock()
			return false
		}
		if limit == 0 || ec.active < limit {
			ec.active++
			ec.concMu.Unlock()
			return true
		}
		ec.concCond.Wait()
		ec.concMu.Unlock()
	}
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) releaseConcurrency() {
	ec.concMu.Lock()
	ec.active--
	ec.concCond.Broadcast()
	ec.concMu.Unlock()
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) EndpointRequest(ctx context.Context, value T) {
	if !ec.acquireConcurrency() {
		return
	}
	defer ec.releaseConcurrency()

	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "local.input",
			tracing.StringAttr("stream", ec.Stream().GetName()),
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
		)
		defer span.End()
	}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		ec.Endpoint().OnBeginRequestFailed(ctx, err)
		return
	}
	tracing.SpanEvent(span, "begin_request")
	startTime := ec.Endpoint().OnRequestStart(handlerCtx)

	var streamID string
	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		streamID = sid.GetID()
	} else {
		streamID = runtime.NewStreamID()
		handlerCtx = runtime.WithStreamId(handlerCtx, streamID)
	}
	if span != nil {
		tracing.SpanAttrs(span, tracing.StringAttr("stream_id", streamID), tracing.BoolAttr("has_result", ec.hasResult))
	}

	doneCh := make(chan struct{})
	result := &customResult[HandlerState, T, R, E]{
		handlerState:       handlerState,
		span:               span,
		doneCh:             doneCh,
		messageCallbackMap: make(map[string]ResultCallback[HandlerState, T, R, E]),
	}
	if ec.hasResult {
		if err = ec.pending.Set(streamID, result); err != nil {
			tracing.SpanError(span, err)
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return
		}
		ec.Endpoint().OnPendingAdd(handlerCtx, streamID)
	}

	if err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, value, result); err != nil {
		if ec.hasResult {
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
			ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		}
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "consume_message")

	if !ec.hasResult {
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
		return
	}

	select {
	case <-doneCh:
		tracing.SpanEvent(span, "done_received")
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
	case <-handlerCtx.Done():
		tracing.SpanError(span, handlerCtx.Err())
		if span != nil {
			tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
		}
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, handlerCtx.Err())
	}
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) consumeResult(ctx context.Context, value R) {
	sid, ok := runtime.StreamIdFromContext(ctx)
	if !ok {
		ec.Endpoint().OnMissingStreamID(ctx)
		return
	}
	result, loaded := ec.pending.Get(sid.GetID())
	if !loaded {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		return
	}

	result.mu.RLock()
	defer result.mu.RUnlock()

	if res, ld := ec.pending.Get(sid.GetID()); !ld || res != result {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		tracing.SpanEvent(result.span, "late_result")
		return
	}

	messageID := ec.handler.GetMessageID(ctx, ec.sc, result.handlerState, value)

	result.cbMu.Lock()
	cb, ok := result.messageCallbackMap[messageID]
	result.cbMu.Unlock()
	if !ok || cb == nil {
		ec.Endpoint().OnUnknownMessageID(ctx, sid.GetID(), messageID)
		if result.span != nil {
			tracing.SpanEvent(result.span, "unknown_message_id", tracing.StringAttr("message_id", messageID))
		}
		return
	}
	if cb(ctx, ec.sc, result.handlerState, value) {
		result.cbMu.Lock()
		if _, exists := result.messageCallbackMap[messageID]; exists {
			delete(result.messageCallbackMap, messageID)
		} else {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), messageID)
			if result.span != nil {
				tracing.SpanEvent(result.span, "duplicate_message_id", tracing.StringAttr("message_id", messageID))
			}
		}
		result.cbMu.Unlock()
	}
	if result.span != nil {
		tracing.SpanEvent(result.span, "result_consumed", tracing.StringAttr("message_id", messageID))
	}
}

func (ds *customDataSource) Start(ctx context.Context) error {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(customInputEndpoint).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *customDataSource) waitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ds *customDataSource) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint customInputEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(customInputEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

func getOrCreateCustomDataSource(id int, env runtime.RuntimeEnvironment) (runtime.DataSource, error) {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for data source with id=%d not found", id)
	}
	inputDS, err := runtime.MakeInputDataSource(cfg, env)
	if err != nil {
		return nil, err
	}
	ds := &customDataSource{
		InputDataSource: inputDS,
	}
	var inputDataSource customInputDataSource = ds
	env.AddDataSource(inputDataSource)
	return ds, nil
}

func createCustomEndpoint[T any](id int, env runtime.RuntimeEnvironment, producer DataProducer[T]) (*customEndpoint[T], error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for source endpoint with id=%d not found", id)
	}
	epCfg, ok := cfg.(*config.CustomEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for source endpoint %q", cfg.GetName())
	}

	dataSource, err := getOrCreateCustomDataSource(epCfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", epCfg.GetName())
	}

	sourceEndpoint, err := runtime.MakeDataSourceEndpoint(dataSource, id, env)
	if err != nil {
		return nil, err
	}
	ep := &customEndpoint[T]{
		DataSourceEndpoint: sourceEndpoint,
		dataProducer:       producer,
	}
	var inputEndpoint customInputEndpoint = ep
	dataSource.AddEndpoint(inputEndpoint)
	return ep, nil
}

func MakeCustomEndpointConsumer[HandlerState, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	producer DataProducer[T],
	handler EndpointHandler[HandlerState, T, R, E],
) (runtime.Consumer[T], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createCustomEndpoint[T](stream.GetEndpointId(), env, producer)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for custom endpoint consumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	endpointConsumer := &customEndpointConsumer[HandlerState, T, R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](endpoint, stream),
		hasResult:                  stream.GetResultStream() != nil,
		handler:                    handler,
		tracer:                     tr,
	}
	endpointConsumer.concCond = sync.NewCond(&endpointConsumer.concMu)
	endpointConsumer.sc = runtime.MakeStreamContext[T, R, E](
		endpointConsumer.Stream(),
		endpointConsumer.Stream().GetResultStream(),
		runtime.CollectFunc[T](endpointConsumer.Out),
		runtime.CollectFunc[E](endpointConsumer.Stream().GetErrorStream().Consume),
	)
	if endpointConsumer.hasResult {
		stream.SetResultConsumer(&resultConsumerProxy[R]{consumer: endpointConsumer})
	}
	endpoint.consumer = endpointConsumer
	env.RegisterEndpointConsumer(endpointConsumer)
	return endpointConsumer, nil
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *customEndpointConsumer[HandlerState, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
