/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
)

const pendingRotationInterval = 30 * time.Second

type HandlerData struct {
	Writer  http.ResponseWriter
	Request *http.Request
}

// HTTPHandler is the function type returned by MakeNetHTTPEndpointConsumer.
// It handles a single HTTP request for the endpoint.
type HTTPHandler func(w http.ResponseWriter, r *http.Request)

// StreamContext bundles the typed stream, result stream, output collector, and error collector
// that are passed to every EndpointHandler lifecycle method.
type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// ResultCallback is the callback type registered via ResultContext.SetResultCallback.
// It is called when a pipeline result R with a matching messageID arrives.
// Return true to deregister the callback after this invocation; false to keep it active.
type ResultCallback[HandlerState, ReqT, ResR, T, R, E any] func(
	ctx context.Context,
	sc StreamContext[T, R, E],
	handlerState HandlerState,
	value R,
	data HandlerData,
) bool

// ResultContext is given to EndpointHandler.ConsumeMessage.
// The handler registers result callbacks keyed by messageID.
// Done() signals that response processing is complete.
type ResultContext[HandlerState, ReqT, ResR, T, R, E any] interface {
	SetResultCallback(messageID string, cb ResultCallback[HandlerState, ReqT, ResR, T, R, E])
	Done()
}

type httpResult[HandlerState, ReqT, ResR, T, R, E any] struct {
	once               sync.Once
	handlerState       HandlerState
	data               HandlerData
	span               tracing.Span
	doneCh             chan struct{}
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap map[string]ResultCallback[HandlerState, ReqT, ResR, T, R, E]
}

func (r *httpResult[HandlerState, ReqT, ResR, T, R, E]) SetResultCallback(messageID string, cb ResultCallback[HandlerState, ReqT, ResR, T, R, E]) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap[messageID] = cb
}

func (r *httpResult[HandlerState, ReqT, ResR, T, R, E]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done")
		close(r.doneCh)
	})
}

// EndpointHandler handles HTTP requests for a single endpoint.
//
// Pipeline lifecycle (pipeline result expected):
//
//	BeginRequest → ConsumeMessage → [await result] → EndRequest
//	                    │                  ↑
//	                    └─ SetResultCallback → cb(R) → write response
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
// BeginRequest initialises per-request handler state and receives HandlerData
// (the http.ResponseWriter and *http.Request). If it returns a non-nil error
// the pipeline will not be started: the framework will NOT call ConsumeMessage
// or EndRequest. BeginRequest is therefore responsible for releasing any
// resources it acquired and writing any error response before returning the error.
//
// ConsumeMessage is called once per HTTP request. It receives HandlerData and
// may push decoded values into the pipeline via sc.Collect, write a response
// directly via data.Writer, or register a callback via resultCtx.SetResultCallback
// to be notified when a matching pipeline result arrives asynchronously. If it
// returns a non-nil error the framework stops processing: EndRequest is called
// with that error.
//
// EndRequest finalises the request. It receives the error that caused the
// pipeline to stop (nil on the happy path). Unlike gRPC handlers, EndRequest
// does not return an error; any error response must be written to data.Writer
// directly.
//
// GetMessageID correlates an inbound pipeline result value R with the originating
// request, enabling the framework to route the result back via the correct
// ResultContext callback.
//
// Thread safety:
// GetMessageID and the ResultContext callbacks registered via SetResultCallback
// may be called concurrently from multiple goroutines (one per pipeline result
// that arrives for the same request), and may also run concurrently with an
// in-progress ConsumeMessage call. Implementations must synchronise all access
// to HandlerState inside these methods.
// BeginRequest, ConsumeMessage, and EndRequest are called sequentially from a
// single goroutine per request, so no synchronisation is needed among them —
// but access to HandlerState from these methods still requires synchronisation
// if it is shared with GetMessageID or a registered callback.
type EndpointHandler[HandlerState, ReqT, ResR, T, R, E any] interface {
	BeginRequest(ctx context.Context, sc StreamContext[T, R, E], data HandlerData) (context.Context, HandlerState, error)
	ConsumeMessage(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, data HandlerData, resultCtx ResultContext[HandlerState, ReqT, ResR, T, R, E]) error
	GetMessageID(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, value R) string
	EndRequest(ctx context.Context, sc StreamContext[T, R, E], err error, handlerState HandlerState, data HandlerData)
}

type netHTTPInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type netHTTPEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
	serveHTTP(w http.ResponseWriter, r *http.Request)
}

type netHTTPInputDataSource interface {
	runtime.DataSource
	registerHandler(endpoint runtime.Endpoint, handler http.Handler)
}

type netHTTPDataSource struct {
	*runtime.InputDataSource
	useDedicatedListener bool
	server               *http.Server
	mux                  *http.ServeMux
	addr                 string
	done                 chan struct{}
	metricsEngine        metrics.MetricsEngine
	tracingEngine        tracing.TracingEngine
}

type netHTTPEndpoint struct {
	*runtime.DataSourceEndpoint
	consumer netHTTPEndpointConsumer
	method   string
}

type resultConsumer[T any] interface {
	consumeResult(ctx context.Context, value T)
}

type resultConsumerProxy[T any] struct {
	consumer resultConsumer[T]
}

func (c *resultConsumerProxy[T]) Consume(ctx context.Context, value T) {
	c.consumer.consumeResult(ctx, value)
}

type netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	sc        StreamContext[T, R, E]
	hasResult bool
	handler   EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	pending   *store.RotatingMap[string, *httpResult[HandlerState, ReqT, ResR, T, R, E]]
	tracer    tracing.Tracer
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Out(ctx context.Context, value T) {
	ec.Consume(ctx, value)
}

func getOrCreateNetHTTPDataSource(id int, env runtime.RuntimeEnvironment) (netHTTPInputDataSource, error) {
	if existing := env.GetDataSource(id); existing != nil {
		ds, ok := existing.(netHTTPInputDataSource)
		if !ok {
			return nil, fmt.Errorf("datasource with id=%d is not a netHTTPInputDataSource", id)
		}
		return ds, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for datasource with id=%d not found", id)
	}
	dsCfg, ok := cfg.(*config.HttpDataConnectorConfig)
	if !ok || dsCfg.Implementation != api.DataConnectorImplementationNetHTTP {
		return nil, fmt.Errorf("invalid config type for datasource %q", cfg.GetName())
	}

	inputDS, err := runtime.MakeInputDataSource(cfg, env)
	if err != nil {
		return nil, err
	}
	ds := &netHTTPDataSource{
		InputDataSource:      inputDS,
		useDedicatedListener: dsCfg.UseDedicatedListener,
		metricsEngine:        env.MetricsEngine(),
		tracingEngine:        env.TracingEngine(),
	}
	if dsCfg.UseDedicatedListener {
		if dsCfg.Host == "" || dsCfg.Port == 0 {
			return nil, fmt.Errorf("no host or port specified for data connector %q", dsCfg.GetName())
		}
		ds.mux = http.NewServeMux()
		ds.addr = fmt.Sprintf("%s:%d", dsCfg.Host, dsCfg.Port)
		ds.done = make(chan struct{})
	}
	env.AddDataSource(ds)
	return ds, nil
}

func createNetHTTPDataSourceEndpoint(id int, env runtime.RuntimeEnvironment) (*netHTTPEndpoint, error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for endpoint with id=%d not found", id)
	}
	epCfg, ok := cfg.(*config.HttpEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for endpoint %q", cfg.GetName())
	}

	dataSource, err := getOrCreateNetHTTPDataSource(epCfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", epCfg.GetName())
	}
	if epCfg.HttpMethodType != api.HTTPMethodTypePOST && epCfg.HttpMethodType != api.HTTPMethodTypeGET {
		return nil, fmt.Errorf("no method specified for http endpoint %q", epCfg.GetName())
	}
	if epCfg.Path == "" {
		return nil, fmt.Errorf("no path specified for http endpoint %q", epCfg.GetName())
	}
	method, err := getHttpMethod(epCfg.HttpMethodType)
	if err != nil {
		return nil, err
	}
	sourceEndpoint, err := runtime.MakeDataSourceEndpoint(dataSource, id, env)
	if err != nil {
		return nil, err
	}
	ep := &netHTTPEndpoint{
		DataSourceEndpoint: sourceEndpoint,
		method:             method,
	}
	var inputEndpoint runtime.InputEndpoint = ep
	dataSource.AddEndpoint(inputEndpoint)
	dataSource.registerHandler(ep, http.HandlerFunc(ep.ServeHTTP))

	return ep, nil
}

func (ds *netHTTPDataSource) Start(ctx context.Context) error {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(netHTTPInputEndpoint).Start(ctx); err != nil {
			return err
		}
	}

	if !ds.useDedicatedListener {
		return nil
	}

	var handler http.Handler = ds.mux
	if ds.metricsEngine != nil {
		handler = ds.metricsEngine.HTTPServerHandler(ds.mux, runtime.ToSnakeCase(ds.GetName()))
	}
	if ds.tracingEngine != nil {
		handler = ds.tracingEngine.HTTPServerHandler(handler, runtime.ToSnakeCase(ds.GetName()))
		inner := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Trace") != "" {
				r = r.WithContext(tracing.EnableSampling(r.Context()))
			}
			inner.ServeHTTP(w, r)
		})
	}
	addr := ds.addr
	if addr == "" {
		addr = ":http"
	}
	ds.server = &http.Server{Addr: addr, Handler: handler}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		err := ds.server.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			panic(fmt.Sprintf("%v", err))
		}
		ds.done <- struct{}{}
	}()
	return nil
}

func (ds *netHTTPDataSource) registerHandler(ep runtime.Endpoint, handler http.Handler) {
	if ds.useDedicatedListener {
		path := ds.GetEnvironment().RuntimeConfig().GetEndpointConfigByID(ep.GetID()).(*config.HttpEndpointConfig).Path
		ds.mux.Handle(path, handler)
	}
}

func (ds *netHTTPDataSource) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		endpoints.At(i).(netHTTPInputEndpoint).Stop(ctx)
	}

	if !ds.useDedicatedListener {
		return
	}

	go func() {
		if err := ds.server.Shutdown(ctx); err != nil {
			ds.GetEnvironment().Log().Warnf("netHTTPDataSource.Stop server shutdown: %s", err.Error())
		}
	}()
	select {
	case <-ds.done:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

func (ep *netHTTPEndpoint) Start(ctx context.Context) error {
	return ep.consumer.Start(ctx)
}

func (ep *netHTTPEndpoint) Stop(ctx context.Context) {
	ep.consumer.Stop(ctx)
}

func getHttpMethod(method api.HTTPMethodType) (string, error) {
	switch method {
	case api.HTTPMethodTypeGET:
		return "GET", nil
	case api.HTTPMethodTypePOST:
		return "POST", nil
	}
	return "", fmt.Errorf("invalid http method %v", method)
}

func (ep *netHTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != ep.method {
		ep.OnInvalidHTTPMethod(r.Context(), r.Method)
		return
	}
	ep.consumer.serveHTTP(w, r)
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *httpResult[HandlerState, ReqT, ResR, T, R, E]](pendingRotationInterval)
		if err := ec.pending.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) serveHTTP(w http.ResponseWriter, r *http.Request) {
	data := HandlerData{Writer: w, Request: r}
	reqCtx := r.Context()
	if _, ok := runtime.StreamIdFromContext(reqCtx); !ok {
		if sid := r.Header.Get("x-stream-id"); sid != "" {
			reqCtx = runtime.WithStreamId(reqCtx, sid)
		}
	}
	var span tracing.Span
	if ec.tracer != nil {
		reqCtx, span = ec.tracer.Start(reqCtx, "http.input",
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
			tracing.StringAttr("method", r.Method),
			tracing.StringAttr("path", r.URL.Path),
		)
		defer span.End()
	}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(reqCtx, ec.sc, data)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
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
	result := &httpResult[HandlerState, ReqT, ResR, T, R, E]{
		handlerState:       handlerState,
		data:               data,
		span:               span,
		doneCh:             doneCh,
		messageCallbackMap: make(map[string]ResultCallback[HandlerState, ReqT, ResR, T, R, E]),
	}
	if ec.hasResult {
		ec.pending.Set(streamID, result)
	}

	if err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, data, result); err != nil {
		if ec.hasResult {
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
		}
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState, data)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "consume_message")

	if !ec.hasResult {
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState, data)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
		return
	}

	select {
	case <-doneCh:
		tracing.SpanEvent(span, "done")
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState, data)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
	case <-handlerCtx.Done():
		tracing.SpanError(span, handlerCtx.Err())
		if span != nil {
			tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
		}
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState, data)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, handlerCtx.Err())
	}
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) consumeResult(ctx context.Context, value R) {
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
	if cb(ctx, ec.sc, result.handlerState, value, result.data) {
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

func MakeNetHTTPEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], HTTPHandler, error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createNetHTTPDataSourceEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, nil, err
	}
	if handler == nil {
		return nil, nil, fmt.Errorf("handler is nil for the http endpoint %q", endpoint.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](endpoint, stream),
		hasResult:                  stream.GetResultStream() != nil,
		handler:                    handler,
		tracer:                     tr,
	}
	ec.sc = runtime.MakeStreamContext[T, R, E](
		ec.Stream(),
		ec.Stream().GetResultStream(),
		runtime.CollectFunc[T](ec.Out),
		runtime.CollectFunc[E](ec.Stream().GetErrorStream().Consume),
	)
	if ec.hasResult {
		stream.SetResultConsumer(&resultConsumerProxy[R]{consumer: ec})
	}
	endpoint.consumer = ec
	env.RegisterEndpointConsumer(ec)
	return ec, ec.serveHTTP, nil
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *netHTTPEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
