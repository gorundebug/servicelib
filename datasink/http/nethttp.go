/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

// Requester builds the outgoing HTTP request inside ConsumeMessage.
// Call NewRequest to initialise the request; use Header to add headers afterwards.
// Handler code only needs the io package — no net/http import required.
type Requester struct {
	req *http.Request
}

// NewRequest creates the HTTP request. Equivalent to http.NewRequest.
// The returned *http.Request may be used to set headers, query parameters, or any
// other request fields; the caller does not need to import net/http to do so.
func (r *Requester) NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	r.req = req
	return req, nil
}

// Response wraps the HTTP response returned by the server.
// StatusCode, Status, Header, Body, and all other fields are accessible directly;
// handler code does not need to import net/http.
type Response struct {
	*http.Response
}

// StreamContext bundles the result stream, output collector, and error collector
// that are passed to every EndpointHandler lifecycle method.
// Stream() provides serde access for the result type R.
type StreamContext[T, R, E any] = runtime.SinkStreamContext[T, R, E]

// EndpointHandler handles outgoing HTTP sink calls for a single endpoint.
//
// Pipeline lifecycle (one HTTP call per Consume):
//
//	BeginRequest → ConsumeMessage → [HTTP call] → HandleResponse → EndRequest
//
// BeginRequest initialises per-request handler state. If it returns a non-nil error
// the pipeline will not proceed: the framework will NOT call ConsumeMessage,
// HandleResponse, or EndRequest. BeginRequest is responsible for releasing any
// resources it acquired before returning the error.
//
// ConsumeMessage converts value T into an HTTP request by calling req.NewRequest.
// Optionally set headers via req.Header(). The framework sends the request after
// ConsumeMessage returns. Returning a non-nil error skips the HTTP call and passes
// the error directly to EndRequest.
//
// HandleResponse is called once with the HTTP response returned by the server.
// Push decoded results into the pipeline via sc.Collect. Returning a non-nil error
// passes it to EndRequest. The response body is automatically closed by the framework
// after HandleResponse returns.
//
// EndRequest finalises the request. Its error argument is the first non-nil error
// encountered (from ConsumeMessage, the HTTP call, or HandleResponse), or nil on the
// happy path. EndRequest does not return an error.
//
// Thread safety: BeginRequest, ConsumeMessage, HandleResponse, and EndRequest are all
// called sequentially from a single goroutine per request; no synchronisation is needed
// among these methods.
type EndpointHandler[HandlerState, ReqT, ResR, T, R, E any] interface {
	BeginRequest(ctx context.Context, sc StreamContext[T, R, E]) (context.Context, HandlerState, error)
	ConsumeMessage(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, value T, req *Requester) error
	HandleResponse(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, resp Response) error
	EndRequest(ctx context.Context, sc StreamContext[T, R, E], err error, handlerState HandlerState)
}

type netHTTPSinkEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type netHTTPSinkEndpointIface interface {
	runtime.SinkEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type netHTTPOutputDataSink interface {
	runtime.DataSink
	WaitGroup() *sync.WaitGroup
}

type netHTTPSinkDataSink struct {
	*runtime.OutputDataSink
	wg sync.WaitGroup
}

func (ds *netHTTPSinkDataSink) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

type netHTTPSinkEndpoint struct {
	*runtime.DataSinkEndpoint
	consumer netHTTPSinkEndpointConsumer
}

type netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E any] struct {
	endpoint runtime.SinkEndpoint
	stream   runtime.TypedSinkStreamWithResult[T, R, E]
	sc       StreamContext[T, R, E]
	handler  EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
	client   Client
	tracer   tracing.Tracer
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Endpoint() runtime.SinkEndpoint {
	return ec.endpoint
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Consume(ctx context.Context, value T) {
	var span tracing.Span
	if ec.tracer != nil {
		ctx, span = ec.tracer.Start(ctx, "http.output",
			tracing.StringAttr("stream", ec.stream.GetName()),
			tracing.StringAttr("endpoint", ec.endpoint.GetName()),
		)
		defer span.End()
	}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		ec.endpoint.OnBeginRequestFailed(ctx, err)
		return
	}
	tracing.SpanEvent(span, "begin_request")
	startTime := ec.endpoint.OnRequestStart(handlerCtx)

	requester := &Requester{}
	if err := ec.handler.ConsumeMessage(handlerCtx, ec.sc, handlerState, value, requester); err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "consume_message")

	if requester.req == nil {
		err := fmt.Errorf("no HTTP request set by handler for sink endpoint %q",
			ec.endpoint.GetName())
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "no_request.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}

	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		requester.req.Header.Set("x-stream-id", sid.GetID())
	}

	httpResp, err := ec.client.Do(requester.req)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "http_call.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	if span != nil {
		tracing.SpanEvent(span, "http_call", tracing.Int64Attr("status_code", int64(httpResp.StatusCode)))
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			ec.endpoint.GetRuntimeEnvironment().Log().Error(err)
		}
	}(httpResp.Body)

	if err := ec.handler.HandleResponse(handlerCtx, ec.sc, handlerState, Response{httpResp}); err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "handle_response.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.endpoint.OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "handle_response")

	ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
	ec.endpoint.OnRequestEnd(handlerCtx, startTime, nil)
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Start(_ context.Context) error {
	return nil
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) Stop(_ context.Context) {
}

func (ds *netHTTPSinkDataSink) Start(ctx context.Context) error {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(netHTTPSinkEndpointIface).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *netHTTPSinkDataSink) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(ep netHTTPSinkEndpointIface) {
			defer ds.wg.Done()
			ep.Stop(ctx)
		}(endpoints.At(i).(netHTTPSinkEndpointIface))
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

func (ep *netHTTPSinkEndpoint) Start(ctx context.Context) error {
	return ep.consumer.Start(ctx)
}

func (ep *netHTTPSinkEndpoint) Stop(ctx context.Context) {
	ep.consumer.Stop(ctx)
}

func getOrCreateNetHTTPSinkDataSink(id int, env runtime.RuntimeEnvironment) (runtime.DataSink, error) {
	dataSink := env.GetDataSink(id)
	if dataSink != nil {
		return dataSink, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for datasink with id=%d not found", id)
	}
	dsCfg, ok := cfg.(*config.HttpDataConnectorConfig)
	if !ok || dsCfg.Implementation != api.DataConnectorImplementationNetHTTP {
		return nil, fmt.Errorf("invalid config type for datasink %q", cfg.GetName())
	}
	outputDS, err := runtime.MakeOutputDataSink(cfg, env)
	if err != nil {
		return nil, err
	}
	ds := &netHTTPSinkDataSink{
		OutputDataSink: outputDS,
	}
	var outputDataSink netHTTPOutputDataSink = ds
	env.AddDataSink(outputDataSink)
	return ds, nil
}

func createNetHTTPSinkEndpoint(id int, env runtime.RuntimeEnvironment) (*netHTTPSinkEndpoint, error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for endpoint with id=%d not found", id)
	}
	epCfg, ok := cfg.(*config.HttpEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for endpoint %q", cfg.GetName())
	}
	dataSink, err := getOrCreateNetHTTPSinkDataSink(epCfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", epCfg.GetName())
	}
	sinkEndpoint, err := runtime.MakeDataSinkEndpoint(dataSink, id, env)
	if err != nil {
		return nil, err
	}
	ep := &netHTTPSinkEndpoint{
		DataSinkEndpoint: sinkEndpoint,
	}
	dataSink.AddEndpoint(ep)
	return ep, nil
}

// MakeNetHTTPSinkEndpointConsumer creates an HTTP sink consumer that, for each
// pipeline value T, invokes handler to build and send an outgoing HTTP request,
// then passes the response back through HandleResponse.
func MakeNetHTTPEndpointConsumer[HandlerState, ReqT, ResR, T, R, E any](
	stream runtime.TypedSinkStreamWithResult[T, R, E],
	client Client,
	handler EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for NetHTTPSinkEndpointConsumer for the stream %q", stream.GetName())
	}
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createNetHTTPSinkEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	if _, ok := endpoint.GetConfig().(*config.HttpEndpointConfig); !ok {
		return nil, fmt.Errorf("invalid endpoint config type for NetHTTPSinkEndpointConsumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	ec := &netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]{
		endpoint: endpoint,
		stream:   stream,
		handler:  handler,
		client:   client,
		tracer:   tr,
	}
	ec.sc = runtime.MakeSinkStreamContext[T, R, E](
		stream,
		runtime.CollectFunc[R](stream.ConsumeResult),
		runtime.CollectFunc[E](stream.GetErrorStream().Consume),
	)
	stream.SetSinkConsumer(ec)
	endpoint.consumer = ec
	env.RegisterEndpointConsumer(ec)
	return ec, nil
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) GetID() int {
	return ec.endpoint.GetID()
}

func (ec *netHTTPSinkEndpointTypedConsumer[HandlerState, ReqT, ResR, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
