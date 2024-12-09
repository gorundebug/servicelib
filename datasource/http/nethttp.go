/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/schema"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/serde"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
)

type HandlerData struct {
	Writer  http.ResponseWriter
	Request *http.Request
}

type NetHTTPEndpointHandler[T any] interface {
	Handler(*HandlerData, runtime.Collect[T])
}

type NetHTTPEndpointRequestData interface {
	ResponseWriter() http.ResponseWriter
	Request() *http.Request
	GetBody() (io.ReadCloser, error)
	GetForm() (url.Values, error)
	GetQuery() url.Values
	GetMethod() string
}

type NetHTTPInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context) error
	Stop(context.Context)
}

type NetHTTPEndpointConsumer interface {
	runtime.InputEndpointConsumer
	EndpointRequest(requestData NetHTTPEndpointRequestData) error
	Start(context.Context) error
	Stop(context.Context)
}

type NetHTTPInputDataSource interface {
	runtime.DataSource
	AddHandler(pattern string, handler http.Handler)
}

type NetHTTPDataSource struct {
	*runtime.InputDataSource
	server http.Server
	mux    *http.ServeMux
	done   chan struct{}
}

type NetHTTPEndpoint struct {
	*runtime.DataSourceEndpoint
	method string
}

type netHTTPEndpointRequestData struct {
	w         http.ResponseWriter
	r         *http.Request
	body      []byte
	form      url.Values
	query     url.Values
	optimized bool
}

func (d *netHTTPEndpointRequestData) ResponseWriter() http.ResponseWriter {
	return d.w
}

func (d *netHTTPEndpointRequestData) GetMethod() string {
	return d.r.Method
}

func (d *netHTTPEndpointRequestData) GetBody() (io.ReadCloser, error) {
	if d.optimized {
		return d.r.Body, nil
	}
	if d.body == nil {
		var err error
		d.body, err = io.ReadAll(d.r.Body)
		if err != nil {
			return nil, err
		}
	}
	return io.NopCloser(bytes.NewReader(d.body)), nil
}

func (d *netHTTPEndpointRequestData) GetForm() (url.Values, error) {
	if d.form == nil {
		if err := d.r.ParseForm(); err != nil {
			return nil, err
		}
		d.form = d.r.Form
	}
	return d.form, nil
}

func (d *netHTTPEndpointRequestData) GetQuery() url.Values {
	if d.query == nil {
		d.query = d.r.URL.Query()
	}
	return d.query
}

func (d *netHTTPEndpointRequestData) Request() *http.Request {
	return d.r
}

type NetHTTPEndpointTypedConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	isTypePtr bool
}

type NetHTTPEndpointJsonConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	reader runtime.TypedEndpointReader[T]
	tType  reflect.Type
}

type NetHTTPEndpointFormConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	reader  runtime.TypedEndpointReader[T]
	tType   reflect.Type
	decoder *schema.Decoder
}

type NetHTTPEndpointCustomConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	handler NetHTTPEndpointHandler[T]
}

func getNetHTTPDataSource(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSource {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
	if cfg == nil {
		env.Log().Fatalf("config for datasource with id=%d not found", id)
	}
	mux := http.NewServeMux()
	if cfg.Host == nil || cfg.Port == nil {
		env.Log().Fatalf("no host or port specified for data connector with id %d", id)
	}
	netHTTPDataSource := &NetHTTPDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, env),
		mux:             mux,
		server: http.Server{
			Addr:    fmt.Sprintf("%s:%d", *cfg.Host, *cfg.Port),
			Handler: mux,
		},
		done: make(chan struct{}),
	}
	var inputDataSource NetHTTPInputDataSource = netHTTPDataSource
	env.AddDataSource(inputDataSource)
	return netHTTPDataSource
}

func getNetHTTPDataSourceEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.InputEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	if cfg == nil {
		env.Log().Fatalf("config for endpoint with id=%d not found", id)
	}
	dataSource := getNetHTTPDataSource(cfg.IdDataConnector, env)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	if cfg.Method == nil {
		env.Log().Fatalf("no method specified for http endpoint with id %d", id)
	}
	netHTTPEndpoint := &NetHTTPEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, id, env),
		method:             *cfg.Method,
	}
	if cfg.Path == nil {
		env.Log().Fatalf("no path specified for http endpoint with id %d", id)
	}
	dataSource.(NetHTTPInputDataSource).AddHandler(*cfg.Path, http.HandlerFunc(netHTTPEndpoint.ServeHTTP))
	var inputEndpoint runtime.InputEndpoint = netHTTPEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return netHTTPEndpoint
}

func (ds *NetHTTPDataSource) Start(ctx context.Context) error {
	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(NetHTTPInputEndpoint).Start(ctx); err != nil {
			return err
		}
	}

	addr := ds.server.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		err := ds.server.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			ds.GetEnvironment().Log().Fatalln(err)
		}
		ds.done <- struct{}{}
	}()
	return nil
}

func (ds *NetHTTPDataSource) AddHandler(pattern string, handler http.Handler) {
	ds.mux.Handle(pattern, handler)
}

func (ds *NetHTTPDataSource) Stop(ctx context.Context) {
	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		endpoints.At(i).(NetHTTPInputEndpoint).Stop(ctx)
	}

	go func() {
		if err := ds.server.Shutdown(ctx); err != nil {
			ds.GetEnvironment().Log().Warnf("NetHTTPDataSource.Stop server shutdown: %s", err.Error())
		}
	}()
	select {
	case <-ds.done:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf("Stop HTTP server for data source %q after timeout. %s", ds.GetName(), ctx.Err().Error())
	}
}

func (ec *NetHTTPEndpointJsonConsumer[T]) DeserializeJson(data string) (T, error) {
	if ec.reader != nil {
		return ec.reader.Read(bytes.NewReader([]byte(data)))
	}
	if !ec.isTypePtr {
		var t T
		return t, json.Unmarshal([]byte(data), &t)
	}

	t := reflect.New(ec.tType).Interface().(T)
	return t, json.Unmarshal([]byte(data), t)
}

func (ec *NetHTTPEndpointJsonConsumer[T]) DeserializeJsonBody(reader io.Reader) (T, error) {
	if ec.reader != nil {
		return ec.reader.Read(reader)
	}
	decoder := json.NewDecoder(reader)

	if !ec.isTypePtr {
		var t T
		err := decoder.Decode(&t)
		return t, err
	}

	t := reflect.New(ec.tType).Interface().(T)
	err := decoder.Decode(t)
	return t, err
}

func (ep *NetHTTPEndpoint) Start(ctx context.Context) error {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		if err := endpointConsumers.At(i).(NetHTTPEndpointConsumer).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ep *NetHTTPEndpoint) Stop(ctx context.Context) {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		endpointConsumers.At(i).(NetHTTPEndpointConsumer).Stop(ctx)
	}
}

func (ep *NetHTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != ep.method {
		errText := fmt.Sprintf("Invalid request method %q for endpoint %q with path %q",
			r.Method, ep.GetName(),
			*ep.GetConfig().Path)
		http.Error(w, errText,
			http.StatusBadRequest)
		ep.GetEnvironment().Log().Warnln(errText)
	} else {
		endpointConsumers := ep.GetEndpointConsumers()
		requestData := netHTTPEndpointRequestData{
			w:         w,
			r:         r,
			optimized: endpointConsumers.Len() == 1,
		}
		length := endpointConsumers.Len()
		for i := 0; i < length; i++ {
			if err := endpointConsumers.At(i).(NetHTTPEndpointConsumer).EndpointRequest(&requestData); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				ep.GetEnvironment().Log().Warnf("ServeHTTP error in endpoint with id=%d: %v", ep.GetId(), err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (ec *NetHTTPEndpointJsonConsumer[T]) EndpointRequest(requestData NetHTTPEndpointRequestData) error {
	var t T
	if reader, err := requestData.GetBody(); err != nil {
		return fmt.Errorf("unable to read request: %s", err.Error())
	} else {
		t, err = func(reader io.ReadCloser) (T, error) {
			defer func() {
				if err := reader.Close(); err != nil {
					ec.Endpoint().GetEnvironment().Log().Warnln(err)
				}
			}()
			return ec.DeserializeJsonBody(reader)
		}(reader)
		if err != nil {
			return fmt.Errorf("invalid request body: %s", err.Error())
		}
	}
	ec.Consume(t)
	return nil
}

func (ec *NetHTTPEndpointCustomConsumer[T]) Start(ctx context.Context) error {
	return nil
}

func (ec *NetHTTPEndpointCustomConsumer[T]) Stop(ctx context.Context) {
}

func (ec *NetHTTPEndpointCustomConsumer[T]) EndpointRequest(requestData NetHTTPEndpointRequestData) error {
	ec.handler.Handler(&HandlerData{
		Writer:  requestData.ResponseWriter(),
		Request: requestData.Request(),
	}, ec)
	return nil
}

func (ec *NetHTTPEndpointCustomConsumer[T]) Out(value T) {
	ec.Stream().Consume(value)
}

func (ec *NetHTTPEndpointFormConsumer[T]) Start(ctx context.Context) error {
	reader := ec.Endpoint().GetEnvironment().GetEndpointReader(ec.Endpoint(), ec.Stream(), serde.GetSerdeType[T]())
	if reader != nil {
		var ok bool
		ec.reader, ok = reader.(runtime.TypedEndpointReader[T])
		if !ok {
			return fmt.Errorf("reader has invalid type for endpoint with id=%d", ec.Endpoint().GetId())
		}
	}
	return nil
}

func (ec *NetHTTPEndpointFormConsumer[T]) Stop(ctx context.Context) {
}

func (ec *NetHTTPEndpointFormConsumer[T]) EndpointRequest(requestData NetHTTPEndpointRequestData) error {
	var form url.Values
	var err error
	if form, err = requestData.GetForm(); err != nil {
		return fmt.Errorf("unable to parse request: %s", err.Error())
	}

	if !ec.isTypePtr {
		var t T
		err = ec.decoder.Decode(&t, form)
		if err != nil {
			return fmt.Errorf("unable to decode data: %s", err.Error())
		}
		ec.Consume(t)
	}

	t := reflect.New(ec.tType).Interface().(T)
	err = ec.decoder.Decode(&t, form)
	if err != nil {
		return fmt.Errorf("unable to decode data: %s", err.Error())
	}
	ec.Consume(t)
	return nil
}

func (ec *NetHTTPEndpointJsonConsumer[T]) Start(ctx context.Context) error {
	reader := ec.Endpoint().GetEnvironment().GetEndpointReader(ec.Endpoint(), ec.Stream(), serde.GetSerdeType[T]())
	if reader != nil {
		var ok bool
		ec.reader, ok = reader.(runtime.TypedEndpointReader[T])
		if !ok {
			return fmt.Errorf("reader has invalid type for endpoint with id=%d", ec.Endpoint().GetId())
		}
	}
	return nil
}

func (ec *NetHTTPEndpointJsonConsumer[T]) Stop(ctx context.Context) {
}

func MakeNetHTTPEndpointConsumer[T any](stream runtime.TypedInputStream[T], handler NetHTTPEndpointHandler[T]) runtime.Consumer[T] {
	env := stream.GetEnvironment()
	endpoint := getNetHTTPDataSourceEndpoint(stream.GetEndpointId(), env)
	cfg := endpoint.GetConfig()

	var consumer runtime.Consumer[T]
	var netHTTPEndpointConsumer NetHTTPEndpointConsumer
	if cfg.Format == nil {
		env.Log().Fatalf("endpoint format not specified for endpoint with id %d", endpoint.GetId())
	}

	switch *cfg.Format {
	case api.DataFormatJson:
		endpointConsumer := &NetHTTPEndpointJsonConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
				isTypePtr:                  serde.IsTypePtr[T](),
			},
			tType: serde.GetSerdeTypeWithoutPtr[T](),
		}
		consumer = endpointConsumer
		netHTTPEndpointConsumer = endpointConsumer

	case api.DataFormatForm:
		endpointConsumer := &NetHTTPEndpointFormConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
				isTypePtr:                  serde.IsTypePtr[T](),
			},
			decoder: schema.NewDecoder(),
			tType:   serde.GetSerdeTypeWithoutPtr[T](),
		}
		consumer = endpointConsumer
		netHTTPEndpointConsumer = endpointConsumer

	case api.DataFormatCustom:
		if handler == nil {
			env.Log().Fatalf("handler is nil for custom format in the shttp endpoint with id=%d", endpoint.GetId())
		}
		endpointConsumer := &NetHTTPEndpointCustomConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
				isTypePtr:                  serde.IsTypePtr[T](),
			},
			handler: handler,
		}
		consumer = endpointConsumer
		netHTTPEndpointConsumer = endpointConsumer

	default:
		env.Log().Fatalf("Unknown endpoint format %q for endpoint %q.",
			*endpoint.GetConfig().Format, endpoint.GetName())
	}

	endpoint.AddEndpointConsumer(netHTTPEndpointConsumer)
	return consumer
}
