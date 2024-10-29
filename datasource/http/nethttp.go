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
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/serde"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
)

type NetHTTPEndpointRequestData interface {
	ResponseWriter() http.ResponseWriter
	GetBody() (io.ReadCloser, error)
	GetForm() (url.Values, error)
	GetQuery() url.Values
	GetMethod() string
}

type NetHTTPEndpointConsumer interface {
	runtime.InputEndpointConsumer
	EndpointRequest(requestData NetHTTPEndpointRequestData) error
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

type NetHTTPEndpointTypedConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	isTypePtr bool
}

type NetHTTPEndpointJsonConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	tType reflect.Type
	param *string
}

type NetHTTPEndpointGorillaSchemaConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	tType   reflect.Type
	decoder *schema.Decoder
}

func getNetHTTPDataSource(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSource {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
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
	dataSource := getNetHTTPDataSource(cfg.IdDataConnector, env)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	if cfg.Method == nil {
		env.Log().Fatalf("no method specified for http endpoint with id %d", id)
	}
	netHTTPEndpoint := &NetHTTPEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, cfg, env),
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
	epReader := ec.GetEndpointReader()
	if epReader != nil {
		return epReader.Read(bytes.NewReader([]byte(data)))
	}
	if !ec.isTypePtr {
		var t T
		return t, json.Unmarshal([]byte(data), &t)
	}

	t := reflect.New(ec.tType).Interface().(T)
	return t, json.Unmarshal([]byte(data), t)
}

func (ec *NetHTTPEndpointJsonConsumer[T]) DeserializeJsonBody(reader io.Reader) (T, error) {
	epReader := ec.GetEndpointReader()
	if epReader != nil {
		return epReader.Read(reader)
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
			optimized: len(endpointConsumers) == 1,
		}
		for _, endpointConsumer := range endpointConsumers {
			if err := endpointConsumer.(NetHTTPEndpointConsumer).EndpointRequest(&requestData); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				ep.GetEnvironment().Log().Warnln(err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (ec *NetHTTPEndpointJsonConsumer[T]) EndpointRequest(requestData NetHTTPEndpointRequestData) error {
	var t T
	if requestData.GetMethod() == http.MethodPost || ec.param == nil || len(*ec.param) == 0 {
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
	} else {
		query := requestData.GetQuery()
		data := query.Get(*ec.param)
		if data == "" {
			return fmt.Errorf("missing %q parameter", *ec.param)
		}
		var err error
		t, err = ec.DeserializeJson(data)
		if err != nil {
			return fmt.Errorf("error deserializing %q parameter: %s", *ec.param, err.Error())
		}
	}
	ec.Consume(t)
	return nil
}

func (ec *NetHTTPEndpointGorillaSchemaConsumer[T]) EndpointRequest(requestData NetHTTPEndpointRequestData) error {
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

func MakeNetHTTPEndpointConsumer[T any](stream runtime.TypedInputStream[T]) runtime.Consumer[T] {
	env := stream.GetEnvironment()
	endpoint := getNetHTTPDataSourceEndpoint(stream.GetEndpointId(), env)
	cfg := endpoint.GetConfig()

	var consumer runtime.Consumer[T]
	var netHTTPEndpointConsumer NetHTTPEndpointConsumer
	if endpoint.GetConfig().Format == nil {
		env.Log().Fatalf("endpoint format not specified for endpoint with id %d", endpoint.GetId())
	}
	switch *endpoint.GetConfig().Format {
	case "json":
		endpointConsumer := &NetHTTPEndpointJsonConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
				isTypePtr:                  serde.IsTypePtr[T](),
			},
			param: cfg.Param,
			tType: serde.GetSerdeTypeWithoutPtr[T](),
		}
		consumer = endpointConsumer
		netHTTPEndpointConsumer = endpointConsumer

	case "gorilla/schema":
		endpointConsumer := &NetHTTPEndpointGorillaSchemaConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
				isTypePtr:                  serde.IsTypePtr[T](),
			},
			decoder: schema.NewDecoder(),
			tType:   serde.GetSerdeTypeWithoutPtr[T](),
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
