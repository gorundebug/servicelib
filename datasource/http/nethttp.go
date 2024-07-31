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
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/runtime"
	"io"
	"net/http"
	"net/url"
	"time"
)

const defaultShutdownTimeout = 30 * time.Second

type NetHTTPDataSource struct {
	*runtime.InputDataSource
	server http.Server
	mux    *http.ServeMux
	quit   chan struct{}
}

type NetHTTPEndpoint struct {
	*runtime.DataSourceEndpoint
	method string
}

type NetHTTPEndpointRequestData struct {
	w         http.ResponseWriter
	r         *http.Request
	body      []byte
	form      url.Values
	query     url.Values
	optimized bool
}

func (d *NetHTTPEndpointRequestData) GetBody() (io.ReadCloser, error) {
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

func (d *NetHTTPEndpointRequestData) getForm() (url.Values, error) {
	if d.form == nil {
		if err := d.r.ParseForm(); err != nil {
			return nil, err
		}
		d.form = d.r.Form
	}
	return d.form, nil
}

func (d *NetHTTPEndpointRequestData) getQuery() url.Values {
	if d.query == nil {
		d.query = d.r.URL.Query()
	}
	return d.query
}

type NetHTTPEndpointTypedConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	endpoint  *NetHTTPEndpoint
	isTypePtr bool
}

type NetHTTPEndpointJsonConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	param string
}

type NetHTTPEndpointGorillaSchemaConsumer[T any] struct {
	NetHTTPEndpointTypedConsumer[T]
	decoder *schema.Decoder
}

func getNetHTTPDataSource(id int, execRuntime runtime.StreamExecutionRuntime) *NetHTTPDataSource {
	dataSource := execRuntime.GetDataSource(id)
	if dataSource != nil {
		return dataSource.(*NetHTTPDataSource)
	}
	cfg := execRuntime.GetConfig().GetDataConnectorById(id)
	mux := http.NewServeMux()
	netHTTPDataSource := &NetHTTPDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, execRuntime),
		mux:             mux,
		server: http.Server{
			Addr:    fmt.Sprintf("%s:%d", cfg.Properties["ip"].(string), cfg.Properties["port"].(int)),
			Handler: mux,
		},
		quit: make(chan struct{}),
	}
	execRuntime.AddDataSource(netHTTPDataSource)
	return netHTTPDataSource
}

func getNetHTTPDataSourceEndpoint(id int, execRuntime runtime.StreamExecutionRuntime) *NetHTTPEndpoint {
	cfg := execRuntime.GetConfig().GetEndpointConfigById(id)
	dataSource := getNetHTTPDataSource(cfg.IdDataConnector, execRuntime)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint.(*NetHTTPEndpoint)
	}
	netHTTPEndpoint := &NetHTTPEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, cfg, execRuntime),
		method:             runtime.GetConfigProperty[string](cfg, "method"),
	}
	dataSource.mux.Handle(runtime.GetConfigProperty[string](cfg, "path"), http.HandlerFunc(netHTTPEndpoint.ServeHTTP))
	return netHTTPEndpoint
}

func (ds *NetHTTPDataSource) Start() error {
	go func() {
		err := ds.server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			log.Panicln(err)
		}
		ds.quit <- struct{}{}
	}()
	return nil
}

func (ds *NetHTTPDataSource) Stop() {
	if err := ds.server.Shutdown(context.Background()); err != nil {
		log.Warnf("NetHTTPDataSource.Stop server shutdown: %s", err.Error())
	} else {
		select {
		case <-ds.quit:
		case <-time.After(defaultShutdownTimeout):
			log.Warnf("Stop HTTP server for data source '%s' timeout.", ds.GetName())
		}
	}
}

func (ec *NetHTTPEndpointJsonConsumer[T]) DeserializeJson(data string) (T, error) {
	var t T
	var err error
	if ec.isTypePtr {
		err = json.Unmarshal([]byte(data), t)
	} else {
		err = json.Unmarshal([]byte(data), &t)
	}
	return t, err
}

func (ec *NetHTTPEndpointJsonConsumer[T]) DeserializeJsonBody(reader io.Reader) (T, error) {
	decoder := json.NewDecoder(reader)
	var t T
	var err error
	if ec.isTypePtr {
		err = decoder.Decode(t)
	} else {
		err = decoder.Decode(&t)
	}
	return t, err
}

func (ep *NetHTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != ep.method {
		http.Error(w, fmt.Sprintf("Invalid request method '%s' for endpoint '%s' with path '%s'",
			r.Method, ep.GetName(),
			runtime.GetConfigProperty[string](ep.GetConfig(), "path")),
			http.StatusBadRequest)
	}
	endpointConsumers := ep.GetEndpointConsumers()
	requestData := NetHTTPEndpointRequestData{
		w:         w,
		r:         r,
		optimized: len(endpointConsumers) == 1,
	}
	for _, endpointConsumer := range endpointConsumers {
		endpointConsumer.EndpointRequest(&requestData)
	}
}

func (ec *NetHTTPEndpointJsonConsumer[T]) EndpointRequest(requestData runtime.EndpointRequestData) {
	netHTTPEndpointRequestData := requestData.(*NetHTTPEndpointRequestData)
	var t T
	if netHTTPEndpointRequestData.r.Method == http.MethodPost || len(ec.param) == 0 {
		if reader, err := netHTTPEndpointRequestData.GetBody(); err != nil {
			http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Unable to read request: %s", err.Error()), http.StatusBadRequest)
			return
		} else {
			t, err = func(reader io.ReadCloser) (T, error) {
				defer func() {
					if err := reader.Close(); err != nil {
						log.Warnln(err)
					}
				}()
				return ec.DeserializeJsonBody(reader)
			}(reader)
			if err != nil {
				http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Invalid request body: %s", err.Error()), http.StatusBadRequest)
				return
			}
		}
	} else {
		query := netHTTPEndpointRequestData.getQuery()
		data := query.Get(ec.param)
		if data == "" {
			http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Missing '%s' parameter", ec.param), http.StatusBadRequest)
			return
		}
		var err error
		t, err = ec.DeserializeJson(data)
		if err != nil {
			http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Error deserializing '%s' parameter: %s", ec.param, err.Error()), http.StatusBadRequest)
			return
		}
	}
	ec.Consume(t)
}

func (ec *NetHTTPEndpointGorillaSchemaConsumer[T]) EndpointRequest(requestData runtime.EndpointRequestData) {
	netHTTPEndpointRequestData := requestData.(*NetHTTPEndpointRequestData)
	var form url.Values
	var err error
	if form, err = netHTTPEndpointRequestData.getForm(); err != nil {
		http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Unable to parse request: %s", err.Error()), http.StatusBadRequest)
		return
	}
	var t T
	if ec.isTypePtr {
		err = ec.decoder.Decode(t, form)
	} else {
		err = ec.decoder.Decode(&t, form)
	}
	if err != nil {
		http.Error(netHTTPEndpointRequestData.w, fmt.Sprintf("Unable to decode data: %s", err.Error()), http.StatusBadRequest)
		return
	}
	ec.Consume(t)
}

func MakeNetHTTPEndpointConsumer[T any](stream runtime.InputTypedStream[T]) runtime.TypedEndpointConsumer[T] {
	execRuntime := stream.GetRuntime()
	endpoint := getNetHTTPDataSourceEndpoint(stream.GetEndpointId(), execRuntime)
	cfg := endpoint.GetConfig()

	var endpointConsumer runtime.TypedEndpointConsumer[T]
	switch endpoint.GetConfig().Properties["format"].(string) {

	case "json":
		endpointConsumer = &NetHTTPEndpointJsonConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](stream),
				endpoint:                   endpoint,
				isTypePtr:                  runtime.IsTypePtr[T](),
			},
			param: runtime.GetConfigProperty[string](cfg, "param"),
		}

	case "gorilla/schema":
		endpointConsumer = &NetHTTPEndpointGorillaSchemaConsumer[T]{
			NetHTTPEndpointTypedConsumer: NetHTTPEndpointTypedConsumer[T]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](stream),
				endpoint:                   endpoint,
				isTypePtr:                  runtime.IsTypePtr[T](),
			},
			decoder: schema.NewDecoder(),
		}

	default:
		log.Panicf("Unknown endpoint format '%s' for endpoint '%s'.",
			cfg.Properties["format"].(string), endpoint.GetName())
	}

	endpoint.AddEndpointConsumer(endpointConsumer)
	return endpointConsumer
}
