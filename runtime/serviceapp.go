/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/api"
	"gitlab.com/gorundebug/servicelib/telemetry"
	"gitlab.com/gorundebug/servicelib/telemetry/metrics"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

var englishUpperCaser = cases.Upper(language.English)

type ServiceApp struct {
	config            *ServiceAppConfig
	serviceConfig     *ServiceConfig
	runtime           StreamExecutionRuntime
	streams           map[int]StreamBase
	dataSources       map[int]DataSource
	dataSinks         map[int]DataSink
	serdes            map[reflect.Type]StreamSerializer
	httpServer        http.Server
	mux               *http.ServeMux
	httpServerDone    chan struct{}
	metrics           metrics.Metrics
	consumeStatistics map[LinkId]ConsumeStatistics
}

func (app *ServiceApp) reloadConfig(config Config) {
	app.config = config.GetServiceConfig()
	app.config.initRuntimeConfig()
	app.runtime.SetConfig(config)
}

func (app *ServiceApp) GetConfig() *ServiceAppConfig {
	return app.config
}

func (app *ServiceApp) GetServiceConfig() *ServiceConfig {
	return app.serviceConfig
}

func (app *ServiceApp) ReloadConfig(config Config) {
}

func (app *ServiceApp) GetMetrics() metrics.Metrics {
	return app.metrics
}

func (app *ServiceApp) registerStream(stream StreamBase) {
	app.streams[stream.GetId()] = stream
}

func (app *ServiceApp) registerSerde(tp reflect.Type, serializer StreamSerializer) {
	app.serdes[tp] = serializer
}

func (app *ServiceApp) getRegisteredSerde(tp reflect.Type) StreamSerializer {
	return app.serdes[tp]
}

func (app *ServiceApp) registerConsumeStatistics(statistics ConsumeStatistics) {
	app.consumeStatistics[statistics.LinkId()] = statistics
}

func (app *ServiceApp) serviceInit(name string, runtime StreamExecutionRuntime, config Config) {
	app.config = config.GetServiceConfig()
	app.config.initRuntimeConfig()
	app.serviceConfig = config.GetServiceConfig().GetServiceConfigByName(name)
	if app.serviceConfig == nil {
		log.Fatalf("Cannot find service config for %s", name)
	}
	app.metrics = telemetry.CreateMetrics(app.config.Settings.MetricsEngine)
	app.runtime = runtime
	app.streams = make(map[int]StreamBase)
	app.consumeStatistics = make(map[LinkId]ConsumeStatistics)
	app.dataSources = make(map[int]DataSource)
	app.dataSinks = make(map[int]DataSink)
	app.serdes = make(map[reflect.Type]StreamSerializer)
	app.mux = http.NewServeMux()
	app.httpServerDone = make(chan struct{})
	app.httpServer = http.Server{
		Handler: app.mux,
		Addr:    fmt.Sprintf("%s:%d", app.serviceConfig.MonitoringHost, app.serviceConfig.MonitoringPort),
	}
	app.mux.Handle("/status", http.HandlerFunc(app.statusHandler))
	app.mux.Handle("/data", http.HandlerFunc(app.dataHandler))
	runtime.SetConfig(config)
}

//go:embed status.html
var statusHtml []byte

func (app *ServiceApp) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(statusHtml); err != nil {
		log.Warnf("statusHandler write error: %s", err.Error())
	}
}

type Node struct {
	Id    int `json:"id"`
	Color struct {
		Background string `json:"background"`
	} `json:"color"`
	Opacity float32 `json:"opacity"`
	Label   string  `json:"label"`
	X       int     `json:"x"`
	Y       int     `json:"y"`
}

type Edge struct {
	From   int    `json:"from"`
	To     int    `json:"to"`
	Arrows string `json:"arrows"`
	Length int    `json:"length"`
	Label  string `json:"label"`
	Color  struct {
		Opacity float32 `json:"opacity"`
		Color   string  `json:"color"`
	} `json:"color"`
}

const (
	opacity    = float32(1.0)
	edgeColor  = "#0050FF"
	edgeLength = 200
)

func (app *ServiceApp) makeNode(stream StreamBase) *Node {
	config := stream.GetConfig()
	background := app.serviceConfig.Color
	serviceName := app.serviceConfig.Name
	if config.IdService != app.serviceConfig.Id {
		for i := range app.config.Services {
			if app.config.Services[i].Id == config.IdService {
				serviceName = app.config.Services[i].Name
				background = app.config.Services[i].Color
				break
			}
		}
	}

	label := fmt.Sprintf("%s(%s)\n[%s]", stream.GetName(),
		englishUpperCaser.String(stream.GetTransformationName()), serviceName)

	return &Node{
		Id: stream.GetId(),
		Color: struct {
			Background string `json:"background"`
		}{Background: background},
		X:       config.XPos,
		Y:       config.YPos,
		Opacity: opacity,
		Label:   label,
	}
}

func (app *ServiceApp) makeEdges(stream StreamBase) []*Edge {
	edges := make([]*Edge, 0)

	for _, consumer := range stream.getConsumers() {
		label, _ := strings.CutPrefix(stream.GetTypeName(), "*")
		label, _ = strings.CutPrefix(label, "types.")
		if stat, ok := app.consumeStatistics[LinkId{From: stream.GetId(), To: consumer.GetId()}]; ok {
			label += fmt.Sprintf("\ncalls: %d", stat.Count())
		}

		config := consumer.GetConfig()

		if config.Type == api.TransformationTypeJoin ||
			config.Type == api.TransformationTypeMultiJoin {
			if config.IdSource == stream.GetId() {
				label = label + " (L)"
			} else {
				label = label + " (R)"
			}
		}
		if stream.GetConfig().IdService != app.serviceConfig.Id ||
			config.IdService != app.serviceConfig.Id {
		}

		edges = append(edges, &Edge{
			From:   stream.GetId(),
			To:     consumer.GetId(),
			Arrows: "to",
			Length: edgeLength,
			Label:  label,
			Color: struct {
				Opacity float32 `json:"opacity"`
				Color   string  `json:"color"`
			}{Opacity: opacity, Color: edgeColor},
		})
	}

	return edges
}

type NetworkData struct {
	Nodes []*Node `json:"nodes"`
	Edges []*Edge `json:"edges"`
}

func (app *ServiceApp) dataHandler(w http.ResponseWriter, r *http.Request) {
	networkData := NetworkData{
		Nodes: make([]*Node, 0),
		Edges: make([]*Edge, 0),
	}
	for _, stream := range app.streams {
		networkData.Nodes = append(networkData.Nodes, app.makeNode(stream))
		networkData.Edges = append(networkData.Edges, app.makeEdges(stream)...)
	}
	jsonData, err := json.Marshal(networkData)
	if err != nil {
		http.Error(w, "Error serializing data to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(jsonData); err != nil {
		log.Warnf("dataHandler write error: %s", err.Error())
	}
}

func (app *ServiceApp) GetDataSource(id int) DataSource {
	return app.dataSources[id]
}

func (app *ServiceApp) AddDataSource(dataSource DataSource) {
	app.dataSources[dataSource.GetId()] = dataSource
}

func (app *ServiceApp) GetDataSink(id int) DataSink {
	return app.dataSinks[id]
}

func (app *ServiceApp) GetEndpointReader(endpoint Endpoint, stream StreamBase, valueType reflect.Type) EndpointReader {
	return nil
}

func (app *ServiceApp) GetEndpointWriter(endpoint Endpoint, stream StreamBase, valueType reflect.Type) EndpointWriter {
	return nil
}

func (app *ServiceApp) AddDataSink(dataSink DataSink) {
	app.dataSinks[dataSink.GetId()] = dataSink
}

func (app *ServiceApp) getSerde(valueType reflect.Type) (Serializer, error) {
	if serde, err := app.runtime.GetSerde(valueType); err != nil {
		return nil, fmt.Errorf("method GetSerde error for type: %s", valueType.Name())
	} else if serde != nil {
		return serde, nil
	}

	if serde, err := makeDefaultSerde(valueType); err != nil {
		return nil, fmt.Errorf("method GetSerde error for type: %s", valueType.Name())
	} else if serde != nil {
		return serde, nil
	}

	return nil, fmt.Errorf("getSerde error. Unsupported type: %s", valueType.Name())
}

func (app *ServiceApp) Start(ctx context.Context) error {
	addr := app.httpServer.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		log.Infof("Monitoring for service '%s' listening at %v", app.serviceConfig.Name, app.httpServer.Addr)

		err := app.httpServer.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalln(err)
		}
		app.httpServerDone <- struct{}{}
	}()

	for _, v := range app.dataSources {
		if err := v.Start(ctx); err != nil {
			log.Fatalln(err)
		}
	}
	for _, v := range app.dataSinks {
		if err := v.Start(ctx); err != nil {
			log.Fatalln(err)
		}
	}
	return nil
}

func (app *ServiceApp) Stop(ctx context.Context) {
	wg := sync.WaitGroup{}
	for _, v := range app.dataSources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}
	for _, v := range app.dataSinks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			if err := app.httpServer.Shutdown(ctx); err != nil {
				log.Warnf("server shutdown: %s", err.Error())
			}
		}()
		select {
		case <-app.httpServerDone:
		case <-ctx.Done():
			log.Warnf("Monitoring server stop timeout for service '%s'. %s", app.serviceConfig.Name, ctx.Err().Error())
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Warnf("ServiceApp '%s' stop timeout: %s", app.serviceConfig.Name, ctx.Err().Error())
	}
}

func (app *ServiceApp) GetConsumeTimeout(from int, to int) time.Duration {
	link := app.config.GetLink(from, to)
	if link != nil {
		propTimeout := link.GetProperty("timeout")
		if propTimeout != nil {
			timeout := propTimeout.(int)
			if timeout > 0 {
				return time.Duration(timeout) * time.Millisecond
			}
		}
	}
	return time.Duration(app.serviceConfig.DefaultGrpcTimeout) * time.Millisecond
}
