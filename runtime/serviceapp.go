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
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var englishUpperCaser = cases.Upper(language.English)

const (
	defaultShutdownTimeout = 30 * time.Second
)

type ServiceApp struct {
	config        *ServiceAppConfig
	serviceConfig *ServiceConfig
	runtime       StreamExecutionRuntime
	streams       map[int]StreamBase
	dataSources   map[int]DataSource
	dataSinks     map[int]DataSink
	serdes        map[reflect.Type]StreamSerializer
	server        http.Server
	mux           *http.ServeMux
	quit          chan struct{}
}

func (app *ServiceApp) configReload(config Config) {
	app.config = config.GetServiceConfig()
	app.ConfigReload(config)
}

func (app *ServiceApp) GetConfig() *ServiceAppConfig {
	return app.config
}

func (app *ServiceApp) ConfigReload(config Config) {
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

func (app *ServiceApp) streamsInit(name string, runtime StreamExecutionRuntime, config Config) {
	app.serviceConfig = config.GetServiceConfig().GetServiceConfigByName(name)
	if app.serviceConfig == nil {
		log.Panicf("Cannot find service config for %s", name)
	}
	app.config = config.GetServiceConfig()
	app.runtime = runtime
	app.streams = make(map[int]StreamBase)
	app.dataSources = make(map[int]DataSource)
	app.dataSinks = make(map[int]DataSink)
	app.serdes = make(map[reflect.Type]StreamSerializer)
	app.mux = http.NewServeMux()
	app.quit = make(chan struct{})
	app.server = http.Server{
		Handler: app.mux,
		Addr:    fmt.Sprintf("%s:%d", app.serviceConfig.MonitoringIp, app.serviceConfig.MonitoringPort),
	}
	app.mux.Handle("/status", http.HandlerFunc(app.statusHandler))
	app.mux.Handle("/data", http.HandlerFunc(app.dataHandler))
	runtime.ServiceInit(config)
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
	externalServiceOpacity = float32(0.3)
	serviceOpacity         = float32(1.0)
	edgeColor              = "#0050FF"
	edgeLength             = 200
)

func (app *ServiceApp) makeNode(stream StreamBase) *Node {
	config := stream.GetConfig()
	opacity := serviceOpacity
	background := app.serviceConfig.Color
	serviceName := app.serviceConfig.Name
	if config.IdService != app.serviceConfig.Id {
		opacity = externalServiceOpacity
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
		config := consumer.GetConfig()

		if config.Type == api.TransformationTypeJoin ||
			config.Type == api.TransformationTypeMultiJoin {
			if config.IdSource == stream.GetId() {
				label = label + " (L)"
			} else {
				label = label + " (R)"
			}
		}
		opacity := serviceOpacity
		if stream.GetConfig().IdService != app.serviceConfig.Id ||
			config.IdService != app.serviceConfig.Id {
			opacity = externalServiceOpacity
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

func (app *ServiceApp) Start() error {
	go func() {
		err := app.server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			log.Panicln(err)
		}
		app.quit <- struct{}{}
	}()

	for _, v := range app.dataSources {
		if err := v.Start(); err != nil {
			log.Panicln(err)
		}
	}
	return nil
}

func (app *ServiceApp) Stop() {
	for _, v := range app.dataSources {
		v.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	if err := app.server.Shutdown(ctx); err != nil {
		log.Warnf("server shutdown: %s", err.Error())
	}
	select {
	case <-app.quit:
	case <-ctx.Done():
		log.Warnf("Monitoring server stop timeout for service '%s'. %s", app.serviceConfig.Name, ctx.Err().Error())
	}
}
