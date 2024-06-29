/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/saapi/api"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var englishUpperCaser = cases.Upper(language.English)

const defaultShutdownTimeout = 30 * time.Second

type ServiceApp struct {
	config        *ServiceAppConfig
	serviceConfig *ServiceConfig
	runtime       StreamExecutionRuntime
	streams       map[int]StreamBase
	dataSources   map[int]DataSource
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

func (app *ServiceApp) AddDataSource(dataSource DataSource) {
	app.dataSources[dataSource.GetId()] = dataSource
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
	Arrow  string `json:"arrow"`
	Length int    `json:"length"`
	Label  string `json:"label"`
	Color  struct {
		Opacity float32 `json:"opacity"`
		Color   string  `json:"color"`
	} `json:"color"`
}

func (app *ServiceApp) makeNode(stream StreamBase) *Node {
	config := stream.GetConfig()
	var opacity float32
	if config.IdService == app.serviceConfig.Id {
		opacity = 1.0
	} else {
		opacity = 0.3
	}

	label := fmt.Sprintf("%s(%s)\n[%s]", stream.GetName(),
		englishUpperCaser.String(stream.GetTransformationName()), app.serviceConfig.Name)

	return &Node{
		Id: stream.GetId(),
		Color: struct {
			Background string `json:"background"`
		}{Background: app.serviceConfig.Color},
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
		edges = append(edges, &Edge{
			From:   stream.GetId(),
			To:     consumer.GetId(),
			Arrow:  "to",
			Length: 200,
			Label:  label,
			Color: struct {
				Opacity float32 `json:"opacity"`
				Color   string  `json:"color"`
			}{Opacity: 1.0, Color: "#0050FF"},
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

	if err := app.server.Shutdown(context.Background()); err != nil {
		log.Warnf("server shutdown: %s", err.Error())
	} else {
		select {
		case <-app.quit:
		case <-time.After(defaultShutdownTimeout):
			log.Warnf("Monitoring server stop timeout for service '%s'.", app.serviceConfig.Name)
		}
	}
}
