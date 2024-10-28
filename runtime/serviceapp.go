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
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/logging"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
	"github.com/gorundebug/servicelib/runtime/telemetry"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var englishUpperCaser = cases.Upper(language.English)

type ServiceApp struct {
	id                int
	config            atomic.Pointer[config.ServiceAppConfig]
	environment       ServiceExecutionEnvironment
	streams           map[int]Stream
	dataSources       map[int]DataSource
	dataSinks         map[int]DataSink
	serdes            map[reflect.Type]serde.StreamSerializer
	httpServer        http.Server
	mux               *http.ServeMux
	httpServerDone    chan struct{}
	metrics           metrics.Metrics
	metricsEngine     metrics.MetricsEngine
	consumeStatistics map[config.LinkId]ConsumeStatistics
	storages          []store.Storage
	delayPool         pool.DelayPool
	taskPools         map[string]pool.TaskPool
	priorityTaskPools map[string]pool.PriorityTaskPool
	loader            ServiceLoader
	logsEngine        log.LogsEngine
	log               log.Logger
}

func (app *ServiceApp) getConfig() *config.ServiceAppConfig {
	return app.config.Load()
}

func (app *ServiceApp) GetRuntime() ServiceExecutionRuntime {
	return app
}

func (app *ServiceApp) reloadConfig(cfg config.Config) {
	appConfig := cfg.GetAppConfig()
	app.config.Store(appConfig)
	app.environment.SetConfig(cfg)
}

func (app *ServiceApp) GetAppConfig() *config.ServiceAppConfig {
	return app.getConfig()
}

func (app *ServiceApp) getServiceConfig() *config.ServiceConfig {
	return app.getConfig().GetServiceConfigById(app.id)
}

func (app *ServiceApp) GetServiceConfig() *config.ServiceConfig {
	return app.getServiceConfig()
}

func (app *ServiceApp) ReloadConfig(config config.Config) {
}

func (app *ServiceApp) GetMetrics() metrics.Metrics {
	return app.metrics
}

func (app *ServiceApp) registerStream(stream Stream) {
	app.streams[stream.GetId()] = stream
}

func (app *ServiceApp) registerStorage(storage store.Storage) {
	app.storages = append(app.storages, storage)
}

func (app *ServiceApp) registerSerde(tp reflect.Type, serializer serde.StreamSerializer) {
	app.serdes[tp] = serializer
}

func (app *ServiceApp) getRegisteredSerde(tp reflect.Type) serde.StreamSerializer {
	return app.serdes[tp]
}

func (app *ServiceApp) registerConsumeStatistics(statistics ConsumeStatistics) {
	app.consumeStatistics[statistics.LinkId()] = statistics
}

func (app *ServiceApp) GetLog() log.Logger {
	return app.getLog()
}

func (app *ServiceApp) getLog() log.Logger {
	return app.log
}

func (app *ServiceApp) serviceInit(name string,
	env ServiceExecutionEnvironment,
	dep environment.ServiceDependency,
	loader ServiceLoader,
	cfg config.Config) error {
	var err error
	appConfig := cfg.GetAppConfig()
	serviceConfig := appConfig.GetServiceConfigByName(name)
	if serviceConfig == nil {
		return fmt.Errorf("cannot find service config for %s", name)
	}
	app.config.Store(appConfig)
	app.id = serviceConfig.Id
	app.loader = loader
	app.environment = env

	if dep != nil {
		app.logsEngine = dep.GetLogsEngine()
		app.metricsEngine = dep.GetMetricsEngine()
	}

	if app.logsEngine == nil {
		logsEngineType := api.Logrus
		if serviceConfig.LogEngine != nil {
			logsEngineType = *serviceConfig.LogEngine
		}
		app.logsEngine, err = logging.CreateLogsEngine(logsEngineType, env)
		if err != nil {
			return err
		}
	}
	app.log = app.logsEngine.DefaultLogger(nil)

	if app.metricsEngine == nil {
		app.metricsEngine, err = telemetry.CreateMetricsEngine(serviceConfig.MetricsEngine, env)
		if err != nil {
			return err
		}
	}
	app.metrics = app.metricsEngine.Metrics()

	app.streams = make(map[int]Stream)
	app.consumeStatistics = make(map[config.LinkId]ConsumeStatistics)
	app.serdes = make(map[reflect.Type]serde.StreamSerializer)

	app.dataSources = make(map[int]DataSource)
	app.dataSinks = make(map[int]DataSink)

	app.delayPool = pool.MakeDelayTaskPool(env)
	app.taskPools = make(map[string]pool.TaskPool)
	app.priorityTaskPools = make(map[string]pool.PriorityTaskPool)

	app.mux = http.NewServeMux()
	app.httpServerDone = make(chan struct{})
	app.httpServer = http.Server{
		Handler: app.mux,
		Addr:    fmt.Sprintf("%s:%d", serviceConfig.MonitoringHost, serviceConfig.MonitoringPort),
	}
	app.mux.Handle("/status", http.HandlerFunc(app.statusHandler))
	app.mux.Handle("/data", http.HandlerFunc(app.dataHandler))
	app.mux.Handle("/metrics", app.metricsEngine.MetricsHandler())

	for idx := range appConfig.Links {
		link := &appConfig.Links[idx]
		streamFrom := appConfig.GetStreamConfigById(link.From)
		streamTo := appConfig.GetStreamConfigById(link.To)
		if streamFrom.IdService == serviceConfig.Id || streamTo.IdService == serviceConfig.Id {
			var callSemantics api.CallSemantics
			if streamFrom.IdService == serviceConfig.Id {
				callSemantics = link.CallSemantics
			} else {
				if link.IncomeCallSemantics == nil {
					return fmt.Errorf("income call semantics does not defined for link{from=%d, to=%d}", link.From, link.To)
				}
				callSemantics = *link.IncomeCallSemantics
			}
			if callSemantics != api.FunctionCall &&
				callSemantics != api.PriorityTaskPool &&
				callSemantics != api.TaskPool {
				return fmt.Errorf("invalid call semantics %d defined for link{from=%d, to=%d}", callSemantics, link.From, link.To)
			}
			if callSemantics != api.FunctionCall {
				var poolName string
				if streamFrom.IdService == serviceConfig.Id {
					if link.PoolName == nil {
						return fmt.Errorf("pool name does not defined for link{from=%d, to=%d}", link.From, link.To)
					}
					if callSemantics == api.PriorityTaskPool && link.Priority == nil {
						return fmt.Errorf("priority for link{from=%d, to=%d} does not defines", link.From, link.To)
					}
					poolName = *link.PoolName
				} else {
					if link.IncomePoolName == nil {
						return fmt.Errorf("income pool name does not defined for link{from=%d, to=%d}", link.From, link.To)
					}
					if callSemantics == api.PriorityTaskPool && link.IncomePriority == nil {
						return fmt.Errorf("priority for link{from=%d, to=%d} does not defines", link.From, link.To)
					}
					poolName = *link.IncomePoolName
				}
				poolConfig := appConfig.GetPoolByName(poolName)
				if poolConfig == nil {
					return fmt.Errorf("task pool %q not found for link{from=%d, to=%d}", poolName, link.From, link.To)
				}
				if callSemantics == api.TaskPool {
					if _, ok := app.taskPools[poolName]; !ok {
						app.taskPools[poolName] = pool.MakeTaskPool(env, poolName)
					}
				} else if callSemantics == api.PriorityTaskPool {
					if _, ok := app.priorityTaskPools[poolName]; !ok {
						app.priorityTaskPools[poolName] = pool.MakePriorityTaskPool(env, poolName)
					}
				} else {
					return fmt.Errorf("invalid call semantics %d for link{from=%d, to=%d}", callSemantics, link.From, link.To)
				}
			}
		}
	}
	env.SetConfig(cfg)

	return nil
}

//go:embed status.html
var statusHtml []byte

func (app *ServiceApp) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(statusHtml); err != nil {
		app.GetLog().Warnf("statusHandler write error: %s", err.Error())
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

func (app *ServiceApp) makeNode(stream Stream) *Node {
	appConfig := app.getConfig()
	cfg := stream.GetConfig()
	serviceConfig := app.getServiceConfig()
	background := serviceConfig.Color
	serviceName := serviceConfig.Name
	if cfg.IdService != serviceConfig.Id {
		for i := range appConfig.Services {
			if appConfig.Services[i].Id == cfg.IdService {
				serviceName = appConfig.Services[i].Name
				background = appConfig.Services[i].Color
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
		X:       cfg.XPos,
		Y:       cfg.YPos,
		Opacity: opacity,
		Label:   label,
	}
}

func (app *ServiceApp) makeEdges(stream Stream) []*Edge {
	edges := make([]*Edge, 0)

	for _, consumer := range stream.GetConsumers() {
		label, _ := strings.CutPrefix(stream.GetTypeName(), "*")
		label, _ = strings.CutPrefix(label, "types.")
		if stat, ok := app.consumeStatistics[config.LinkId{From: stream.GetId(), To: consumer.GetId()}]; ok {
			label += fmt.Sprintf("\ncalls: %d", stat.Count())
		}

		cfg := consumer.GetConfig()

		if cfg.Type == api.TransformationTypeJoin ||
			cfg.Type == api.TransformationTypeMultiJoin {
			if cfg.IdSource == stream.GetId() {
				label = label + " (L)"
			} else {
				label = label + " (R)"
			}
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
		Nodes: make([]*Node, 0, len(app.streams)),
		Edges: make([]*Edge, 0, len(app.streams)*2),
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
		app.GetLog().Warnf("dataHandler write error: %s", err.Error())
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

func (app *ServiceApp) GetEndpointReader(endpoint Endpoint, stream Stream, valueType reflect.Type) EndpointReader {
	return nil
}

func (app *ServiceApp) GetEndpointWriter(endpoint Endpoint, stream Stream, valueType reflect.Type) EndpointWriter {
	return nil
}

func (app *ServiceApp) AddDataSink(dataSink DataSink) {
	app.dataSinks[dataSink.GetId()] = dataSink
}

func (app *ServiceApp) getSerde(valueType reflect.Type) (serde.Serializer, error) {
	if ser, err := app.environment.GetSerde(valueType); err != nil {
		return nil, fmt.Errorf("method GetSerde error for type: %s", valueType.Name())
	} else if ser != nil {
		return ser, nil
	}

	if ser, err := serde.MakeDefaultSerde(valueType); err != nil {
		return nil, fmt.Errorf("method GetSerde error for type: %s", valueType.Name())
	} else if ser != nil {
		return ser, nil
	}

	return nil, fmt.Errorf("getSerde error. Unsupported type: %s", valueType.Name())
}

func (app *ServiceApp) Start(ctx context.Context) error {
	serviceConfig := app.getServiceConfig()

	addr := app.httpServer.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		app.GetLog().Infof("Monitoring for service %q listening at %v", serviceConfig.Name, app.httpServer.Addr)

		err := app.httpServer.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			app.GetLog().Fatalln(err)
		}
		app.httpServerDone <- struct{}{}
	}()
	for _, v := range app.dataSources {
		if err := v.Start(ctx); err != nil {
			app.GetLog().Fatalln(err)
		}
	}
	for _, v := range app.dataSinks {
		if err := v.Start(ctx); err != nil {
			app.GetLog().Fatalln(err)
		}
	}
	for _, v := range app.storages {
		if err := v.Start(ctx); err != nil {
			app.GetLog().Fatalln(err)
		}
	}
	if err := app.delayPool.Start(ctx); err != nil {
		return err
	}
	for _, taskPool := range app.taskPools {
		if err := taskPool.Start(ctx); err != nil {
			return err
		}
	}
	for _, priorityTaskPool := range app.priorityTaskPools {
		if err := priorityTaskPool.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (app *ServiceApp) Stop(ctx context.Context) {
	serviceConfig := app.getServiceConfig()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		app.loader.Stop()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		app.delayPool.Stop(ctx)
	}()

	for _, v := range app.taskPools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}

	for _, v := range app.priorityTaskPools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}

	for _, v := range app.dataSources {
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
				app.GetLog().Warnf("server shutdown: %s", err.Error())
			}
		}()
		select {
		case <-app.httpServerDone:
		case <-ctx.Done():
			app.GetLog().Warnf("Monitoring server stop timeout for service %q. %s", serviceConfig.Name, ctx.Err().Error())
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timeout := false

	select {
	case <-done:
	case <-ctx.Done():
		timeout = true
		app.GetLog().Warnf("ServiceApp %q stop timeout: %s", serviceConfig.Name, ctx.Err().Error())
	}

	if !timeout {
		wg = sync.WaitGroup{}

		for _, v := range app.dataSinks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v.Stop(ctx)
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			app.GetLog().Warnf("ServiceApp %q stop timeout: %s", serviceConfig.Name, ctx.Err().Error())
		}
	}
}

func (app *ServiceApp) GetConsumeTimeout(from int, to int) time.Duration {
	appConfig := app.getConfig()
	serviceConfig := app.getServiceConfig()
	link := appConfig.GetLink(from, to)
	if link == nil || link.Timeout == nil {
		return time.Duration(serviceConfig.DefaultGrpcTimeout) * time.Millisecond
	}
	return time.Duration(*link.Timeout) * time.Millisecond
}

func (app *ServiceApp) Delay(duration time.Duration, f func()) {
	_ = app.delayPool.Delay(duration, f)
}

func (app *ServiceApp) getTaskPool(name string) pool.TaskPool {
	return app.taskPools[name]
}

func (app *ServiceApp) getPriorityTaskPool(name string) pool.PriorityTaskPool {
	return app.priorityTaskPools[name]
}
