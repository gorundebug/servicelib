/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/logging"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
	"github.com/gorundebug/servicelib/runtime/telemetry"
)

var _ RuntimeEnvironment = (*ServiceApp)(nil)

type ServiceApp struct {
	id                    int
	config                atomic.Pointer[config.RuntimeConfig]
	environment           RuntimeEnvironment
	streams               map[int]RuntimeStream
	endpointConsumers     map[int]RuntimeEndpointConsumer
	dataSources           map[int]DataSource
	dataSinks             map[int]DataSink
	managedDataConnectors map[int]ManagedDataConnector
	serdes                map[reflect.Type]serde.StreamSerializer
	httpServer            *http.Server
	mux                   *http.ServeMux
	httpServerDone        chan struct{}
	metrics               metrics.Metrics
	metricsEngine         metrics.MetricsEngine
	tracingEngine         tracing.TracingEngine
	consumeStatistics     map[config.LinkID]ConsumeStatistics
	runtimeLinks          []RuntimeLinkInfo
	storages              []store.Storage
	delayPool             environment.DelayPool
	taskPools             map[string]pool.TaskPool
	priorityTaskPools     map[string]pool.PriorityTaskPool
	loader                ServiceLoader
	logsEngine            log.LogsEngine
	log                   log.Logger
	dep                   environment.ServiceDependencies
	components            []environment.Lifecycle
	parallel              sync.WaitGroup
	isolated              bool
}

func (app *ServiceApp) GetSerde(_ reflect.Type) (serde.Serializer, error) {
	return nil, nil
}

func (app *ServiceApp) AddComponent(component environment.Lifecycle) {
	app.components = append(app.components, component)
}

func (app *ServiceApp) RuntimeConfig() *config.RuntimeConfig {
	return app.config.Load()
}

func (app *ServiceApp) GetConfig() config.Config {
	return app.RuntimeConfig().GetConfig()
}

func (app *ServiceApp) GetRuntime() ServiceExecutionRuntime {
	return app
}

func (app *ServiceApp) updateConfig(cfg *config.RuntimeConfig) {
	app.config.Store(cfg)
	app.environment.ReloadConfig()
}

func (app *ServiceApp) ServiceConfig() *config.ServiceConfig {
	return app.RuntimeConfig().GetServiceConfigByID(app.id)
}

func (app *ServiceApp) ServiceDependencies() environment.ServiceDependencies {
	return nil
}

func (app *ServiceApp) ReloadConfig() {
}

func (app *ServiceApp) Metrics() metrics.Metrics {
	if app.isolated {
		return app.environment.Metrics()
	}
	return app.metrics
}

func (app *ServiceApp) MetricsEngine() metrics.MetricsEngine {
	if app.isolated {
		return app.environment.MetricsEngine()
	}
	return app.metricsEngine
}

func (app *ServiceApp) Tracing() tracing.Tracing {
	if app.isolated {
		return app.environment.Tracing()
	}
	return app.tracingEngine.Tracing()
}

func (app *ServiceApp) TracingEngine() tracing.TracingEngine {
	if app.isolated {
		return app.environment.TracingEngine()
	}
	return app.tracingEngine
}

func (app *ServiceApp) RegisterStream(stream RuntimeStream) {
	app.streams[stream.Stream().GetID()] = stream
}

func (app *ServiceApp) RegisterEndpointConsumer(consumer RuntimeEndpointConsumer) {
	app.endpointConsumers[consumer.GetID()] = consumer
}

func (app *ServiceApp) RegisterStorage(storage store.Storage) {
	app.storages = append(app.storages, storage)
}

// BuildRegisteredStreams resolves callers for an isolated graph after all
// generated streams and links have been registered.
func (app *ServiceApp) BuildRegisteredStreams() error {
	ids := make([]int, 0, len(app.streams))
	for id := range app.streams {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		stream := app.streams[id]
		if err := stream.Build(); err != nil {
			return err
		}
	}
	return nil
}

func (app *ServiceApp) registerSerializer(tp reflect.Type, serializer serde.StreamSerializer) {
	app.serdes[tp] = serializer
}

func (app *ServiceApp) getRegisteredSerializer(tp reflect.Type) serde.StreamSerializer {
	return app.serdes[tp]
}

func (app *ServiceApp) registerConsumeStatistics(linkID config.LinkID, statistics ConsumeStatistics) {
	app.consumeStatistics[linkID] = statistics
}

func (app *ServiceApp) registerLinkInfo(linkID config.LinkID, callSemantics config.CallSemanticsConfig) {
	app.runtimeLinks = append(app.runtimeLinks, RuntimeLinkInfo{
		From:          linkID.From,
		To:            linkID.To,
		CallSemantics: callSemantics,
	})
}

func (app *ServiceApp) beginParallel() { app.parallel.Add(1) }

func (app *ServiceApp) endParallel() { app.parallel.Done() }

// RunParallel schedules graph work using the process runtime. Workflow-backed
// environments override this method with their deterministic SDK scheduler;
// ordinary services retain the existing goroutine and drain semantics.
func (app *ServiceApp) RunParallel(ctx context.Context, fn func()) {
	if app.isolated {
		app.environment.RunParallel(ctx, fn)
		return
	}
	app.beginParallel()
	go func() {
		defer app.endParallel()
		fn()
	}()
}

// InitIsolatedGraphRuntime initializes only the in-memory graph registry of a
// ServiceApp. It deliberately starts no server, watcher, telemetry exporter,
// connector or process pool. A specialized environment embeds ServiceApp,
// supplies those policies itself, and uses the same stream/operator runtime.
func (app *ServiceApp) InitIsolatedGraphRuntime(
	runtimeConfig *config.RuntimeConfig,
	env RuntimeEnvironment,
	serviceID int,
) error {
	if runtimeConfig == nil {
		return errors.New("isolated graph runtime config is nil")
	}
	if env == nil {
		return errors.New("isolated graph environment is nil")
	}
	if runtimeConfig.GetServiceConfigByID(serviceID) == nil {
		return fmt.Errorf("isolated graph service id=%d not found", serviceID)
	}
	app.id = serviceID
	app.isolated = true
	app.environment = env
	app.config.Store(runtimeConfig)
	app.streams = make(map[int]RuntimeStream)
	app.endpointConsumers = make(map[int]RuntimeEndpointConsumer)
	app.consumeStatistics = make(map[config.LinkID]ConsumeStatistics)
	app.runtimeLinks = nil
	app.serdes = make(map[reflect.Type]serde.StreamSerializer)
	app.dataSources = make(map[int]DataSource)
	app.dataSinks = make(map[int]DataSink)
	app.managedDataConnectors = make(map[int]ManagedDataConnector)
	app.storages = nil
	app.taskPools = make(map[string]pool.TaskPool)
	app.priorityTaskPools = make(map[string]pool.PriorityTaskPool)
	return nil
}

func (app *ServiceApp) Log() log.Logger {
	if app.isolated {
		return app.environment.Log()
	}
	return app.log
}

func (app *ServiceApp) ServiceInit() error {
	return nil
}

func (app *ServiceApp) RegisterHTTPHandler(path string, handler http.Handler) {
	if app.mux == nil {
		panic("http server was not initialized for application")
	}
	app.mux.Handle(path, handler)
}

func (app *ServiceApp) ServiceContext() interface{} {
	return app.environment
}

func (app *ServiceApp) initRuntime(ctx context.Context,
	name string,
	env RuntimeEnvironment,
	dep environment.ServiceDependencies,
	loader ServiceLoader,
	runtimeConfig *config.RuntimeConfig,
) error {

	var err error

	app.dep = dep
	app.loader = loader
	app.environment = env
	app.config.Store(runtimeConfig)

	serviceConfig := runtimeConfig.GetServiceConfigByName(name)
	if serviceConfig == nil {
		return fmt.Errorf("cannot find service config for %s", name)
	}
	app.id = serviceConfig.ID

	if dep == nil {
		dep = env.ServiceDependencies()
	}

	if dep != nil {
		app.logsEngine, err = dep.LogsEngine(ctx, env)
		if err != nil {
			return err
		}
		app.metricsEngine, err = dep.MetricsEngine(ctx, env)
		if err != nil {
			return err
		}
		app.tracingEngine, err = dep.TracingEngine(ctx, env)
		if err != nil {
			return err
		}
	}

	if app.logsEngine == nil {
		app.logsEngine, err = logging.CreateLogsEngine(logging.Logrus, env)
		if err != nil {
			return err
		}
	}
	app.log = app.logsEngine.DefaultLogger(nil)

	if app.metricsEngine == nil {
		app.metricsEngine, err = telemetry.CreatePrometheusMetricsEngine(env)
		if err != nil {
			return err
		}
	}
	app.metrics = app.metricsEngine.Metrics()

	if app.tracingEngine == nil {
		app.tracingEngine, err = telemetry.CreateStdoutTracingEngine(env)
		if err != nil {
			return err
		}
	}

	infoGauge, err := app.metrics.Scope("service", metrics.Labels{
		"service":     serviceConfig.Name,
		"environment": string(serviceConfig.Environment),
	}).Gauge("info", "Service information (value is always 1)", nil)
	if err != nil {
		return fmt.Errorf("failed to create service_info gauge: %w", err)
	}
	infoGauge.Set(1)

	app.streams = make(map[int]RuntimeStream)
	app.endpointConsumers = make(map[int]RuntimeEndpointConsumer)
	app.consumeStatistics = make(map[config.LinkID]ConsumeStatistics)
	app.serdes = make(map[reflect.Type]serde.StreamSerializer)

	app.dataSources = make(map[int]DataSource)
	app.dataSinks = make(map[int]DataSink)
	app.managedDataConnectors = make(map[int]ManagedDataConnector)

	if dep != nil {
		app.delayPool, err = dep.DelayPool(ctx, env)
		if err != nil {
			return err
		}
	}
	if app.delayPool == nil {
		app.delayPool, err = pool.MakeDelayTaskPool(env)
		if err != nil {
			return err
		}
	}
	app.taskPools = make(map[string]pool.TaskPool)
	app.priorityTaskPools = make(map[string]pool.PriorityTaskPool)

	makeTaskPool := func(callSemantics config.CallSemanticsConfig) error {
		switch cs := callSemantics.(type) {
		case *config.FunctionCallSemanticsConfig: // skip
		case *config.TaskPoolCallSemanticsConfig:
			poolConfig := runtimeConfig.GetPoolByName(cs.PoolName)
			if poolConfig == nil {
				poolConfig = &config.PoolConfig{
					Name:           cs.PoolName,
					ExecutorsCount: 1,
					Properties:     nil,
				}
			}
			if _, ok := app.taskPools[poolConfig.Name]; !ok {
				p, err := pool.MakeTaskPool(env, poolConfig)
				if err != nil {
					return err
				}
				app.taskPools[poolConfig.Name] = p
			}
		case *config.PriorityTaskPoolCallSemanticsConfig:
			poolConfig := runtimeConfig.GetPoolByName(cs.PoolName)
			if poolConfig == nil {
				poolConfig = &config.PoolConfig{
					Name:           cs.PoolName,
					ExecutorsCount: 1,
					Properties:     nil,
				}
			}
			if _, ok := app.priorityTaskPools[poolConfig.Name]; !ok {
				p, err := pool.MakePriorityTaskPool(env, poolConfig)
				if err != nil {
					return err
				}
				app.priorityTaskPools[poolConfig.Name] = p
			}
		}
		return nil
	}

	if serviceConfig.DefaultCallSemantics != nil {
		callSemantics := serviceConfig.DefaultCallSemantics.Get()
		if callSemantics != nil {
			if err = makeTaskPool(callSemantics); err != nil {
				return err
			}
		}
	}

	for _, link := range runtimeConfig.GetConfig().GetLinks() {
		callSemantics := link.GetCallSemantics()
		if callSemantics != nil {
			if err = makeTaskPool(callSemantics); err != nil {
				return err
			}
		}
	}

	if !app.environment.HasCustomHTTPServer() {
		app.mux = http.NewServeMux()
	}

	return env.ServiceInit()
}

func (app *ServiceApp) HasCustomHTTPServer() bool {
	return false
}

func (app *ServiceApp) GetDataSource(id int) DataSource {
	return app.dataSources[id]
}

func (app *ServiceApp) AddDataSource(dataSource DataSource) {
	app.dataSources[dataSource.GetID()] = dataSource
}

func (app *ServiceApp) GetDataSink(id int) DataSink {
	return app.dataSinks[id]
}

func (app *ServiceApp) CreateKeyValueJoinStorage(_ api.JoinStorageType, _ store.JoinStorageConfig, _ Stream) store.Storage {
	return nil
}

func (app *ServiceApp) AddDataSink(dataSink DataSink) {
	app.dataSinks[dataSink.GetID()] = dataSink
}

func (app *ServiceApp) AddManagedDataConnector(connector ManagedDataConnector) {
	app.managedDataConnectors[connector.GetID()] = connector
}

func (app *ServiceApp) GetManagedDataConnector(id int) ManagedDataConnector {
	return app.managedDataConnectors[id]
}

func (app *ServiceApp) getSerializer(valueType reflect.Type) (serde.Serializer, error) {
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

	return nil, fmt.Errorf("getSerializer error. Unsupported type: %s", valueType.Name())
}

func (app *ServiceApp) Start(ctx context.Context) error {

	serviceConfig := app.environment.ServiceConfig()

	for _, stream := range app.streams {
		if err := stream.Build(); err != nil {
			return err
		}
	}

	for _, connector := range app.managedDataConnectors {
		if err := connector.Start(ctx); err != nil {
			return err
		}
	}
	for _, v := range app.storages {
		if err := v.Start(ctx); err != nil {
			return err
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
	for _, component := range app.components {
		if err := component.Start(ctx); err != nil {
			return err
		}
	}
	for _, v := range app.dataSinks {
		if err := v.Start(ctx); err != nil {
			return err
		}
	}
	// Sources are the graph admission boundary and may emit from Start. Start
	// them only after every downstream resource is ready.
	for _, v := range app.dataSources {
		if err := v.Start(ctx); err != nil {
			return err
		}
	}

	if len(serviceConfig.StatusHandler) > 0 {
		statusPath := "/" + strings.TrimPrefix(serviceConfig.StatusHandler, "/")
		app.environment.RegisterHTTPHandler(statusPath, http.HandlerFunc(app.statusHandler))
		app.environment.RegisterHTTPHandler(statusPath+"/data", http.HandlerFunc(app.dataHandler))
		app.environment.RegisterHTTPHandler(statusPath+"/graph", http.HandlerFunc(app.graphHandler))
		app.environment.RegisterHTTPHandler(statusPath+"/vis.min.js", http.HandlerFunc(app.visJSHandler))
		app.environment.RegisterHTTPHandler(statusPath+"/vis.min.css", http.HandlerFunc(app.visCSSHandler))
	}

	if len(serviceConfig.MetricsHandler) > 0 && app.metricsEngine.HTTPMetricsHandler() != nil {
		metricsPath := "/" + strings.TrimPrefix(serviceConfig.MetricsHandler, "/")
		app.environment.RegisterHTTPHandler(metricsPath, app.metricsEngine.HTTPMetricsHandler())
	}

	registeredHealthPaths := make(map[string]struct{}, 3)
	for _, configuredPath := range []string{
		serviceConfig.StartupHandler,
		serviceConfig.ReadinessHandler,
		serviceConfig.LivenessHandler,
	} {
		if configuredPath == "" {
			continue
		}
		healthPath := "/" + strings.TrimPrefix(configuredPath, "/")
		if _, exists := registeredHealthPaths[healthPath]; exists {
			continue
		}
		registeredHealthPaths[healthPath] = struct{}{}
		app.environment.RegisterHTTPHandler(healthPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		}))
	}

	if app.mux != nil {
		var handler http.Handler = app.mux
		if app.metricsEngine != nil {
			handler = app.metricsEngine.HTTPServerHandler(handler, ToSnakeCase(serviceConfig.Name))
		}
		if app.tracingEngine != nil {
			handler = app.tracingEngine.HTTPServerHandler(handler, ToSnakeCase(serviceConfig.Name))
		}
		addr := fmt.Sprintf("%s:%d", serviceConfig.HttpHost, serviceConfig.HttpPort)
		app.httpServerDone = make(chan struct{})
		app.httpServer = &http.Server{Handler: handler, Addr: addr}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		go func() {
			app.environment.Log().Info(ctx, "Http service listening", log.Str("service", serviceConfig.Name), log.Any("addr", app.httpServer.Addr))
			err := app.httpServer.Serve(ln)
			if !errors.Is(err, http.ErrServerClosed) {
				panic(err)
			}
			app.httpServerDone <- struct{}{}
		}()
	}

	return nil
}

func (app *ServiceApp) Release() {
}

func (app *ServiceApp) Stop(ctx context.Context) {
	serviceConfig := app.ServiceConfig()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		app.loader.Stop(ctx)
	}()

	for _, v := range app.components {
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

	if app.httpServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			go func() {
				if err := app.httpServer.Shutdown(ctx); err != nil {
					app.environment.Log().Warn(ctx, "server shutdown", log.Err(err))
				}
			}()
			select {
			case <-app.httpServerDone:
			case <-ctx.Done():
				app.environment.Log().Warn(ctx, "monitoring server stop timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
			}
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
		app.environment.Log().Warn(ctx, "ServiceApp stop timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	// Managed connectors may be used by graph work already admitted by another
	// source. In particular, a Cron job can be waiting for a Temporal result.
	// Drain ordinary sources first while those shared clients/workers are still
	// available; stopping both groups concurrently can strand the accepted job.
	wg = sync.WaitGroup{}
	for _, connector := range app.managedDataConnectors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connector.StopAdmission(ctx)
		}()
	}
	done = make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		app.environment.Log().Warn(ctx, "managed connector admission shutdown timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	// Source admission has stopped and source-owned work (including Cron and
	// Temporal worker executions) has drained. Keep pools and storages alive
	// until nested graph work has also completed.
	parallelDone := make(chan struct{})
	go func() {
		app.parallel.Wait()
		close(parallelDone)
	}()
	select {
	case <-parallelDone:
	case <-ctx.Done():
		app.environment.Log().Warn(ctx, "ServiceApp graph drain timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	wg = sync.WaitGroup{}
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

	for _, v := range app.storages {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}

	done = make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		app.environment.Log().Warn(ctx, "runtime pool and storage shutdown timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	wg = sync.WaitGroup{}
	for _, v := range app.dataSinks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop(ctx)
		}()
	}

	done = make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		app.environment.Log().Warn(ctx, "ServiceApp stop timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	// Durable clients remain open until every sink submission has drained.
	// Their Activity Workers were stopped with the admission sources above.
	wg = sync.WaitGroup{}
	for _, connector := range app.managedDataConnectors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connector.Stop(ctx)
		}()
	}
	done = make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		app.environment.Log().Warn(ctx, "managed connector shutdown timeout", log.Str("service", serviceConfig.Name), log.Err(ctx.Err()))
	}

	shutdownTelemetry := func(name string, shutdown func(context.Context) error) {
		result := make(chan error, 1)
		go func() { result <- shutdown(ctx) }()
		select {
		case err := <-result:
			if err != nil {
				app.environment.Log().Warn(ctx, name+" shutdown", log.Err(err))
			}
		case <-ctx.Done():
			app.environment.Log().Warn(ctx, name+" shutdown timeout", log.Err(ctx.Err()))
		}
	}
	shutdownTelemetry("metrics engine", app.metricsEngine.Shutdown)
	shutdownTelemetry("tracing engine", app.tracingEngine.Shutdown)
	shutdownTelemetry("logs engine", app.logsEngine.Shutdown)
}

func (app *ServiceApp) Delay(ctx context.Context, duration time.Duration, f func()) error {
	if app.isolated {
		return app.environment.Delay(ctx, duration, f)
	}
	return app.delayPool.Delay(ctx, duration, f)
}

func (app *ServiceApp) GetTaskPool(name string) pool.TaskPool {
	if app.isolated {
		return app.environment.GetTaskPool(name)
	}
	return app.taskPools[name]
}

func (app *ServiceApp) GetPriorityTaskPool(name string) pool.PriorityTaskPool {
	if app.isolated {
		return app.environment.GetPriorityTaskPool(name)
	}
	return app.priorityTaskPools[name]
}
