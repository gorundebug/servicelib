/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/xid"
	"github.com/spf13/viper"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
)

type Caller[T any] interface {
	Consumer[T]
}

type ConsumeStatistics interface {
	Count() int64
}

type ServiceLoader interface {
	Stop()
}

type ServiceExecutionRuntime interface {
	updateConfig(config *config.RuntimeConfig)
	initRuntime(
		name string,
		env RuntimeEnvironment,
		dep environment.ServiceDependencies,
		loader ServiceLoader,
		runtimeConfig *config.RuntimeConfig,
	) error
	registerSerializer(tp reflect.Type, serializer serde.StreamSerializer)
	getSerializer(valueType reflect.Type) (serde.Serializer, error)
	getRegisteredSerializer(tp reflect.Type) serde.StreamSerializer
	registerConsumeStatistics(linkID config.LinkID, statistics ConsumeStatistics)
}

func getPath(argPath string) (string, error) {
	var filePath string
	if !filepath.IsAbs(argPath) {
		dir, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("path error: %w", err)
		}
		filePath = filepath.Join(dir, argPath)
	} else {
		filePath = argPath
	}
	return filePath, nil
}

type serviceLoader[Environment RuntimeEnvironment, Cfg config.Config] struct {
	watcher                    *fsnotify.Watcher
	wg                         sync.WaitGroup
	service                    Environment
	viper                      *viper.Viper
	configReloadSuccessCounter metrics.Int64Counter
	configReloadErrorCounter   metrics.Int64Counter
}

func (l *serviceLoader[Environment, Cfg]) createConfigReloadCounters() {
	scope := l.service.Metrics().Scope("service", metrics.Labels{
		"service": l.service.ServiceConfig().Name,
	})
	var err error
	l.configReloadSuccessCounter, err = scope.Counter("config_reloads_total", "Total number of config reload attempts", metrics.Labels{"event": "success"})
	if err != nil {
		l.service.Log().Errorf("failed to create config_reloads_total counter: %v", err)
	}
	l.configReloadErrorCounter, err = scope.Counter("config_reloads_total", "Total number of config reload attempts", metrics.Labels{"event": "error"})
	if err != nil {
		l.service.Log().Errorf("failed to create config_reloads_total counter: %v", err)
	}
}

func (l *serviceLoader[Environment, Cfg]) Stop() {
	if err := l.watcher.Close(); err != nil {
		l.service.Log().Warnf("watcher close error: %s", err)
	}
}

func (l *serviceLoader[Environment, Cfg]) init(
	ctx context.Context,
	name string,
	dep environment.ServiceDependencies,
	baseConfigPath *string,
	overrideConfigPath *string,
	serviceMaker func() Environment,
	configMaker func() Cfg,
) error {

	l.viper = viper.New()
	l.viper.SetConfigType("yaml")
	l.service = serviceMaker()
	cfg := configMaker()

	if baseConfigPath == nil || *baseConfigPath == "" {

		if err := cfg.ApplyEnvironment(); err != nil {
			return fmt.Errorf("apply environment error: %w", err)
		}

		runtimeCfg, err := config.NewRuntimeConfig(cfg)
		if err != nil {
			return fmt.Errorf("create runtime config error: %w", err)
		}

		if err := l.service.GetRuntime().initRuntime(name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}
		l.createConfigReloadCounters()
		return nil
	}

	baseConfigFileName, err := getPath(*baseConfigPath)
	if err != nil {
		return fmt.Errorf("get config file path error: %w", err)
	}

	if _, err = os.Stat(baseConfigFileName); os.IsNotExist(err) {
		return fmt.Errorf("config file %q does not exist", baseConfigFileName)
	}

	configData, err := os.ReadFile(baseConfigFileName)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	if overrideConfigPath != nil && *overrideConfigPath != "" {
		overrideConfigFileName, err := getPath(*overrideConfigPath)
		if err != nil {
			return fmt.Errorf("get override config file path error: %w", err)
		}

		if _, err = os.Stat(overrideConfigFileName); os.IsNotExist(err) {
			return fmt.Errorf("override config file %q does not exist", overrideConfigFileName)
		}

		overrideConfigFile := filepath.Clean(overrideConfigFileName)
		overrideConfigDir, _ := filepath.Split(overrideConfigFile)
		realOverrideConfigFile, err := filepath.EvalSymlinks(overrideConfigFileName)
		if err != nil {
			return fmt.Errorf("error evaluating override config file path: %w", err)
		}

		closeWatcher := true

		l.watcher, err = fsnotify.NewWatcher()
		if err != nil {
			return fmt.Errorf("failed to create watcher: %w", err)
		}

		defer func() {
			if closeWatcher {
				_ = l.watcher.Close()
			}
		}()

		if err := l.watcher.Add(overrideConfigDir); err != nil {
			return fmt.Errorf("watcher add error: %w", err)
		}

		overrideData, err := os.ReadFile(realOverrideConfigFile)
		if err != nil {
			return fmt.Errorf("error reading override config file: %w", err)
		}

		if err = l.viper.ReadConfig(bytes.NewReader(configData)); err != nil {
			return fmt.Errorf("viper read config error: %w", err)
		}

		if err = l.viper.MergeConfig(bytes.NewReader(overrideData)); err != nil {
			return fmt.Errorf("viper merge config error: %w", err)
		}

		if err := l.viper.Unmarshal(cfg); err != nil {
			return fmt.Errorf("unmarshal config error: %w", err)
		}

		if err := cfg.ApplyEnvironment(); err != nil {
			return fmt.Errorf("apply environment error: %w", err)
		}

		runtimeCfg, err := config.NewRuntimeConfig(cfg)
		if err != nil {
			return fmt.Errorf("create runtime config error: %w", err)
		}

		if err := l.service.GetRuntime().initRuntime(name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}

		l.createConfigReloadCounters()
		closeWatcher = false

		l.wg.Add(1)
		go func() {
			defer l.wg.Done()

			for {
				select {
				case event, ok := <-l.watcher.Events:
					if !ok {
						return
					}
					currentOverrideConfigFile, err := filepath.EvalSymlinks(overrideConfigFileName)
					if err != nil {
						l.service.Log().Errorf("error evaluating override config file path: %s", err)
						continue
					}
					eventPath, err := filepath.EvalSymlinks(event.Name)
					if err != nil {
						l.service.Log().Errorf("error evaluating override config file path: %s", err)
						continue
					}
					// we only care about the config file with the following cases:
					// 1 - if the config file was modified or created
					// 2 - if the real path to the config file changed (eg: k8s ConfigMap replacement)
					if (filepath.Clean(eventPath) == overrideConfigFile &&
						(event.Has(fsnotify.Write) || event.Has(fsnotify.Create))) ||
						(currentOverrideConfigFile != "" && currentOverrideConfigFile != realOverrideConfigFile) {
						realOverrideConfigFile = currentOverrideConfigFile

						newOverrideData, err := os.ReadFile(realOverrideConfigFile)
						if err != nil {
							l.service.Log().Errorf("error reading override config file: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newConfigData, err := os.ReadFile(baseConfigFileName)
						if err != nil {
							l.service.Log().Errorf("error reading config file: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newViper := viper.New()
						newViper.SetConfigType("yaml")

						if err := newViper.ReadConfig(bytes.NewReader(newConfigData)); err != nil {
							l.service.Log().Errorf("viper read config error: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						if err := newViper.MergeConfig(bytes.NewReader(newOverrideData)); err != nil {
							l.service.Log().Errorf("viper merge config error: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newCfg := configMaker()

						if err := newViper.Unmarshal(newCfg); err != nil {
							l.service.Log().Errorf("Viper unmarshal config error: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						if err := newCfg.ApplyEnvironment(); err != nil {
							l.service.Log().Errorf("apply environment error: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						l.viper = newViper

						newRuntimeCfg, err := config.NewRuntimeConfig(newCfg)
						if err != nil {
							l.service.Log().Errorf("create runtime config error: %s", err)
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
						} else {
							if l.configReloadSuccessCounter != nil {
								l.configReloadSuccessCounter.Inc(ctx)
							}
						}
						l.service.GetRuntime().updateConfig(newRuntimeCfg)
					}
				case err, ok := <-l.watcher.Errors:
					if ok {
						l.service.Log().Errorf("watcher error: %s", err)
					}
				}
			}
		}()

	} else {
		if err := l.viper.ReadConfig(bytes.NewReader(configData)); err != nil {
			return fmt.Errorf("viper read config error: %w", err)
		}

		if err := l.viper.Unmarshal(cfg); err != nil {
			return fmt.Errorf("unmarshal config error: %w", err)
		}

		if err := cfg.ApplyEnvironment(); err != nil {
			return fmt.Errorf("apply environment error: %w", err)
		}

		runtimeCfg, err := config.NewRuntimeConfig(cfg)
		if err != nil {
			return fmt.Errorf("create runtime config error: %w", err)
		}

		if err := l.service.GetRuntime().initRuntime(name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}
		l.createConfigReloadCounters()
	}

	return nil
}

func MakeService[Environment RuntimeEnvironment, Cfg config.Config](
	ctx context.Context,
	name string,
	dep environment.ServiceDependencies,
	baseConfigPath *string,
	overrideConfigPath *string,
	serviceMaker func() Environment,
	configMaker func() Cfg,
) (Environment, error) {
	loader := &serviceLoader[Environment, Cfg]{}
	if err := loader.init(ctx, name, dep, baseConfigPath, overrideConfigPath, serviceMaker, configMaker); err != nil {
		var env Environment
		return env, fmt.Errorf("make service %q error: %w", name, err)
	}
	return loader.service, nil
}

func MakeSerdeForType(tp reflect.Type, rt ServiceExecutionRuntime) (serde.Serializer, error) {
	var err error
	var ser serde.Serializer

	ser, err = rt.getSerializer(tp)
	if err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = MakeSerdeForType(tp.Elem(), rt)
			if err != nil {
				return nil, err
			}
			return serde.MakeArraySerde(tp, ser), nil
		} else if tp.Kind() == reflect.Map {
			serKeyArray, err := MakeSerdeForType(reflect.SliceOf(tp.Key()), rt)
			if err != nil {
				return nil, err
			}
			serValueArray, err := MakeSerdeForType(reflect.SliceOf(tp.Elem()), rt)
			if err != nil {
				return nil, err
			}
			return serde.MakeMapSerde(tp, serKeyArray, serValueArray), nil
		}
	}
	return ser, err
}

func MakeTypedArraySerde[T any](rt ServiceExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	elementType := v.Type().Elem()
	serElm, err := MakeSerdeForType(elementType, rt)
	if err != nil {
		return nil, err
	}
	arrSer, err := serde.MakeTypedArraySerde[T](serElm)
	if err != nil {
		return nil, err
	}
	return arrSer, nil
}

func MakeTypedMapSerde[T any](rt ServiceExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	mapType := v.Type()
	keyType := mapType.Key()
	keyArraySerde, err := MakeSerdeForType(reflect.SliceOf(keyType), rt)
	if err != nil {
		return nil, err
	}
	valueType := mapType.Elem()
	valueArraySerde, err := MakeSerdeForType(reflect.SliceOf(valueType), rt)
	if err != nil {
		return nil, err
	}
	mapSer, err := serde.MakeTypedMapSerde[T](keyArraySerde, valueArraySerde)
	if err != nil {
		return nil, err
	}
	return mapSer, nil
}

func registerSerde[T any](rt ServiceExecutionRuntime, ser serde.StreamSerde[T]) {
	rt.registerSerializer(serde.GetSerdeType[T](), ser)
}

func getRegisteredSerde[T any](rt ServiceExecutionRuntime) serde.StreamSerde[T] {
	if ser := rt.getRegisteredSerializer(serde.GetSerdeType[T]()); ser != nil {
		return ser.(serde.StreamSerde[T])
	}
	return nil
}

func MakeSerde[T any](env RuntimeEnvironment) serde.StreamSerde[T] {
	rt := env.GetRuntime()
	if ser := getRegisteredSerde[T](rt); ser != nil {
		return ser
	}
	tp := serde.GetSerdeType[T]()

	var err error
	var ser serde.Serializer

	if ser, err = rt.getSerializer(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = MakeTypedArraySerde[T](rt)
		} else if tp.Kind() == reflect.Map {
			ser, err = MakeTypedMapSerde[T](rt)
		}
	}
	if ser == nil || err != nil {
		ser = serde.MakeStubSerde[T]()
	}
	serT, ok := ser.(serde.Serde[T])
	if !ok {
		panic(fmt.Sprintf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name()))
	}
	streamSer := serde.MakeStreamSerde(serT)
	registerSerde[T](rt, streamSer)
	return streamSer
}

func MakeKeyValueSerde[K comparable, V any](env RuntimeEnvironment) serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]] {
	rt := env.GetRuntime()
	if ser := getRegisteredSerde[datastruct.KeyValue[K, V]](rt); ser != nil {
		return ser.(serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]])
	}
	tp := serde.GetSerdeType[K]()

	var err error
	var ser serde.Serializer

	if ser, err = rt.getSerializer(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = MakeTypedArraySerde[K](rt)
		} else if tp.Kind() == reflect.Map {
			ser, err = MakeTypedMapSerde[K](rt)
		}
	}
	if err != nil {
		ser = serde.MakeStubSerde[K]()
	}
	serdeK, ok := ser.(serde.Serde[K])
	if !ok {
		panic(fmt.Sprintf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name()))
	}

	tp = serde.GetSerdeType[V]()
	if ser, err = rt.getSerializer(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = MakeTypedArraySerde[V](rt)
		} else if tp.Kind() == reflect.Map {
			ser, err = MakeTypedMapSerde[V](rt)
		}
	}
	if ser == nil || err != nil {
		ser = serde.MakeStubSerde[V]()
	}
	serdeV, ok := ser.(serde.Serde[V])
	if !ok {
		panic(fmt.Sprintf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name()))
	}
	streamSer := serde.MakeStreamKeyValueSerde[K, V](serdeK, serdeV)
	registerSerde[datastruct.KeyValue[K, V]](rt, streamSer)
	return streamSer
}

var keyValuePattern = regexp.MustCompile(`^KeyValue\[\w+,\w+]$`)

func IsKeyValueType[T any]() bool {
	tp := serde.GetSerdeType[T]()
	return tp.PkgPath() == "github.com/gorundebug/servicelib/runtime/datastruct" && keyValuePattern.MatchString(tp.Name())
}

var defaultCallSemantic = config.FunctionCallSemanticsConfig{}

func MakeCaller[T any](source TypedStream[T]) Caller[T] {
	env := source.GetRuntimeEnvironment()
	cfg := env.RuntimeConfig()
	serviceConfig := env.ServiceConfig()
	consumer := source.GetConsumer()
	link := cfg.GetLink(source.GetID(), consumer.GetID())

	var callSemantics config.CallSemanticsConfig

	if link != nil {
		callSemantics = link.GetCallSemantics()
	}

	if callSemantics == nil {
		if serviceConfig.DefaultCallSemantics != nil {
			callSemantics = serviceConfig.DefaultCallSemantics.Get()
		}
		if callSemantics == nil {
			callSemantics = &defaultCallSemantic
		}
	}

	scope := env.Metrics().Scope("stream", metrics.Labels{
		"service": env.ServiceConfig().Name,
		"from":    source.GetName(),
		"to":      consumer.GetName(),
	})
	messagesCounter, err := scope.Counter("messages_total", "Total number of messages processed by stream link", nil)
	if err != nil {
		env.Log().Errorf("failed to create stream_messages_total counter: %v", err)
	}

	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(serviceConfig.Name)
	}

	var streamCaller Caller[T]
	consumeStat := &consumeStatistics{}
	switch cs := callSemantics.(type) {
	case *config.FunctionCallSemanticsConfig:
		c := &directCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
		}
		streamCaller = c

	case *config.TaskPoolCallSemanticsConfig:
		taskPool := env.GetTaskPool(cs.PoolName)
		c := &taskPoolCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
			pool: taskPool,
		}
		streamCaller = c

	case *config.PriorityTaskPoolCallSemanticsConfig:
		priorityTaskPool := env.GetPriorityTaskPool(cs.PoolName)
		c := &priorityTaskPoolCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
			pool:     priorityTaskPool,
			priority: cs.Priority,
		}
		streamCaller = c

	default:
		panic(fmt.Sprintf("undefined callSemantics type [%T] ", callSemantics))
	}

	env.GetRuntime().registerConsumeStatistics(config.LinkID{From: source.GetID(), To: consumer.GetID()}, consumeStat)
	return streamCaller
}

type consumeStatistics struct {
	count atomic.Int64
}

func (s *consumeStatistics) Count() int64 {
	return s.count.Load()
}

func (s *consumeStatistics) Inc() {
	s.count.Add(1)
}

// noopSpan is returned by StartSpan when tracing is disabled, avoiding nil checks in callers.
type noopSpan struct{}

func (noopSpan) End()                                                {}
func (noopSpan) SetAttributes(_ ...tracing.Attribute)                {}
func (noopSpan) RecordError(_ error)                                 {}
func (noopSpan) SetStatus(_ tracing.StatusCode, _ string)            {}
func (noopSpan) AddEvent(_ string, _ ...tracing.Attribute)           {}
func (noopSpan) SpanContext() tracing.SpanContext                     { return tracing.SpanContext{} }

type caller[T any] struct {
	statistics      *consumeStatistics
	source          TypedStream[T]
	consumer        TypedStreamConsumer[T]
	messagesCounter metrics.Int64Counter
	tracer          tracing.Tracer
}

type directCaller[T any] struct {
	caller[T]
}

func (c *directCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	if c.tracer != nil {
		var span tracing.Span
		ctx, span = c.tracer.Start(ctx, "stream.call",
			tracing.StringAttr("from", c.source.GetName()),
			tracing.StringAttr("to", c.consumer.GetName()),
		)
		defer span.End()
	}
	c.consumer.Consume(ctx, value)
}

type taskPoolCaller[T any] struct {
	caller[T]
	pool pool.TaskPool
}

func (c *taskPoolCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	if c.tracer != nil {
		var span tracing.Span
		ctx, span = c.tracer.Start(ctx, "stream.call",
			tracing.StringAttr("from", c.source.GetName()),
			tracing.StringAttr("to", c.consumer.GetName()),
			tracing.StringAttr("type", "taskpool"),
		)
		c.pool.AddTask(ctx, func() {
			defer span.End()
			c.consumer.Consume(ctx, value)
		})
	} else {
		c.pool.AddTask(ctx, func() {
			c.consumer.Consume(ctx, value)
		})
	}
}

type priorityTaskPoolCaller[T any] struct {
	caller[T]
	pool     pool.PriorityTaskPool
	priority int
}

func (c *priorityTaskPoolCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	if c.tracer != nil {
		var span tracing.Span
		ctx, span = c.tracer.Start(ctx, "stream.call",
			tracing.StringAttr("from", c.source.GetName()),
			tracing.StringAttr("to", c.consumer.GetName()),
			tracing.StringAttr("type", "prioritytaskpool"),
		)
		c.pool.AddTask(ctx, c.priority, func() {
			defer span.End()
			c.consumer.Consume(ctx, value)
		})
	} else {
		c.pool.AddTask(ctx, c.priority, func() {
			c.consumer.Consume(ctx, value)
		})
	}
}

type ServiceStream[T any] struct {
	environment RuntimeEnvironment
	id          int
	tracer      tracing.Tracer
}

func (s *ServiceStream[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *ServiceStream[T]) GetName() string {
	return s.GetConfig().GetName()
}

func (s *ServiceStream[T]) GetID() int {
	return s.id
}

func (s *ServiceStream[T]) GetConfig() config.StreamConfig {
	return s.environment.RuntimeConfig().GetStreamConfigByID(s.id)
}

func (s *ServiceStream[T]) GetEnvironment() environment.ServiceEnvironment {
	return s.environment
}

func (s *ServiceStream[T]) GetTransformationName() string {
	return config.GetTransformationName(s.GetConfig().GetType())
}

func MakeServiceStream[T any](id int, env RuntimeEnvironment) ServiceStream[T] {
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	return ServiceStream[T]{
		environment: env,
		id:          id,
		tracer:      tr,
	}
}

// StartSpan starts a new span for this stream's operation.
// Returns a no-op span when tracing is disabled, so callers never need nil checks.
func (s *ServiceStream[T]) StartSpan(ctx context.Context, operation string) (context.Context, tracing.Span) {
	if s.tracer == nil {
		return ctx, noopSpan{}
	}
	return s.tracer.Start(ctx, operation, tracing.StringAttr("stream", s.GetName()))
}

type ConsumedStream[T any] struct {
	ServiceStream[T]
	downstream Caller[T]
	serde      serde.StreamSerde[T]
	consumer   TypedStreamConsumer[T]
}

func MakeConsumedStream[T any](id int, env RuntimeEnvironment, ser serde.StreamSerde[T]) ConsumedStream[T] {
	return ConsumedStream[T]{
		ServiceStream: MakeServiceStream[T](id, env),
		serde:         ser,
	}
}

func (s *ConsumedStream[T]) GetConsumers() []Stream {
	if s.consumer != nil {
		return []Stream{s.consumer}
	}
	return []Stream{}
}

func (s *ConsumedStream[T]) GetSerde() serde.StreamSerde[T] {
	return s.serde
}

func (s *ConsumedStream[T]) Build() error {
	return nil
}

func (s *ConsumedStream[T]) GetRuntimeEnvironment() RuntimeEnvironment {
	return s.environment
}

func (s *ConsumedStream[T]) SetDownstream(consumer TypedStreamConsumer[T], stream TypedStream[T]) {
	if s.consumer != nil {
		panic(fmt.Sprintf("consumer already assigned to the stream %s", s.ServiceStream.GetConfig().GetName()))
	}
	s.consumer = consumer
	s.downstream = MakeCaller[T](stream)
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}

func (s *ConsumedStream[T]) Collector() Collect[T] {
	return MakeCollector[T](s.downstream)
}

func (s *ConsumedStream[T]) Emit(ctx context.Context, value T) {
	if s.downstream != nil {
		s.downstream.Consume(ctx, value)
	}
}

type StreamFunction[T any] struct {
	stream ServiceStream[T] //nolint:unused
}

func (f *StreamFunction[T]) BeforeCall() {
}

func (f *StreamFunction[T]) AfterCall() {
}

type StreamId interface {
	GetID() string
}

type streamId struct {
	id string
}

func (s *streamId) GetID() string {
	return s.id
}

type streamIdKeyType struct{}

var streamIdKey = streamIdKeyType{}

func WithStreamId(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, streamIdKey, &streamId{id: id})
}

func StreamIdFromContext(ctx context.Context) (StreamId, bool) {
	v := ctx.Value(streamIdKey)
	if v == nil {
		return nil, false
	}
	sid, ok := v.(StreamId)
	return sid, ok
}

func NewStreamID() string {
	return xid.New().String()
}
