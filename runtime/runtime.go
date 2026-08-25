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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/xid"
	"github.com/spf13/viper"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
)

type Caller[T any] interface {
	Consumer[T]
	IsAsync() bool
}

type ConsumeStatistics interface {
	Count() int64
}

type ServiceLoader interface {
	Stop(ctx context.Context)
}

type ServiceExecutionRuntime interface {
	updateConfig(config *config.RuntimeConfig)
	initRuntime(
		ctx context.Context,
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
	registerLinkInfo(linkID config.LinkID, callSemantics config.CallSemanticsConfig)
	beginParallel()
	endParallel()
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

func (l *serviceLoader[Environment, Cfg]) createConfigReloadCounters(ctx context.Context) {
	scope := l.service.Metrics().Scope("service", metrics.Labels{
		"service": l.service.ServiceConfig().Name,
	})
	var err error
	l.configReloadSuccessCounter, err = scope.Counter("config_reloads_total", "Total number of config reload attempts", metrics.Labels{"event": "success"})
	if err != nil {
		l.service.Log().Error(ctx, "failed to create config_reloads_total counter", log.Err(err))
	}
	l.configReloadErrorCounter, err = scope.Counter("config_reloads_total", "Total number of config reload attempts", metrics.Labels{"event": "error"})
	if err != nil {
		l.service.Log().Error(ctx, "failed to create config_reloads_total counter", log.Err(err))
	}
}

func (l *serviceLoader[Environment, Cfg]) Stop(ctx context.Context) {
	if l.watcher == nil {
		return
	}
	if err := l.watcher.Close(); err != nil {
		l.service.Log().Warn(ctx, "watcher close error", log.Err(err))
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

		if err := l.service.GetRuntime().initRuntime(ctx, name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}
		l.createConfigReloadCounters(ctx)
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

		if err := l.service.GetRuntime().initRuntime(ctx, name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}

		l.createConfigReloadCounters(ctx)
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
						l.service.Log().Error(ctx, "error evaluating override config file path", log.Err(err))
						continue
					}
					eventPath, err := filepath.EvalSymlinks(event.Name)
					if err != nil {
						l.service.Log().Error(ctx, "error evaluating override config file path", log.Err(err))
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
							l.service.Log().Error(ctx, "error reading override config file", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newConfigData, err := os.ReadFile(baseConfigFileName)
						if err != nil {
							l.service.Log().Error(ctx, "error reading config file", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newViper := viper.New()
						newViper.SetConfigType("yaml")

						if err := newViper.ReadConfig(bytes.NewReader(newConfigData)); err != nil {
							l.service.Log().Error(ctx, "viper read config error", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						if err := newViper.MergeConfig(bytes.NewReader(newOverrideData)); err != nil {
							l.service.Log().Error(ctx, "viper merge config error", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newCfg := configMaker()

						if err := newViper.Unmarshal(newCfg); err != nil {
							l.service.Log().Error(ctx, "viper unmarshal config error", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						if err := newCfg.ApplyEnvironment(); err != nil {
							l.service.Log().Error(ctx, "apply environment error", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						newRuntimeCfg, err := config.NewRuntimeConfig(newCfg)
						if err != nil {
							l.service.Log().Error(ctx, "create runtime config error", log.Err(err))
							if l.configReloadErrorCounter != nil {
								l.configReloadErrorCounter.Inc(ctx)
							}
							continue
						}

						l.viper = newViper
						if l.configReloadSuccessCounter != nil {
							l.configReloadSuccessCounter.Inc(ctx)
						}
						l.service.GetRuntime().updateConfig(newRuntimeCfg)
					}
				case err, ok := <-l.watcher.Errors:
					if ok {
						l.service.Log().Error(ctx, "watcher error", log.Err(err))
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

		if err := l.service.GetRuntime().initRuntime(ctx, name, l.service, dep, l, runtimeCfg); err != nil {
			return fmt.Errorf("service init error: %w", err)
		}
		l.createConfigReloadCounters(ctx)
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

func MakeCaller[T any](source TypedStream[T], consumer TypedStreamConsumer[T]) (Caller[T], error) {
	env := source.GetRuntimeEnvironment()
	cfg := env.RuntimeConfig()
	serviceConfig := env.ServiceConfig()
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

	fromName := source.GetName()
	toName := consumer.GetName()
	scope := env.Metrics().Scope("stream", metrics.Labels{
		"service": env.ServiceConfig().Name,
		"from":    fromName,
		"to":      toName,
	})
	messagesCounter, err := scope.Counter("messages_total", "Total number of messages processed by stream link", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream_messages_total counter: %w", err)
	}

	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(serviceConfig.Name)
	}

	var streamCaller Caller[T]
	consumeStat := &consumeStatistics{}
	linkID := config.LinkID{From: source.GetID(), To: consumer.GetID()}
	switch cs := callSemantics.(type) {
	case *config.FunctionCallSemanticsConfig:
		c := &directCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				fromName:        fromName,
				toName:          toName,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
			async: cs.Async,
		}
		streamCaller = c

	case *config.TaskPoolCallSemanticsConfig:
		taskPool := env.GetTaskPool(cs.PoolName)
		c := &taskPoolCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				fromName:        fromName,
				toName:          toName,
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
				fromName:        fromName,
				toName:          toName,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
			pool:     priorityTaskPool,
			priority: cs.Priority,
		}
		streamCaller = c

	case *config.ParallelCallSemanticsConfig:
		c := &parallelCaller[T]{
			caller: caller[T]{
				source:          source,
				consumer:        consumer,
				fromName:        fromName,
				toName:          toName,
				statistics:      consumeStat,
				messagesCounter: messagesCounter,
				tracer:          tr,
			},
			runtime: env.GetRuntime(),
		}
		streamCaller = c

	case *config.DurableCallSemanticsConfig:
		transport := env.GetDurableTransport(cs.IdDataConnector)
		if transport == nil {
			return nil, fmt.Errorf("durable transport for Temporal connector id=%d is not registered", cs.IdDataConnector)
		}
		ser := source.GetSerde()
		if err := transport.RegisterLink(linkID, func(activityCtx context.Context, envelope DurableEnvelope) error {
			value, err := ser.Deserialize(envelope.Payload)
			if err != nil {
				return fmt.Errorf("deserialize durable link %d->%d payload: %w", linkID.From, linkID.To, err)
			}
			activityCtx, cancel := durableEnvelopeContext(activityCtx, envelope)
			defer cancel()
			if tr != nil && tracing.SamplingEnabled(activityCtx) {
				var span tracing.Span
				activityCtx, span = tr.Start(activityCtx, "temporal.activity",
					tracing.StringAttr("boundary", "durable_call"),
					tracing.StringAttr("from", fromName),
					tracing.StringAttr("to", toName),
				)
				BindDurableCallSpan(activityCtx, span)
			}
			consumer.Consume(activityCtx, value)
			return nil
		}); err != nil {
			return nil, err
		}
		streamCaller = &durableCaller[T]{
			caller: caller[T]{
				source: source, consumer: consumer, fromName: fromName, toName: toName,
				statistics: consumeStat, messagesCounter: messagesCounter, tracer: tr,
			},
			linkID: linkID, transport: transport, serde: ser,
		}

	default:
		return nil, fmt.Errorf("undefined callSemantics type [%T]", callSemantics)
	}

	env.GetRuntime().registerConsumeStatistics(linkID, consumeStat)
	env.GetRuntime().registerLinkInfo(linkID, callSemantics)
	return streamCaller, nil
}

type durableCaller[T any] struct {
	caller[T]
	linkID    config.LinkID
	transport DurableTransport
	serde     serde.StreamSerde[T]
}

func (c *durableCaller[T]) startSpan(ctx context.Context) (context.Context, tracing.Span) {
	if !c.samplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return c.tracer.Start(ctx, "stream.call",
		tracing.StringAttr("from", c.fromName),
		tracing.StringAttr("to", c.toName),
		tracing.StringAttr("type", "durable"),
	)
}

// nextDurableCallID returns one identity for one logical DurableCall emission.
// Calls made while executing a Temporal Activity are derived from the parent
// call and a per-payload occurrence number. An Activity retry recreates the
// scope and therefore produces the same child IDs, while two equal values
// emitted by the same Activity still remain two distinct durable calls.
func nextDurableCallID(ctx context.Context, linkID config.LinkID, payload []byte) string {
	scope, ok := DurableCallContextFromContext(ctx)
	if !ok || scope == nil {
		// There is no durable parent outside an Activity. A fresh identity is the
		// only honest default: stream IDs identify a request, not every message
		// emitted by that request.
		return NewStreamID()
	}
	payloadDigest := sha256.Sum256(payload)
	key := fmt.Sprintf("%d\x00%d\x00%x", linkID.From, linkID.To, payloadDigest)
	scope.mu.Lock()
	if scope.counts == nil {
		scope.counts = make(map[string]uint64)
	}
	scope.counts[key]++
	occurrence := scope.counts[key]
	scope.mu.Unlock()

	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%s\x00%s\x00%d", scope.parentID, key, occurrence)
	return hex.EncodeToString(digest.Sum(nil))
}

func (c *durableCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	ctx, span := c.startSpan(ctx)
	defer span.End()
	payload, err := c.serde.Serialize(value)
	if err != nil {
		tracing.SpanError(span, err)
		c.source.GetEnvironment().Log().Error(ctx, "durable call serialization failed", log.Err(err))
		return
	}
	streamID, ok := StreamIdFromContext(ctx)
	if !ok {
		ctx = WithStreamId(ctx, NewStreamID())
		streamID, _ = StreamIdFromContext(ctx)
	}
	priority, _ := PriorityFromContext(ctx)
	envelope := DurableEnvelope{
		Version: 1, From: c.linkID.From, To: c.linkID.To,
		StreamID: streamID.GetID(), Priority: priority,
		Payload: payload,
	}
	if deadline, present := ctx.Deadline(); present {
		envelope.DeadlineUnixNano = deadline.UTC().UnixNano()
	}
	envelope.CallID = nextDurableCallID(ctx, c.linkID, payload)
	if err := c.transport.SubmitLink(ctx, c.linkID, envelope); err != nil {
		tracing.SpanError(span, err)
		c.source.GetEnvironment().Log().Error(ctx, "durable call submission failed", log.Err(err))
	}
}

func (c *durableCaller[T]) IsAsync() bool { return true }

func durableEnvelopeContext(parent context.Context, envelope DurableEnvelope) (context.Context, context.CancelFunc) {
	ctx := parent
	if _, present := DurableCallContextFromContext(ctx); !present {
		// Transport adapters normally attach the processing-side scope before
		// invoking this handler. Keep envelope reconstruction self-contained for
		// direct adapters and tests while never replacing an already active scope.
		ctx = WithDurableCallContext(ctx, NewDurableCallContext(envelope.CallID, nil, nil))
	}
	if envelope.StreamID != "" {
		ctx = WithStreamId(ctx, envelope.StreamID)
	}
	ctx = WithPriority(ctx, envelope.Priority)
	if envelope.DeadlineUnixNano > 0 {
		return context.WithDeadline(ctx, time.Unix(0, envelope.DeadlineUnixNano))
	}
	return ctx, func() {}
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

func (noopSpan) End()                                      {}
func (noopSpan) SetAttributes(_ ...tracing.Attribute)      {}
func (noopSpan) RecordError(_ error)                       {}
func (noopSpan) SetStatus(_ tracing.StatusCode, _ string)  {}
func (noopSpan) AddEvent(_ string, _ ...tracing.Attribute) {}
func (noopSpan) SpanContext() tracing.SpanContext          { return tracing.SpanContext{} }

type caller[T any] struct {
	statistics      *consumeStatistics
	source          TypedStream[T]
	consumer        TypedStreamConsumer[T]
	fromName        string
	toName          string
	messagesCounter metrics.Int64Counter
	tracer          tracing.Tracer
}

func (c *caller[T]) samplingEnabled(ctx context.Context) bool {
	return c.tracer != nil && tracing.SamplingEnabled(ctx)
}

type directCaller[T any] struct {
	caller[T]
	async bool
}

func (c *directCaller[T]) startSpan(ctx context.Context) (context.Context, tracing.Span) {
	if !c.samplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return c.tracer.Start(ctx, "stream.call",
		tracing.StringAttr("from", c.fromName),
		tracing.StringAttr("to", c.toName),
	)
}

func (c *directCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	ctx, span := c.startSpan(ctx)
	defer span.End()
	c.consumer.Consume(ctx, value)
}

func (c *directCaller[T]) IsAsync() bool {
	return c.async
}

type taskPoolCaller[T any] struct {
	caller[T]
	pool pool.TaskPool
}

func (c *taskPoolCaller[T]) startSpan(ctx context.Context) (context.Context, tracing.Span) {
	if !c.samplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return c.tracer.Start(ctx, "stream.call",
		tracing.StringAttr("from", c.fromName),
		tracing.StringAttr("to", c.toName),
		tracing.StringAttr("type", "taskpool"),
		tracing.StringAttr("taskpoolname", c.pool.GetName()),
	)
}

func (c *taskPoolCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	ctx, span := c.startSpan(ctx)
	if err := c.pool.AddTask(ctx, func() {
		defer span.End()
		c.consumer.Consume(ctx, value)
	}); err != nil {
		tracing.SpanError(span, err)
		span.End()
		c.source.GetEnvironment().Log().Warn(ctx, "task pool rejected task", log.Str("pool", c.pool.GetName()), log.Err(err))
	}
}

func (c *taskPoolCaller[T]) IsAsync() bool {
	return true
}

type priorityTaskPoolCaller[T any] struct {
	caller[T]
	pool     pool.PriorityTaskPool
	priority int
}

func (c *priorityTaskPoolCaller[T]) startSpan(ctx context.Context) (context.Context, tracing.Span) {
	if !c.samplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return c.tracer.Start(ctx, "stream.call",
		tracing.StringAttr("from", c.fromName),
		tracing.StringAttr("to", c.toName),
		tracing.StringAttr("type", "prioritytaskpool"),
		tracing.StringAttr("taskpoolname", c.pool.GetName()),
	)
}

func (c *priorityTaskPoolCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	ctx, span := c.startSpan(ctx)
	priority := c.priority
	if p, ok := PriorityFromContext(ctx); ok {
		priority = p
	}
	if err := c.pool.AddTask(ctx, priority, func() {
		defer span.End()
		c.consumer.Consume(ctx, value)
	}); err != nil {
		tracing.SpanError(span, err)
		span.End()
		c.source.GetEnvironment().Log().Warn(ctx, "priority task pool rejected task", log.Str("pool", c.pool.GetName()), log.Err(err))
	}
}

func (c *priorityTaskPoolCaller[T]) IsAsync() bool {
	return true
}

type parallelCaller[T any] struct {
	caller[T]
	runtime ServiceExecutionRuntime
}

func (c *parallelCaller[T]) startSpan(ctx context.Context) (context.Context, tracing.Span) {
	if !c.samplingEnabled(ctx) {
		return ctx, noopSpan{}
	}
	return c.tracer.Start(ctx, "stream.call",
		tracing.StringAttr("from", c.fromName),
		tracing.StringAttr("to", c.toName),
		tracing.StringAttr("type", "parallel"),
	)
}

func (c *parallelCaller[T]) Consume(ctx context.Context, value T) {
	c.statistics.Inc()
	if c.messagesCounter != nil {
		c.messagesCounter.Inc(ctx)
	}
	ctx, span := c.startSpan(ctx)
	c.runtime.beginParallel()
	go func() {
		defer c.runtime.endParallel()
		defer span.End()
		c.consumer.Consume(ctx, value)
	}()
}

func (c *parallelCaller[T]) IsAsync() bool {
	return true
}

type ServiceStream[T any] struct {
	environment        RuntimeEnvironment
	id                 int
	name               string
	transformationName string
	tracer             tracing.Tracer
}

func (s *ServiceStream[T]) GetTypeName() string {
	return reflect.TypeOf((*T)(nil)).Elem().String()
}

func (s *ServiceStream[T]) GetName() string {
	return s.name
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
	return s.transformationName
}

func MakeServiceStream[T any](id int, env RuntimeEnvironment) ServiceStream[T] {
	streamConfig := env.RuntimeConfig().GetStreamConfigByID(id)
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	return ServiceStream[T]{
		environment:        env,
		id:                 id,
		name:               streamConfig.GetName(),
		transformationName: config.GetTransformationName(streamConfig.GetType()),
		tracer:             tr,
	}
}

// TracingEnabled reports whether a span should be created for this context.
func (s *ServiceStream[T]) TracingEnabled(ctx context.Context) bool {
	return s.tracer != nil && tracing.SamplingEnabled(ctx)
}

// StartSpan starts a new span. Safe to call unconditionally; returns a no-op span
// when tracing is disabled or sampling not requested.
func (s *ServiceStream[T]) StartSpan(ctx context.Context, operation string) (context.Context, tracing.Span) {
	if s.tracer == nil || !tracing.SamplingEnabled(ctx) {
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

func (s *ConsumedStream[T]) SetDownstream(consumer TypedStreamConsumer[T], stream TypedStream[T]) error {
	if s.consumer != nil {
		return fmt.Errorf("consumer already assigned to the stream %s", s.ServiceStream.GetConfig().GetName())
	}
	caller, err := MakeCaller[T](stream, consumer)
	if err != nil {
		return err
	}
	s.consumer = consumer
	s.downstream = caller
	return nil
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

type priorityKeyType struct{}

var priorityKey = priorityKeyType{}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, priorityKey, priority)
}

func PriorityFromContext(ctx context.Context) (int, bool) {
	v := ctx.Value(priorityKey)
	if v == nil {
		return 0, false
	}
	p, ok := v.(int)
	return p, ok
}
