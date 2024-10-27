/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

type Caller[T any] interface {
	Consumer[T]
}

type ConsumeStatistics interface {
	Count() int64
	LinkId() config.LinkId
}

type ServiceLoader interface {
	Stop()
}

type ServiceExecutionRuntime interface {
	reloadConfig(config.Config)
	serviceInit(name string, env ServiceExecutionEnvironment, loader ServiceLoader, config config.Config) error
	getSerde(valueType reflect.Type) (serde.Serializer, error)
	registerStream(stream Stream)
	registerSerde(tp reflect.Type, serializer serde.StreamSerializer)
	getRegisteredSerde(tp reflect.Type) serde.StreamSerializer
	registerConsumeStatistics(statistics ConsumeStatistics)
	registerStorage(storage store.Storage)
	getTaskPool(name string) pool.TaskPool
	getPriorityTaskPool(name string) pool.PriorityTaskPool
}

func getPath(argPath string) (string, error) {
	var filePath string
	if !filepath.IsAbs(argPath) {
		dir, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("path error: %s", err.Error())
		}
		filePath = filepath.Join(dir, argPath)
	} else {
		filePath = argPath
	}
	return filePath, nil
}

func replacePlaceholders(config interface{}, values map[string]interface{}) interface{} {
	switch val := config.(type) {
	case string:
		if strings.HasPrefix(val, "$") {
			placeholder := val[1:]
			if val, ok := values[placeholder]; ok {
				return val
			}
		}
		return val
	case map[interface{}]interface{}:
		for key, value := range val {
			val[key] = replacePlaceholders(value, values)
		}
		return val
	case map[string]interface{}:
		for key, value := range val {
			val[key] = replacePlaceholders(value, values)
		}
		return val
	case []interface{}:
		for i, value := range val {
			val[i] = replacePlaceholders(value, values)
		}
		return val
	default:
		return val
	}
}

func getConfigData(cfg map[string]any) io.Reader {
	output, err := yaml.Marshal(cfg)
	if err != nil {
		log.Fatalf("Error marshaling config to YAML: %s", err)
	}
	return bytes.NewReader(output)
}

func getConfigMap(configData []byte, configValuesFile string) (map[string]any, error) {
	valuesData, err := os.ReadFile(configValuesFile)
	if err != nil {
		return nil, fmt.Errorf("error reading values file: %s", err)
	}

	var cfg map[string]any
	var values map[string]any

	if err = yaml.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshalling config YAML: %s", err)
	}
	if err = yaml.Unmarshal(valuesData, &values); err != nil {
		return nil, fmt.Errorf("error unmarshalling values YAML: %s", err)
	}

	replacePlaceholders(cfg, values)
	return cfg, nil
}

type serviceLoader[Environment ServiceExecutionEnvironment, Cfg config.Config] struct {
	watcher *fsnotify.Watcher
	wg      sync.WaitGroup
}

func (l *serviceLoader[Environment, Cfg]) Stop() {
	if err := l.watcher.Close(); err != nil {
		log.Warnf("watcher close error: %s", err)
	}
}

func (l *serviceLoader[Environment, Cfg]) init(service Environment, name string, configSettings *config.ConfigSettings) error {
	valuesPathArg := flag.String("values", "./values.yaml", "service config values path")
	configPathArg := flag.String("config", "./config.yaml", "service config path")
	flag.Parse()

	configFileName, err := getPath(*configPathArg)
	if err != nil {
		return fmt.Errorf("get config file path error: %s", err)
	}

	_, err = os.Stat(configFileName)
	if os.IsNotExist(err) {
		return fmt.Errorf("config file %q does not exist", configFileName)
	}

	valuesFileName, err := getPath(*valuesPathArg)
	if err != nil {
		return fmt.Errorf("get config values file path error: %s", err)
	}

	_, err = os.Stat(valuesFileName)
	if os.IsNotExist(err) {
		return fmt.Errorf("config values file %q does not exist", valuesFileName)
	}

	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	configFile := filepath.Clean(configFileName)

	valuesFile := filepath.Clean(valuesFileName)
	valuesDir, _ := filepath.Split(valuesFile)
	realValuesFile, _ := filepath.EvalSymlinks(valuesFileName)

	configData, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("error reading config file: %s", err)
	}

	l.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %s", err)
	}

	configType := serde.GetSerdeTypeWithoutPtr[Cfg]()

	err = func() error {
		if err := l.watcher.Add(valuesDir); err != nil {
			return fmt.Errorf("watcher add error: %s", err)
		}

		cfgMap, err := getConfigMap(configData, valuesFile)
		if err != nil {
			return fmt.Errorf("get config map error: %s", err)
		}

		err = viper.ReadConfig(getConfigData(cfgMap))
		if err != nil {
			return fmt.Errorf("viper read config error: %s\n", err)
		}

		cfg := reflect.New(configType).Interface().(Cfg)

		if err := viper.Unmarshal(cfg); err != nil {
			return fmt.Errorf("unmarshal config error: %s", err)
		}

		cfg.GetAppConfig().InitRuntimeConfig()
		return service.GetRuntime().serviceInit(name, service, l, cfg)
	}()

	if err != nil {
		if err := l.watcher.Close(); err != nil {
			log.Errorf("watcher close error: %s", err)
		}
		return err
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		for {
			select {
			case event, ok := <-l.watcher.Events:
				if !ok {
					return
				}
				currentValuesFile, _ := filepath.EvalSymlinks(valuesFileName)
				// we only care about the config file with the following cases:
				// 1 - if the config file was modified or created
				// 2 - if the real path to the config file changed (eg: k8s ConfigMap replacement)
				if (filepath.Clean(event.Name) == valuesFile &&
					(event.Has(fsnotify.Write) || event.Has(fsnotify.Create))) ||
					(currentValuesFile != "" && currentValuesFile != realValuesFile) {
					realValuesFile = currentValuesFile

					if cfgMap, err := getConfigMap(configData, realValuesFile); err != nil {
						log.Errorf("Reload config map error: %s", err)
					} else {
						if err := viper.MergeConfigMap(cfgMap); err != nil {
							log.Errorf("Viper merge config error: %s", err)
						} else {
							cfg := reflect.New(configType).Interface().(Cfg)
							if err := viper.Unmarshal(cfg); err != nil {
								log.Errorf("Viper unmarshal config error: %s", err)
							} else {
								cfg.GetAppConfig().InitRuntimeConfig()
								service.GetRuntime().reloadConfig(cfg)
							}
						}
					}
				} else if event.Has(fsnotify.Remove) &&
					filepath.Clean(event.Name) == valuesFile {
					return
				}

			case err, ok := <-l.watcher.Errors:
				if ok {
					log.Errorf("watcher error: %s", err)
				}
				return
			}
		}
	}()
	return nil
}

func MakeService[Environment ServiceExecutionEnvironment, Cfg config.Config](name string, configSettings *config.ConfigSettings) Environment {
	serviceType := serde.GetSerdeTypeWithoutPtr[Environment]()
	service := reflect.New(serviceType).Interface().(Environment)

	loader := &serviceLoader[Environment, Cfg]{}
	if err := loader.init(service, name, configSettings); err != nil {
		log.Fatalf("make service %q error: %s", name, err)
	}
	return service
}

func makeSerdeForType(tp reflect.Type, runtime ServiceExecutionRuntime) (serde.Serializer, error) {
	var err error
	var ser serde.Serializer

	ser, err = runtime.getSerde(tp)
	if err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = makeSerdeForType(tp.Elem(), runtime)
			if err != nil {
				return nil, err
			}
			return serde.MakeArraySerde(tp, ser), nil
		} else if tp.Kind() == reflect.Map {
			serKeyArray, err := makeSerdeForType(reflect.SliceOf(tp.Key()), runtime)
			if err != nil {
				return nil, err
			}
			serValueArray, err := makeSerdeForType(reflect.SliceOf(tp.Elem()), runtime)
			if err != nil {
				return nil, err
			}
			return serde.MakeMapSerde(tp, serKeyArray, serValueArray), nil
		}
	}
	return ser, err
}

func makeTypedArraySerde[T any](runtime ServiceExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	elementType := v.Type().Elem()
	serElm, err := makeSerdeForType(elementType, runtime)
	if err != nil {
		return nil, err
	}
	return serde.MakeTypedArraySerde[T](serElm), nil
}

func makeTypedMapSerde[T any](runtime ServiceExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	mapType := v.Type()
	keyType := mapType.Key()
	keyArraySerde, err := makeSerdeForType(reflect.SliceOf(keyType), runtime)
	if err != nil {
		return nil, err
	}
	valueType := mapType.Elem()
	valueArraySerde, err := makeSerdeForType(reflect.SliceOf(valueType), runtime)
	if err != nil {
		return nil, err
	}
	return serde.MakeTypedMapSerde[T](keyArraySerde, valueArraySerde), nil
}

func registerSerde[T any](runtime ServiceExecutionRuntime, ser serde.StreamSerde[T]) {
	runtime.registerSerde(serde.GetSerdeType[T](), ser)
}

func getRegisteredSerde[T any](runtime ServiceExecutionRuntime) serde.StreamSerde[T] {
	if ser := runtime.getRegisteredSerde(serde.GetSerdeType[T]()); ser != nil {
		return ser.(serde.StreamSerde[T])
	}
	return nil
}

func MakeSerde[T any](runtime ServiceExecutionRuntime) serde.StreamSerde[T] {
	if ser := getRegisteredSerde[T](runtime); ser != nil {
		return ser
	}
	tp := serde.GetSerdeType[T]()

	var err error
	var ser serde.Serializer

	if ser, err = runtime.getSerde(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = makeTypedArraySerde[T](runtime)
		} else if tp.Kind() == reflect.Map {
			ser, err = makeTypedMapSerde[T](runtime)
		}
	}
	if ser == nil || err != nil {
		ser = serde.MakeStubSerde[T]()
	}
	serT, ok := ser.(serde.Serde[T])
	if !ok {
		log.Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}
	streamSer := serde.MakeStreamSerde(serT)
	registerSerde[T](runtime, streamSer)
	return streamSer
}

func MakeKeyValueSerde[K comparable, V any](runtime ServiceExecutionRuntime) serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]] {
	if ser := getRegisteredSerde[datastruct.KeyValue[K, V]](runtime); ser != nil {
		return ser.(serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]])
	}
	tp := serde.GetSerdeType[K]()

	var err error
	var ser serde.Serializer

	if ser, err = runtime.getSerde(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = makeTypedArraySerde[K](runtime)
		} else if tp.Kind() == reflect.Map {
			ser, err = makeTypedMapSerde[K](runtime)
		}
	}
	if err != nil {
		ser = serde.MakeStubSerde[K]()
	}
	serdeK, ok := ser.(serde.Serde[K])
	if !ok {
		log.Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}

	tp = serde.GetSerdeType[V]()
	if ser, err = runtime.getSerde(tp); err != nil {
		if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
			ser, err = makeTypedArraySerde[V](runtime)
		} else if tp.Kind() == reflect.Map {
			ser, err = makeTypedMapSerde[V](runtime)
		}
	}
	if ser == nil || err != nil {
		ser = serde.MakeStubSerde[V]()
	}
	serdeV, ok := ser.(serde.Serde[V])
	if !ok {
		log.Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}
	streamSer := serde.MakeStreamKeyValueSerde[K, V](serdeK, serdeV)
	registerSerde[datastruct.KeyValue[K, V]](runtime, streamSer)
	return streamSer
}

var keyValuePattern = regexp.MustCompile(`^KeyValue\[\w+,\w+]$`)

func IsKeyValueType[T any]() bool {
	tp := serde.GetSerdeType[T]()
	return tp.PkgPath() == "github.com/gorundebug/servicelib/runtime/datastruct" && keyValuePattern.MatchString(tp.Name())
}

func makeCaller[T any](env ServiceExecutionEnvironment, source TypedStream[T]) Caller[T] {
	runtime := env.GetRuntime()
	cfg := env.GetAppConfig()
	serviceConfig := env.GetServiceConfig()
	consumer := source.GetConsumer()
	link := cfg.GetLink(source.GetId(), consumer.GetId())
	if link == nil {
		log.Fatalf("No link found between streams from=%d to=%d", source.GetId(), consumer.GetId())
		return nil
	}
	streamFrom := cfg.GetStreamConfigById(link.From)
	var callSemantics api.CallSemantics
	if streamFrom.IdService == serviceConfig.Id {
		callSemantics = link.CallSemantics
	} else {
		callSemantics = *link.IncomeCallSemantics
	}
	var streamCaller Caller[T]
	var consumeStat ConsumeStatistics
	switch callSemantics {
	case api.FunctionCall:
		c := &directCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
		}
		consumeStat = c
		streamCaller = c

	case api.TaskPool:
		var taskPool pool.TaskPool
		if streamFrom.IdService == serviceConfig.Id {
			taskPool = runtime.getTaskPool(*link.PoolName)
		} else {
			taskPool = runtime.getTaskPool(*link.IncomePoolName)
		}
		c := &taskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
			pool: taskPool,
		}
		consumeStat = c
		streamCaller = c

	case api.PriorityTaskPool:
		var priorityTaskPool pool.PriorityTaskPool
		var priority int
		if streamFrom.IdService == serviceConfig.Id {
			priorityTaskPool = runtime.getPriorityTaskPool(*link.PoolName)
			priority = *link.Priority
		} else {
			priorityTaskPool = runtime.getPriorityTaskPool(*link.IncomePoolName)
			priority = *link.IncomePriority
		}
		c := &priorityTaskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
			pool:     priorityTaskPool,
			priority: priority,
		}
		consumeStat = c
		streamCaller = c

	default:
		log.Fatalf("undefined callSemantics [%d] ", callSemantics)
	}

	runtime.registerConsumeStatistics(consumeStat)
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

type caller[T any] struct {
	consumeStatistics
	runtime  ServiceExecutionRuntime
	source   TypedStream[T]
	consumer TypedStreamConsumer[T]
}

func (c *caller[T]) LinkId() config.LinkId {
	return config.LinkId{From: c.source.GetId(), To: c.consumer.GetId()}
}

type directCaller[T any] struct {
	caller[T]
}

func (c *directCaller[T]) Consume(value T) {
	c.Inc()
	c.consumer.Consume(value)
}

type taskPoolCaller[T any] struct {
	caller[T]
	pool pool.TaskPool
}

func (c *taskPoolCaller[T]) Consume(value T) {
	c.Inc()
	c.pool.AddTask(func() {
		c.consumer.Consume(value)
	})
}

type priorityTaskPoolCaller[T any] struct {
	caller[T]
	pool     pool.PriorityTaskPool
	priority int
}

func (c *priorityTaskPoolCaller[T]) Consume(value T) {
	c.Inc()
	c.pool.AddTask(c.priority, func() {
		c.consumer.Consume(value)
	})
}

type StreamBase[T any] struct {
	environment ServiceExecutionEnvironment
	id          int
}

func (s *StreamBase[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *StreamBase[T]) GetName() string {
	return s.GetConfig().Name
}

func (s *StreamBase[T]) GetId() int {
	return s.id
}

func (s *StreamBase[T]) GetConfig() *config.StreamConfig {
	return s.environment.GetAppConfig().GetStreamConfigById(s.id)
}

func (s *StreamBase[T]) GetEnvironment() ServiceExecutionEnvironment {
	return s.environment
}

func (s *StreamBase[T]) GetTransformationName() string {
	return s.GetConfig().GetTransformationName()
}

type ConsumedStream[T any] struct {
	StreamBase[T]
	caller   Caller[T]
	serde    serde.StreamSerde[T]
	consumer TypedStreamConsumer[T]
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

func (s *ConsumedStream[T]) SetConsumer(consumer TypedStreamConsumer[T]) {
	if s.consumer != nil {
		log.Fatalf("consumer already assigned to the stream %s", s.StreamBase.GetConfig().Name)
	}
	s.consumer = consumer
	s.caller = makeCaller[T](s.environment, s)
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}
