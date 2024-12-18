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
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/store"
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
}

type ServiceLoader interface {
	Stop()
}

type ServiceExecutionRuntime interface {
	reloadConfig(config.Config)
	serviceInit(name string, env ServiceExecutionEnvironment,
		dep environment.ServiceDependency,
		loader ServiceLoader,
		config config.Config) error
	getSerde(valueType reflect.Type) (serde.Serializer, error)
	registerStream(stream Stream)
	registerSerde(tp reflect.Type, serializer serde.StreamSerializer)
	getRegisteredSerde(tp reflect.Type) serde.StreamSerializer
	registerConsumeStatistics(linkId config.LinkId, statistics ConsumeStatistics)
	registerStorage(storage store.Storage)
	getLog() log.Logger
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

func getConfigData(cfg map[string]any) (io.Reader, error) {
	output, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("error marshaling config to YAML: %s", err)
	}
	return bytes.NewReader(output), nil
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
	service Environment
}

func (l *serviceLoader[Environment, Cfg]) Stop() {
	if err := l.watcher.Close(); err != nil {
		l.service.Log().Warnf("watcher close error: %s", err)
	}
}

func (l *serviceLoader[Environment, Cfg]) init(name string,
	dep environment.ServiceDependency,
	configSettings *config.ConfigSettings) error {

	serviceType := serde.GetSerdeTypeWithoutPtr[Environment]()
	l.service = reflect.New(serviceType).Interface().(Environment)

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

		reader, err := getConfigData(cfgMap)
		if err != nil {
			return fmt.Errorf("get config data error: %s", err)
		}

		err = viper.ReadConfig(reader)
		if err != nil {
			return fmt.Errorf("viper read config error: %s\n", err)
		}

		cfg := reflect.New(configType).Interface().(Cfg)

		if err := viper.Unmarshal(cfg); err != nil {
			return fmt.Errorf("unmarshal config error: %s", err)
		}

		cfg.AppConfig().InitRuntimeConfig()
		return l.service.GetRuntime().serviceInit(name, l.service, dep, l, cfg)
	}()

	if err != nil {
		_ = l.watcher.Close()
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
						l.service.Log().Errorf("Reload config map error: %s", err)
					} else {
						if err := viper.MergeConfigMap(cfgMap); err != nil {
							l.service.Log().Errorf("Viper merge config error: %s", err)
						} else {
							cfg := reflect.New(configType).Interface().(Cfg)
							if err := viper.Unmarshal(cfg); err != nil {
								l.service.Log().Errorf("Viper unmarshal config error: %s", err)
							} else {
								cfg.AppConfig().InitRuntimeConfig()
								l.service.GetRuntime().reloadConfig(cfg)
							}
						}
					}
				} else if event.Has(fsnotify.Remove) &&
					filepath.Clean(event.Name) == valuesFile {
					return
				}

			case err, ok := <-l.watcher.Errors:
				if ok {
					l.service.Log().Errorf("watcher error: %s", err)
				}
				return
			}
		}
	}()

	return nil
}

func MakeService[Environment ServiceExecutionEnvironment, Cfg config.Config](name string,
	dep environment.ServiceDependency,
	configSettings *config.ConfigSettings) (Environment, error) {
	loader := &serviceLoader[Environment, Cfg]{}
	if err := loader.init(name, dep, configSettings); err != nil {
		var env Environment
		return env, fmt.Errorf("make service %q error: %s", name, err)
	}
	return loader.service, nil
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
	arrSer, err := serde.MakeTypedArraySerde[T](serElm)
	if err != nil {
		runtime.getLog().Fatalln(err)
	}
	return arrSer, err
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
	mapSer, err := serde.MakeTypedMapSerde[T](keyArraySerde, valueArraySerde)
	if err != nil {
		runtime.getLog().Fatalln(err)
	}
	return mapSer, nil
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
		runtime.getLog().Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
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
		runtime.getLog().Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
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
		runtime.getLog().Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
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

func makeCaller[T any](source TypedStream[T]) Caller[T] {
	env := source.GetEnvironment()
	runtime := env.GetRuntime()
	cfg := env.AppConfig()
	serviceConfig := env.ServiceConfig()
	consumer := source.GetConsumer()
	link := cfg.GetLink(source.GetId(), consumer.GetId())
	if link == nil {
		env.Log().Fatalf("No link found between streams from=%d to=%d", source.GetId(), consumer.GetId())
		return nil
	}
	streamFrom := source.GetConfig()
	var callSemantics api.CallSemantics
	if streamFrom.IdService == serviceConfig.Id {
		callSemantics = link.CallSemantics
	} else {
		callSemantics = *link.IncomeCallSemantics
	}
	var streamCaller Caller[T]
	consumeStat := &consumeStatistics{}
	switch callSemantics {
	case api.FunctionCall:
		c := &directCaller[T]{
			caller: caller[T]{
				source:     source,
				consumer:   consumer,
				statistics: consumeStat,
			},
		}
		streamCaller = c

	case api.TaskPool:
		var taskPool pool.TaskPool
		if streamFrom.IdService == serviceConfig.Id {
			taskPool = env.GetTaskPool(*link.PoolName)
		} else {
			taskPool = env.GetTaskPool(*link.IncomePoolName)
		}
		c := &taskPoolCaller[T]{
			caller: caller[T]{
				source:     source,
				consumer:   consumer,
				statistics: consumeStat,
			},
			pool: taskPool,
		}
		streamCaller = c

	case api.PriorityTaskPool:
		var priorityTaskPool pool.PriorityTaskPool
		var priority int
		if streamFrom.IdService == serviceConfig.Id {
			priorityTaskPool = env.GetPriorityTaskPool(*link.PoolName)
			priority = *link.Priority
		} else {
			priorityTaskPool = env.GetPriorityTaskPool(*link.IncomePoolName)
			priority = *link.IncomePriority
		}
		c := &priorityTaskPoolCaller[T]{
			caller: caller[T]{
				source:     source,
				consumer:   consumer,
				statistics: consumeStat,
			},
			pool:     priorityTaskPool,
			priority: priority,
		}
		streamCaller = c

	default:
		env.Log().Fatalf("undefined callSemantics [%d] ", callSemantics)
	}

	runtime.registerConsumeStatistics(config.LinkId{From: source.GetId(), To: consumer.GetId()}, consumeStat)
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
	statistics *consumeStatistics
	source     TypedStream[T]
	consumer   TypedStreamConsumer[T]
}

type directCaller[T any] struct {
	caller[T]
}

func (c *directCaller[T]) Consume(value T) {
	c.statistics.Inc()
	c.consumer.Consume(value)
}

type taskPoolCaller[T any] struct {
	caller[T]
	pool pool.TaskPool
}

func (c *taskPoolCaller[T]) Consume(value T) {
	c.statistics.Inc()
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
	c.statistics.Inc()
	c.pool.AddTask(c.priority, func() {
		c.consumer.Consume(value)
	})
}

type ServiceStream[T any] struct {
	environment ServiceExecutionEnvironment
	id          int
}

func (s *ServiceStream[T]) GetTypeName() string {
	var t T
	return reflect.TypeOf(t).String()
}

func (s *ServiceStream[T]) GetName() string {
	return s.GetConfig().Name
}

func (s *ServiceStream[T]) GetId() int {
	return s.id
}

func (s *ServiceStream[T]) GetConfig() *config.StreamConfig {
	return s.environment.AppConfig().GetStreamConfigById(s.id)
}

func (s *ServiceStream[T]) GetEnvironment() ServiceExecutionEnvironment {
	return s.environment
}

func (s *ServiceStream[T]) GetTransformationName() string {
	return s.GetConfig().GetTransformationName()
}

func (s *ServiceStream[T]) Validate() error {
	return nil
}

type ConsumedStream[T any] struct {
	ServiceStream[T]
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
		consumer.GetEnvironment().Log().Fatalf("consumer already assigned to the stream %s", s.ServiceStream.GetConfig().Name)
	}
	s.consumer = consumer
	s.caller = makeCaller[T](s)
}

func (s *ConsumedStream[T]) GetConsumer() TypedStreamConsumer[T] {
	return s.consumer
}

type StreamFunction[T any] struct {
	context ServiceStream[T] //nolint:unused
}

func (f *StreamFunction[T]) BeforeCall() {
}

func (f *StreamFunction[T]) AfterCall() {
}
