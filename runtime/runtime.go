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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gitlab.com/gorundebug/servicelib/api"
	"gitlab.com/gorundebug/servicelib/runtime/config"
	"gitlab.com/gorundebug/servicelib/runtime/datastruct"
	"gitlab.com/gorundebug/servicelib/runtime/serde"
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
)

type Caller[T any] interface {
	Consume(value T)
}

type ConsumeStatistics interface {
	Count() int64
	LinkId() config.LinkId
}

type StreamExecutionRuntime interface {
	StreamExecutionEnvironment
	reloadConfig(config.Config)
	serviceInit(name string, runtime StreamExecutionRuntime, config config.Config)
	getSerde(valueType reflect.Type) (serde.Serializer, error)
	registerStream(stream StreamBase)
	registerSerde(tp reflect.Type, serializer serde.StreamSerializer)
	getRegisteredSerde(tp reflect.Type) serde.StreamSerializer
	registerConsumeStatistics(statistics ConsumeStatistics)
}

func getPath(argPath *string) string {
	var filePath string
	if !filepath.IsAbs(*argPath) {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatalf("path error: %s", err.Error())
		}
		filePath = filepath.Join(dir, *argPath)
	} else {
		filePath = *argPath
	}
	return filePath
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

func getConfigData(configPathArg *string, configValuesPathArg *string) io.Reader {
	configFile := getPath(configPathArg)
	configValuesFile := getPath(configValuesPathArg)

	configData, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	valuesData, err := os.ReadFile(configValuesFile)
	if err != nil {
		log.Fatalf("Error reading values file: %s", err)
	}

	var config map[string]interface{}
	var values map[string]interface{}

	if err := yaml.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Error unmarshalling config YAML: %s", err)
	}
	if err := yaml.Unmarshal(valuesData, &values); err != nil {
		log.Fatalf("Error unmarshalling values YAML: %s", err)
	}

	replacePlaceholders(config, values)

	output, err := yaml.Marshal(config)
	if err != nil {
		log.Fatalf("Error marshaling config to YAML: %s", err)
	}
	return bytes.NewReader(output)
}

func MakeService[Runtime StreamExecutionRuntime, Cfg config.Config](name string, configSettings *config.ConfigSettings) Runtime {
	configValuesPathArg := flag.String("values", "./values.yaml", "service config values path")
	configPathArg := flag.String("config", "./config.yaml", "service config path")
	flag.Parse()

	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	if err := viper.ReadConfig(getConfigData(configPathArg, configValuesPathArg)); err != nil {
		fmt.Println("fatal error config file:\n", err)
		os.Exit(1)
	}

	configType := serde.GetSerdeType[Cfg]()
	cfg := reflect.New(configType).Interface().(Cfg)

	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatalf("fatal error config file: %s", err)
	}
	appType := serde.GetSerdeType[Runtime]()
	runtime := reflect.New(appType).Interface().(Runtime)

	viper.OnConfigChange(func(e fsnotify.Event) {
		configType := serde.GetSerdeType[Cfg]()
		cfg := reflect.New(configType).Interface().(Cfg)
		if err := viper.Unmarshal(cfg); err != nil {
			log.Println("error config update:\n", err)
		} else {
			runtime.reloadConfig(cfg)
		}
	})
	viper.WatchConfig()
	runtime.serviceInit(name, runtime, cfg)
	return runtime
}

func makeSerdeForType(tp reflect.Type, runtime StreamExecutionRuntime) (serde.Serializer, error) {
	var err error
	var ser serde.Serializer
	if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
		ser, err = makeSerdeForType(tp.Elem(), runtime)
		if err != nil {
			return nil, err
		}
		return serde.MakeArraySerde(tp, ser), nil
	} else if tp.Kind() == reflect.Map {
		serKey, err := makeSerdeForType(tp.Key(), runtime)
		if err != nil {
			return nil, err
		}
		serValue, err := makeSerdeForType(tp.Elem(), runtime)
		if err != nil {
			return nil, err
		}
		return serde.MakeMapSerde(tp, serKey, serValue), nil
	} else {
		ser, err = runtime.getSerde(tp)
	}
	return ser, err
}

func makeTypedArraySerde[T any](runtime StreamExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	elementType := v.Type().Elem()
	for {
		if elementType.Kind() == reflect.Ptr {
			elementType = elementType.Elem()
		} else {
			break
		}
	}
	serElm, err := makeSerdeForType(elementType, runtime)
	if err != nil {
		return nil, err
	}
	return serde.MakeTypedArraySerde[T](serElm), nil
}

func makeTypedMapSerde[T any](runtime StreamExecutionRuntime) (serde.Serializer, error) {
	var t T
	v := reflect.ValueOf(t)
	mapType := v.Type()
	keyType := mapType.Key()
	for {
		if keyType.Kind() == reflect.Ptr {
			keyType = keyType.Elem()
		} else {
			break
		}
	}
	serKey, err := makeSerdeForType(keyType, runtime)
	if err != nil {
		return nil, err
	}
	valueType := mapType.Elem()
	for {
		if valueType.Kind() == reflect.Ptr {
			valueType = valueType.Elem()
		} else {
			break
		}
	}
	serValue, err := makeSerdeForType(valueType, runtime)
	if err != nil {
		return nil, err
	}
	return serde.MakeTypedMapSerde[T](serKey, serValue), nil
}

func registerSerde[T any](runtime StreamExecutionRuntime, ser serde.StreamSerde[T]) {
	runtime.registerSerde(serde.GetSerdeType[T](), ser)
}

func getRegisteredSerde[T any](runtime StreamExecutionRuntime) serde.StreamSerde[T] {
	if ser := runtime.getRegisteredSerde(serde.GetSerdeType[T]()); ser != nil {
		return ser.(serde.StreamSerde[T])
	}
	return nil
}

func MakeSerde[T any](runtime StreamExecutionRuntime) serde.StreamSerde[T] {
	if ser := getRegisteredSerde[T](runtime); ser != nil {
		return ser
	}
	tp := serde.GetSerdeType[T]()
	var err error
	var ser serde.Serializer
	if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
		ser, err = makeTypedArraySerde[T](runtime)
	} else if tp.Kind() == reflect.Map {
		ser, err = makeTypedMapSerde[T](runtime)
	} else {
		ser, err = makeSerdeForType(tp, runtime)
	}
	if err != nil {
		log.Fatalln(err)
	}
	serT, ok := ser.(serde.Serde[T])
	if !ok {
		log.Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}
	streamSer := serde.MakeStreamSerde(serT)
	registerSerde[T](runtime, streamSer)
	return streamSer
}

func MakeKeyValueSerde[K comparable, V any](runtime StreamExecutionRuntime) serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]] {
	if ser := getRegisteredSerde[datastruct.KeyValue[K, V]](runtime); ser != nil {
		return ser.(serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]])
	}
	tp := serde.GetSerdeType[K]()
	ser, err := runtime.getSerde(tp)
	if err != nil {
		log.Fatalf("Serializer for type %s not found. %s", tp.Name(), err)
	}
	serdeK, ok := ser.(serde.Serde[K])
	if !ok {
		log.Fatalf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}

	tp = serde.GetSerdeType[V]()
	ser, err = runtime.getSerde(tp)
	if err != nil {
		log.Fatalf("Serializer for type %s not found. %s", tp.Name(), err)
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
	return tp.PkgPath() == "gitlab.com/gorundebug/servicelib/runtime/datastruct" && keyValuePattern.MatchString(tp.Name())
}

func RegisterSerde[T any](runtime StreamExecutionRuntime) serde.StreamSerde[T] {
	return MakeSerde[T](runtime)
}

func RegisterKeyValueSerde[K comparable, V any](runtime StreamExecutionRuntime) serde.StreamKeyValueSerde[datastruct.KeyValue[K, V]] {
	return MakeKeyValueSerde[K, V](runtime)
}

func makeCaller[T any](runtime StreamExecutionRuntime, source TypedStream[T]) Caller[T] {
	consumer := source.GetConsumer()
	communicationType := api.FunctionCall
	link := runtime.GetConfig().GetLink(source.GetId(), consumer.GetId())
	if link != nil {
		communicationType = link.CallSemantics
	}
	var streamCaller Caller[T]
	var consumeStat ConsumeStatistics
	switch communicationType {
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
		c := &taskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
		}
		consumeStat = c
		streamCaller = c

	case api.PriorityTaskPool:
		c := &priorityTaskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
		}
		consumeStat = c
		streamCaller = c

	default:
		log.Fatalf("undefined CommunicationType [%d] ", communicationType)
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
	runtime  StreamExecutionRuntime
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
}

func (c *taskPoolCaller[T]) Consume(value T) {
	c.Inc()
}

type priorityTaskPoolCaller[T any] struct {
	caller[T]
}

func (c *priorityTaskPoolCaller[T]) Consume(value T) {
	c.Inc()
}
