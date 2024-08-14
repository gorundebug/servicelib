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
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

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

func MakeService[Runtime StreamExecutionRuntime, Cfg Config](name string, configSettings *ConfigSettings) Runtime {
	configValuesPathArg := flag.String("values", "./values.yaml", "service config values path")
	configPathArg := flag.String("config", "./config.yaml", "service config path")
	flag.Parse()

	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	if err := viper.ReadConfig(getConfigData(configPathArg, configValuesPathArg)); err != nil {
		fmt.Println("fatal error config file:\n", err)
		os.Exit(1)
	}

	configType := GetSerdeType[Cfg]()
	cfg := reflect.New(configType).Interface().(Cfg)

	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatalf("fatal error config file: %s", err)
	}
	appType := GetSerdeType[Runtime]()
	runtime := reflect.New(appType).Interface().(Runtime)

	viper.OnConfigChange(func(e fsnotify.Event) {
		configType := GetSerdeType[Cfg]()
		cfg := reflect.New(configType).Interface().(Cfg)
		if err := viper.Unmarshal(cfg); err != nil {
			log.Println("error config update:\n", err)
		} else {
			runtime.configReload(cfg)
		}
	})
	viper.WatchConfig()
	runtime.streamsInit(name, runtime, cfg)
	return runtime
}

func makeSerdeForType(tp reflect.Type, runtime StreamExecutionRuntime) (Serializer, error) {
	var err error
	var ser Serializer
	if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
		ser, err = makeSerdeForType(tp.Elem(), runtime)
		if err != nil {
			return nil, err
		}
		return makeArraySerde(tp, ser), nil
	} else if tp.Kind() == reflect.Map {
		serKey, err := makeSerdeForType(tp.Key(), runtime)
		if err != nil {
			return nil, err
		}
		serValue, err := makeSerdeForType(tp.Elem(), runtime)
		if err != nil {
			return nil, err
		}
		return makeMapSerde(tp, serKey, serValue), nil
	} else {
		ser, err = runtime.getSerde(tp)
	}
	return ser, err
}

func makeTypedArraySerde[T any](runtime StreamExecutionRuntime) (Serializer, error) {
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
	return MakeArraySerde[T](serElm), nil
}

func makeTypedMapSerde[T any](runtime StreamExecutionRuntime) (Serializer, error) {
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
	return MakeMapSerde[T](serKey, serValue), nil
}

func registerSerde[T any](runtime StreamExecutionRuntime, serde StreamSerde[T]) {
	runtime.registerSerde(GetSerdeType[T](), serde)
}

func getRegisteredSerde[T any](runtime StreamExecutionRuntime) StreamSerde[T] {
	if serde := runtime.getRegisteredSerde(GetSerdeType[T]()); serde != nil {
		return serde.(StreamSerde[T])
	}
	return nil
}

func makeSerde[T any](runtime StreamExecutionRuntime) StreamSerde[T] {
	if serde := getRegisteredSerde[T](runtime); serde != nil {
		return serde
	}
	tp := GetSerdeType[T]()
	var err error
	var ser Serializer
	if tp.Kind() == reflect.Array || tp.Kind() == reflect.Slice {
		ser, err = makeTypedArraySerde[T](runtime)
	} else if tp.Kind() == reflect.Map {
		ser, err = makeTypedMapSerde[T](runtime)
	} else {
		ser, err = makeSerdeForType(tp, runtime)
	}
	if err != nil {
		log.Panicln(err)
	}
	serT, ok := ser.(Serde[T])
	if !ok {
		log.Panicf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}
	serde := makeStreamSerde(serT)
	registerSerde[T](runtime, serde)
	return serde
}

func makeKeyValueSerde[K comparable, V any](runtime StreamExecutionRuntime) StreamKeyValueSerde[KeyValue[K, V]] {
	if serde := getRegisteredSerde[KeyValue[K, V]](runtime); serde != nil {
		return serde.(StreamKeyValueSerde[KeyValue[K, V]])
	}
	tp := GetSerdeType[K]()
	ser, err := runtime.getSerde(tp)
	if err != nil {
		log.Panicf("Serializer for type %s not found. %s", tp.Name(), err)
	}
	serdeK, ok := ser.(Serde[K])
	if !ok {
		log.Panicf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}

	tp = GetSerdeType[V]()
	ser, err = runtime.getSerde(tp)
	if err != nil {
		log.Panicf("Serializer for type %s not found. %s", tp.Name(), err)
	}
	serdeV, ok := ser.(Serde[V])
	if !ok {
		log.Panicf("Invalid type conversion from SerdeType to Serde[%s] ", tp.Name())
	}
	serde := makeStreamKeyValueSerde[K, V](serdeK, serdeV)
	registerSerde[KeyValue[K, V]](runtime, serde)
	return serde
}

func makeCaller[T any](runtime StreamExecutionRuntime,
	source TypedStream[T],
	serde StreamSerde[T]) Caller[T] {
	consumer := source.GetConsumer()
	communicationType := runtime.GetConfig().GetCallSemantics(source.GetId(), consumer.GetId())
	switch communicationType {

	case api.FunctionCall:
		return &directCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
			serde: serde,
		}

	case api.TaskPool:
		return &taskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
			serde: serde,
		}

	case api.PriorityTaskPool:
		return &priorityTaskPoolCaller[T]{
			caller: caller[T]{
				runtime:  runtime,
				source:   source,
				consumer: consumer,
			},
			serde: serde,
		}
	}

	log.Panicf("undefined CommunicationType [%d] ", communicationType)
	return nil
}

type caller[T any] struct {
	runtime  StreamExecutionRuntime
	source   TypedStream[T]
	consumer TypedStreamConsumer[T]
}

type directCaller[T any] struct {
	caller[T]
	serde StreamSerde[T]
}

func (c *directCaller[T]) Consume(value T) {
	c.consumer.Consume(value)
}

type taskPoolCaller[T any] struct {
	caller[T]
	serde StreamSerde[T]
}

func (c *taskPoolCaller[T]) Consume(value T) {
}

type priorityTaskPoolCaller[T any] struct {
	caller[T]
	serde StreamSerde[T]
}

func (c *priorityTaskPoolCaller[T]) Consume(value T) {
	if c.serde.IsKeyValue() {
		serdeKV := c.serde.(StreamKeyValueSerde[T])
		_, _ = serdeKV.SerializeKey(value)
	} else {
		_, _ = c.serde.Serialize(value)
	}
}
