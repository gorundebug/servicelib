/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
	"github.com/gorundebug/servicelib/api"
	cfg "github.com/gorundebug/servicelib/runtime/config"
)

const (
	operatorServiceId = iota + 1
)

const (
	// Map pipeline
	inputMapId = iota + 1
	mapStreamId
	sinkMapId

	// Filter pipeline
	inputFilterId
	filterStreamId
	sinkFilterId

	// FlatMap pipeline
	inputFlatMapId
	flatMapStreamId
	sinkFlatMapId

	// FlatMapIterable pipeline
	inputFlatMapIterableId
	flatMapIterableStreamId
	sinkFlatMapIterableId

	// Process pipeline
	inputProcessId
	processStreamId
	sinkProcessId
	sinkProcessErrorId

	// Split pipeline
	inputSplitId
	splitStreamId
	sinkSplit1Id
	sinkSplit2Id

	// Merge pipeline
	inputMerge1Id
	inputMerge2Id
	mergeStreamId
	sinkMergeId

	// Case pipeline
	inputCaseId
	caseStreamId
	whenAId
	whenBId
	sinkCaseAId
	sinkCaseBId

	// Delay pipeline
	inputDelayId
	delayStreamId
	sinkDelayId

	// KeyBy pipeline
	inputKeyById
	keyByStreamId
	sinkKeyById

	// Join pipeline
	inputJoinLeftId
	keyByJoinLeftId
	inputJoinRightId
	keyByJoinRightId
	joinStreamId
	sinkJoinId

	// Left join pipeline
	inputJoinLLeftId
	keyByJoinLLeftId
	inputJoinRLeftId
	keyByJoinRLeftId
	joinStreamLeftId
	sinkJoinLeftId

	// Right join pipeline
	inputJoinLRightId
	keyByJoinLRightId
	inputJoinRRightId
	keyByJoinRRightId
	joinStreamRightId
	sinkJoinRightId

	// Outer join pipeline
	inputJoinLOuterId
	keyByJoinLOuterId
	inputJoinROuterId
	keyByJoinROuterId
	joinStreamOuterId
	sinkJoinOuterId

	// LocalSource pipeline (no-result path)
	inputLocalSourceId
	sinkLocalSourceId

	// LocalSourceResult pipeline (hasResult=true: result flows back via callback)
	inputLocalSourceResultId
	mapLocalSourceResultId

	// HttpSink pipeline (tests datasink/http)
	inputHttpSinkId
	sinkHttpSinkId

	// MultiJoin pipeline
	inputMultiJoin1Id
	keyByMultiJoin1Id
	inputMultiJoin2Id
	keyByMultiJoin2Id
	inputMultiJoin3Id
	keyByMultiJoin3Id
	multiJoinStreamId
	sinkMultiJoinId

	// HttpResult pipeline (HTTP source hasResult=true path)
	inputHttpResultId
	mapHttpResultId
)

const (
	httpServerConnId  = iota + 1
	CustomSinkConnId  // exported: used by ExerciseSinkInternals
	localSourceConnId
	HttpSinkConnId // exported: used by ExerciseSinkInternals
)

const (
	endpointMapId = iota + 1
	endpointFilterId
	endpointFlatMapId
	endpointFlatMapIterableId
	endpointProcessId
	endpointSplitId
	endpointMerge1Id
	endpointMerge2Id
	endpointCaseId
	endpointDelayId
	endpointKeyById
	endpointJoinLeftId
	endpointJoinRightId
	endpointJoinLLeftId
	endpointJoinRLeftId
	endpointJoinLRightId
	endpointJoinRRightId
	endpointJoinLOuterId
	endpointJoinROuterId
	endpointMultiJoin1Id
	endpointMultiJoin2Id
	endpointMultiJoin3Id

	sinkEndpointMapId
	sinkEndpointFilterId
	sinkEndpointFlatMapId
	sinkEndpointFlatMapIterableId
	sinkEndpointProcessId
	sinkEndpointProcessErrorId
	sinkEndpointSplit1Id
	sinkEndpointSplit2Id
	sinkEndpointMergeId
	sinkEndpointCaseAId
	sinkEndpointCaseBId
	sinkEndpointDelayId
	sinkEndpointKeyById
	sinkEndpointJoinId
	sinkEndpointJoinLeftId
	sinkEndpointJoinRightId
	sinkEndpointJoinOuterId
	endpointLocalSourceId
	sinkEndpointLocalSourceId
	endpointLocalSourceResultId
	endpointHttpSinkInId
	sinkEndpointHttpSinkOutId
	sinkEndpointMultiJoinId
	endpointHttpResultId
)

type Config struct {
	Services struct {
		OperatorService cfg.ServiceConfig `yaml:"operatorService" mapstructure:"operatorService"`
	} `yaml:"services" mapstructure:"services"`

	Streams struct {
		InputMap    cfg.InputStreamConfig `yaml:"inputMap" mapstructure:"inputMap"`
		MapStream   cfg.MapStreamConfig   `yaml:"mapStream" mapstructure:"mapStream"`
		SinkMap     cfg.SinkStreamConfig  `yaml:"sinkMap" mapstructure:"sinkMap"`

		InputFilter    cfg.InputStreamConfig  `yaml:"inputFilter" mapstructure:"inputFilter"`
		FilterStream   cfg.FilterStreamConfig `yaml:"filterStream" mapstructure:"filterStream"`
		SinkFilter     cfg.SinkStreamConfig   `yaml:"sinkFilter" mapstructure:"sinkFilter"`

		InputFlatMap    cfg.InputStreamConfig    `yaml:"inputFlatMap" mapstructure:"inputFlatMap"`
		FlatMapStream   cfg.FlatMapStreamConfig  `yaml:"flatMapStream" mapstructure:"flatMapStream"`
		SinkFlatMap     cfg.SinkStreamConfig     `yaml:"sinkFlatMap" mapstructure:"sinkFlatMap"`

		InputFlatMapIterable    cfg.InputStreamConfig            `yaml:"inputFlatMapIterable" mapstructure:"inputFlatMapIterable"`
		FlatMapIterableStream   cfg.FlatMapIterableStreamConfig  `yaml:"flatMapIterableStream" mapstructure:"flatMapIterableStream"`
		SinkFlatMapIterable     cfg.SinkStreamConfig             `yaml:"sinkFlatMapIterable" mapstructure:"sinkFlatMapIterable"`

		InputProcess      cfg.InputStreamConfig   `yaml:"inputProcess" mapstructure:"inputProcess"`
		ProcessStream     cfg.ProcessStreamConfig `yaml:"processStream" mapstructure:"processStream"`
		SinkProcess       cfg.SinkStreamConfig    `yaml:"sinkProcess" mapstructure:"sinkProcess"`
		SinkProcessError  cfg.SinkStreamConfig    `yaml:"sinkProcessError" mapstructure:"sinkProcessError"`

		InputSplit  cfg.InputStreamConfig  `yaml:"inputSplit" mapstructure:"inputSplit"`
		SplitStream cfg.SplitStreamConfig  `yaml:"splitStream" mapstructure:"splitStream"`
		SinkSplit1  cfg.SinkStreamConfig   `yaml:"sinkSplit1" mapstructure:"sinkSplit1"`
		SinkSplit2  cfg.SinkStreamConfig   `yaml:"sinkSplit2" mapstructure:"sinkSplit2"`

		InputMerge1  cfg.InputStreamConfig `yaml:"inputMerge1" mapstructure:"inputMerge1"`
		InputMerge2  cfg.InputStreamConfig `yaml:"inputMerge2" mapstructure:"inputMerge2"`
		MergeStream  cfg.MergeStreamConfig `yaml:"mergeStream" mapstructure:"mergeStream"`
		SinkMerge    cfg.SinkStreamConfig  `yaml:"sinkMerge" mapstructure:"sinkMerge"`

		InputCase   cfg.InputStreamConfig `yaml:"inputCase" mapstructure:"inputCase"`
		CaseStream  cfg.CaseStreamConfig  `yaml:"caseStream" mapstructure:"caseStream"`
		WhenA       cfg.WhenStreamConfig  `yaml:"whenA" mapstructure:"whenA"`
		WhenB       cfg.WhenStreamConfig  `yaml:"whenB" mapstructure:"whenB"`
		SinkCaseA   cfg.SinkStreamConfig  `yaml:"sinkCaseA" mapstructure:"sinkCaseA"`
		SinkCaseB   cfg.SinkStreamConfig  `yaml:"sinkCaseB" mapstructure:"sinkCaseB"`

		InputDelay  cfg.InputStreamConfig  `yaml:"inputDelay" mapstructure:"inputDelay"`
		DelayStream cfg.DelayStreamConfig  `yaml:"delayStream" mapstructure:"delayStream"`
		SinkDelay   cfg.SinkStreamConfig   `yaml:"sinkDelay" mapstructure:"sinkDelay"`

		InputKeyBy  cfg.InputStreamConfig  `yaml:"inputKeyBy" mapstructure:"inputKeyBy"`
		KeyByStream cfg.KeyByStreamConfig  `yaml:"keyByStream" mapstructure:"keyByStream"`
		SinkKeyBy   cfg.SinkStreamConfig   `yaml:"sinkKeyBy" mapstructure:"sinkKeyBy"`

		InputJoinLeft   cfg.InputStreamConfig `yaml:"inputJoinLeft" mapstructure:"inputJoinLeft"`
		KeyByJoinLeft   cfg.KeyByStreamConfig `yaml:"keyByJoinLeft" mapstructure:"keyByJoinLeft"`
		InputJoinRight  cfg.InputStreamConfig `yaml:"inputJoinRight" mapstructure:"inputJoinRight"`
		KeyByJoinRight  cfg.KeyByStreamConfig `yaml:"keyByJoinRight" mapstructure:"keyByJoinRight"`
		JoinStream      cfg.JoinStreamConfig  `yaml:"joinStream" mapstructure:"joinStream"`
		SinkJoin        cfg.SinkStreamConfig  `yaml:"sinkJoin" mapstructure:"sinkJoin"`

		InputJoinLLeft   cfg.InputStreamConfig `yaml:"inputJoinLLeft" mapstructure:"inputJoinLLeft"`
		KeyByJoinLLeft   cfg.KeyByStreamConfig `yaml:"keyByJoinLLeft" mapstructure:"keyByJoinLLeft"`
		InputJoinRLeft   cfg.InputStreamConfig `yaml:"inputJoinRLeft" mapstructure:"inputJoinRLeft"`
		KeyByJoinRLeft   cfg.KeyByStreamConfig `yaml:"keyByJoinRLeft" mapstructure:"keyByJoinRLeft"`
		JoinStreamLeft   cfg.JoinStreamConfig  `yaml:"joinStreamLeft" mapstructure:"joinStreamLeft"`
		SinkJoinLeft     cfg.SinkStreamConfig  `yaml:"sinkJoinLeft" mapstructure:"sinkJoinLeft"`

		InputJoinLRight  cfg.InputStreamConfig `yaml:"inputJoinLRight" mapstructure:"inputJoinLRight"`
		KeyByJoinLRight  cfg.KeyByStreamConfig `yaml:"keyByJoinLRight" mapstructure:"keyByJoinLRight"`
		InputJoinRRight  cfg.InputStreamConfig `yaml:"inputJoinRRight" mapstructure:"inputJoinRRight"`
		KeyByJoinRRight  cfg.KeyByStreamConfig `yaml:"keyByJoinRRight" mapstructure:"keyByJoinRRight"`
		JoinStreamRight  cfg.JoinStreamConfig  `yaml:"joinStreamRight" mapstructure:"joinStreamRight"`
		SinkJoinRight    cfg.SinkStreamConfig  `yaml:"sinkJoinRight" mapstructure:"sinkJoinRight"`

		InputJoinLOuter  cfg.InputStreamConfig `yaml:"inputJoinLOuter" mapstructure:"inputJoinLOuter"`
		KeyByJoinLOuter  cfg.KeyByStreamConfig `yaml:"keyByJoinLOuter" mapstructure:"keyByJoinLOuter"`
		InputJoinROuter  cfg.InputStreamConfig `yaml:"inputJoinROuter" mapstructure:"inputJoinROuter"`
		KeyByJoinROuter  cfg.KeyByStreamConfig `yaml:"keyByJoinROuter" mapstructure:"keyByJoinROuter"`
		JoinStreamOuter  cfg.JoinStreamConfig  `yaml:"joinStreamOuter" mapstructure:"joinStreamOuter"`
		SinkJoinOuter    cfg.SinkStreamConfig  `yaml:"sinkJoinOuter" mapstructure:"sinkJoinOuter"`

		InputLocalSource       cfg.InputStreamConfig `yaml:"inputLocalSource" mapstructure:"inputLocalSource"`
		SinkLocalSource        cfg.SinkStreamConfig  `yaml:"sinkLocalSource" mapstructure:"sinkLocalSource"`
		InputLocalSourceResult cfg.InputStreamConfig `yaml:"inputLocalSourceResult" mapstructure:"inputLocalSourceResult"`
		MapLocalSourceResult   cfg.MapStreamConfig   `yaml:"mapLocalSourceResult" mapstructure:"mapLocalSourceResult"`

		InputHttpSink cfg.InputStreamConfig `yaml:"inputHttpSink" mapstructure:"inputHttpSink"`
		SinkHttpSink  cfg.SinkStreamConfig  `yaml:"sinkHttpSink" mapstructure:"sinkHttpSink"`

		InputMultiJoin1  cfg.InputStreamConfig      `yaml:"inputMultiJoin1" mapstructure:"inputMultiJoin1"`
		KeyByMultiJoin1  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin1" mapstructure:"keyByMultiJoin1"`
		InputMultiJoin2  cfg.InputStreamConfig      `yaml:"inputMultiJoin2" mapstructure:"inputMultiJoin2"`
		KeyByMultiJoin2  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin2" mapstructure:"keyByMultiJoin2"`
		InputMultiJoin3  cfg.InputStreamConfig      `yaml:"inputMultiJoin3" mapstructure:"inputMultiJoin3"`
		KeyByMultiJoin3  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin3" mapstructure:"keyByMultiJoin3"`
		MultiJoinStream  cfg.MultiJoinStreamConfig  `yaml:"multiJoinStream" mapstructure:"multiJoinStream"`
		SinkMultiJoin    cfg.SinkStreamConfig       `yaml:"sinkMultiJoin" mapstructure:"sinkMultiJoin"`

		InputHttpResult cfg.InputStreamConfig `yaml:"inputHttpResult" mapstructure:"inputHttpResult"`
		MapHttpResult   cfg.MapStreamConfig   `yaml:"mapHttpResult" mapstructure:"mapHttpResult"`
	} `yaml:"streams" mapstructure:"streams"`

	DataConnectors struct {
		HttpServer   cfg.HttpDataConnectorConfig   `yaml:"httpServer" mapstructure:"httpServer"`
		CustomSink   cfg.CustomDataConnectorConfig `yaml:"customSink" mapstructure:"customSink"`
		LocalSource  cfg.CustomDataConnectorConfig `yaml:"localSource" mapstructure:"localSource"`
		HttpSink     cfg.HttpDataConnectorConfig   `yaml:"httpSink" mapstructure:"httpSink"`
	} `yaml:"dataConnectors" mapstructure:"dataConnectors"`

	Endpoints struct {
		Map             cfg.HttpEndpointConfig   `yaml:"map" mapstructure:"map"`
		Filter          cfg.HttpEndpointConfig   `yaml:"filter" mapstructure:"filter"`
		FlatMap         cfg.HttpEndpointConfig   `yaml:"flatMap" mapstructure:"flatMap"`
		FlatMapIterable cfg.HttpEndpointConfig   `yaml:"flatMapIterable" mapstructure:"flatMapIterable"`
		Process         cfg.HttpEndpointConfig   `yaml:"process" mapstructure:"process"`
		Split           cfg.HttpEndpointConfig   `yaml:"split" mapstructure:"split"`
		Merge1          cfg.HttpEndpointConfig   `yaml:"merge1" mapstructure:"merge1"`
		Merge2          cfg.HttpEndpointConfig   `yaml:"merge2" mapstructure:"merge2"`
		Case            cfg.HttpEndpointConfig   `yaml:"case" mapstructure:"case"`
		Delay           cfg.HttpEndpointConfig   `yaml:"delay" mapstructure:"delay"`
		KeyBy           cfg.HttpEndpointConfig   `yaml:"keyBy" mapstructure:"keyBy"`
		JoinLeft        cfg.HttpEndpointConfig   `yaml:"joinLeft" mapstructure:"joinLeft"`
		JoinRight       cfg.HttpEndpointConfig   `yaml:"joinRight" mapstructure:"joinRight"`
		JoinLLeft       cfg.HttpEndpointConfig   `yaml:"joinLLeft" mapstructure:"joinLLeft"`
		JoinRLeft       cfg.HttpEndpointConfig   `yaml:"joinRLeft" mapstructure:"joinRLeft"`
		JoinLRight      cfg.HttpEndpointConfig   `yaml:"joinLRight" mapstructure:"joinLRight"`
		JoinRRight      cfg.HttpEndpointConfig   `yaml:"joinRRight" mapstructure:"joinRRight"`
		JoinLOuter      cfg.HttpEndpointConfig   `yaml:"joinLOuter" mapstructure:"joinLOuter"`
		JoinROuter      cfg.HttpEndpointConfig   `yaml:"joinROuter" mapstructure:"joinROuter"`
		MultiJoin1      cfg.HttpEndpointConfig   `yaml:"multiJoin1" mapstructure:"multiJoin1"`
		MultiJoin2      cfg.HttpEndpointConfig   `yaml:"multiJoin2" mapstructure:"multiJoin2"`
		MultiJoin3      cfg.HttpEndpointConfig   `yaml:"multiJoin3" mapstructure:"multiJoin3"`

		SinkMap             cfg.CustomEndpointConfig `yaml:"sinkMap" mapstructure:"sinkMap"`
		SinkFilter          cfg.CustomEndpointConfig `yaml:"sinkFilter" mapstructure:"sinkFilter"`
		SinkFlatMap         cfg.CustomEndpointConfig `yaml:"sinkFlatMap" mapstructure:"sinkFlatMap"`
		SinkFlatMapIterable cfg.CustomEndpointConfig `yaml:"sinkFlatMapIterable" mapstructure:"sinkFlatMapIterable"`
		SinkProcess         cfg.CustomEndpointConfig `yaml:"sinkProcess" mapstructure:"sinkProcess"`
		SinkProcessError    cfg.CustomEndpointConfig `yaml:"sinkProcessError" mapstructure:"sinkProcessError"`
		SinkSplit1          cfg.CustomEndpointConfig `yaml:"sinkSplit1" mapstructure:"sinkSplit1"`
		SinkSplit2          cfg.CustomEndpointConfig `yaml:"sinkSplit2" mapstructure:"sinkSplit2"`
		SinkMerge           cfg.CustomEndpointConfig `yaml:"sinkMerge" mapstructure:"sinkMerge"`
		SinkCaseA           cfg.CustomEndpointConfig `yaml:"sinkCaseA" mapstructure:"sinkCaseA"`
		SinkCaseB           cfg.CustomEndpointConfig `yaml:"sinkCaseB" mapstructure:"sinkCaseB"`
		SinkDelay           cfg.CustomEndpointConfig `yaml:"sinkDelay" mapstructure:"sinkDelay"`
		SinkKeyBy           cfg.CustomEndpointConfig `yaml:"sinkKeyBy" mapstructure:"sinkKeyBy"`
		SinkJoin            cfg.CustomEndpointConfig `yaml:"sinkJoin" mapstructure:"sinkJoin"`
		SinkJoinLeft        cfg.CustomEndpointConfig `yaml:"sinkJoinLeft" mapstructure:"sinkJoinLeft"`
		SinkJoinRight       cfg.CustomEndpointConfig `yaml:"sinkJoinRight" mapstructure:"sinkJoinRight"`
		SinkJoinOuter       cfg.CustomEndpointConfig `yaml:"sinkJoinOuter" mapstructure:"sinkJoinOuter"`
		LocalSource             cfg.CustomEndpointConfig `yaml:"localSource" mapstructure:"localSource"`
		SinkLocalSource         cfg.CustomEndpointConfig `yaml:"sinkLocalSource" mapstructure:"sinkLocalSource"`
		LocalSourceResult       cfg.CustomEndpointConfig `yaml:"localSourceResult" mapstructure:"localSourceResult"`
		HttpSinkIn              cfg.HttpEndpointConfig   `yaml:"httpSinkIn" mapstructure:"httpSinkIn"`
		SinkHttpSinkOut         cfg.HttpEndpointConfig   `yaml:"sinkHttpSinkOut" mapstructure:"sinkHttpSinkOut"`
		SinkMultiJoin           cfg.CustomEndpointConfig `yaml:"sinkMultiJoin" mapstructure:"sinkMultiJoin"`
		HttpResult              cfg.HttpEndpointConfig   `yaml:"httpResult" mapstructure:"httpResult"`
	} `yaml:"endpoints" mapstructure:"endpoints"`

	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (c *Config) GetProperty(name string) interface{} {
	return c.Properties[name]
}

func (c *Config) GetServices() []*cfg.ServiceConfig {
	return []*cfg.ServiceConfig{&c.Services.OperatorService}
}

func (c *Config) GetStreams() []cfg.StreamConfig {
	return []cfg.StreamConfig{
		&c.Streams.InputMap, &c.Streams.MapStream, &c.Streams.SinkMap,
		&c.Streams.InputFilter, &c.Streams.FilterStream, &c.Streams.SinkFilter,
		&c.Streams.InputFlatMap, &c.Streams.FlatMapStream, &c.Streams.SinkFlatMap,
		&c.Streams.InputFlatMapIterable, &c.Streams.FlatMapIterableStream, &c.Streams.SinkFlatMapIterable,
		&c.Streams.InputProcess, &c.Streams.ProcessStream, &c.Streams.SinkProcess, &c.Streams.SinkProcessError,
		&c.Streams.InputSplit, &c.Streams.SplitStream, &c.Streams.SinkSplit1, &c.Streams.SinkSplit2,
		&c.Streams.InputMerge1, &c.Streams.InputMerge2, &c.Streams.MergeStream, &c.Streams.SinkMerge,
		&c.Streams.InputCase, &c.Streams.CaseStream, &c.Streams.WhenA, &c.Streams.WhenB,
		&c.Streams.SinkCaseA, &c.Streams.SinkCaseB,
		&c.Streams.InputDelay, &c.Streams.DelayStream, &c.Streams.SinkDelay,
		&c.Streams.InputKeyBy, &c.Streams.KeyByStream, &c.Streams.SinkKeyBy,
		&c.Streams.InputJoinLeft, &c.Streams.KeyByJoinLeft,
		&c.Streams.InputJoinRight, &c.Streams.KeyByJoinRight,
		&c.Streams.JoinStream, &c.Streams.SinkJoin,
		&c.Streams.InputJoinLLeft, &c.Streams.KeyByJoinLLeft,
		&c.Streams.InputJoinRLeft, &c.Streams.KeyByJoinRLeft,
		&c.Streams.JoinStreamLeft, &c.Streams.SinkJoinLeft,
		&c.Streams.InputJoinLRight, &c.Streams.KeyByJoinLRight,
		&c.Streams.InputJoinRRight, &c.Streams.KeyByJoinRRight,
		&c.Streams.JoinStreamRight, &c.Streams.SinkJoinRight,
		&c.Streams.InputJoinLOuter, &c.Streams.KeyByJoinLOuter,
		&c.Streams.InputJoinROuter, &c.Streams.KeyByJoinROuter,
		&c.Streams.JoinStreamOuter, &c.Streams.SinkJoinOuter,
		&c.Streams.InputLocalSource, &c.Streams.SinkLocalSource,
		&c.Streams.InputLocalSourceResult, &c.Streams.MapLocalSourceResult,
		&c.Streams.InputHttpSink, &c.Streams.SinkHttpSink,
		&c.Streams.InputMultiJoin1, &c.Streams.KeyByMultiJoin1,
		&c.Streams.InputMultiJoin2, &c.Streams.KeyByMultiJoin2,
		&c.Streams.InputMultiJoin3, &c.Streams.KeyByMultiJoin3,
		&c.Streams.MultiJoinStream, &c.Streams.SinkMultiJoin,
		&c.Streams.InputHttpResult, &c.Streams.MapHttpResult,
	}
}

func (c *Config) GetDataConnectors() []cfg.DataConnectorConfig {
	return []cfg.DataConnectorConfig{
		&c.DataConnectors.HttpServer,
		&c.DataConnectors.CustomSink,
		&c.DataConnectors.LocalSource,
		&c.DataConnectors.HttpSink,
	}
}

func (c *Config) GetEndpoints() []cfg.EndpointConfig {
	return []cfg.EndpointConfig{
		&c.Endpoints.Map, &c.Endpoints.Filter, &c.Endpoints.FlatMap,
		&c.Endpoints.FlatMapIterable, &c.Endpoints.Process, &c.Endpoints.Split,
		&c.Endpoints.Merge1, &c.Endpoints.Merge2, &c.Endpoints.Case,
		&c.Endpoints.Delay, &c.Endpoints.KeyBy,
		&c.Endpoints.JoinLeft, &c.Endpoints.JoinRight,
		&c.Endpoints.JoinLLeft, &c.Endpoints.JoinRLeft,
		&c.Endpoints.JoinLRight, &c.Endpoints.JoinRRight,
		&c.Endpoints.JoinLOuter, &c.Endpoints.JoinROuter,
		&c.Endpoints.MultiJoin1, &c.Endpoints.MultiJoin2, &c.Endpoints.MultiJoin3,

		&c.Endpoints.SinkMap, &c.Endpoints.SinkFilter, &c.Endpoints.SinkFlatMap,
		&c.Endpoints.SinkFlatMapIterable, &c.Endpoints.SinkProcess, &c.Endpoints.SinkProcessError,
		&c.Endpoints.SinkSplit1, &c.Endpoints.SinkSplit2, &c.Endpoints.SinkMerge,
		&c.Endpoints.SinkCaseA, &c.Endpoints.SinkCaseB, &c.Endpoints.SinkDelay,
		&c.Endpoints.SinkKeyBy,
		&c.Endpoints.SinkJoin, &c.Endpoints.SinkJoinLeft, &c.Endpoints.SinkJoinRight, &c.Endpoints.SinkJoinOuter,
		&c.Endpoints.LocalSource, &c.Endpoints.SinkLocalSource,
		&c.Endpoints.LocalSourceResult,
		&c.Endpoints.HttpSinkIn, &c.Endpoints.SinkHttpSinkOut,
		&c.Endpoints.SinkMultiJoin,
		&c.Endpoints.HttpResult,
	}
}

func (c *Config) GetPools() []*cfg.PoolConfig      { return nil }
func (c *Config) GetLinks() []*cfg.LinkConfig       { return nil }
func (c *Config) GetModules() []*cfg.ModuleConfig   { return nil }
func (c *Config) GetTypes() []*cfg.TypeConfig       { return nil }
func (c *Config) ApplyEnvironment() error           { return nil }

func MakeConfig() *Config {
	funcCall := &cfg.CallSemanticsGroup{FunctionCall: &cfg.FunctionCallSemanticsConfig{}}
	return &Config{
		Services: struct {
			OperatorService cfg.ServiceConfig `yaml:"operatorService" mapstructure:"operatorService"`
		}{
			OperatorService: cfg.ServiceConfig{
				ID:                   operatorServiceId,
				Name:                 "OperatorService",
				Color:                "#D2E5FF",
				GrpcHost:             "localhost",
				GrpcPort:             9301,
				HttpHost:             "localhost",
				HttpPort:             9092,
				MetricsHandler:       "metrics",
				ShutdownTimeout:      30000,
				StatusHandler:        "status",
				Environment:          "operator_test",
				DefaultCallSemantics: funcCall,
			},
		},
		Streams: struct {
			InputMap    cfg.InputStreamConfig `yaml:"inputMap" mapstructure:"inputMap"`
			MapStream   cfg.MapStreamConfig   `yaml:"mapStream" mapstructure:"mapStream"`
			SinkMap     cfg.SinkStreamConfig  `yaml:"sinkMap" mapstructure:"sinkMap"`

			InputFilter    cfg.InputStreamConfig  `yaml:"inputFilter" mapstructure:"inputFilter"`
			FilterStream   cfg.FilterStreamConfig `yaml:"filterStream" mapstructure:"filterStream"`
			SinkFilter     cfg.SinkStreamConfig   `yaml:"sinkFilter" mapstructure:"sinkFilter"`

			InputFlatMap    cfg.InputStreamConfig    `yaml:"inputFlatMap" mapstructure:"inputFlatMap"`
			FlatMapStream   cfg.FlatMapStreamConfig  `yaml:"flatMapStream" mapstructure:"flatMapStream"`
			SinkFlatMap     cfg.SinkStreamConfig     `yaml:"sinkFlatMap" mapstructure:"sinkFlatMap"`

			InputFlatMapIterable    cfg.InputStreamConfig            `yaml:"inputFlatMapIterable" mapstructure:"inputFlatMapIterable"`
			FlatMapIterableStream   cfg.FlatMapIterableStreamConfig  `yaml:"flatMapIterableStream" mapstructure:"flatMapIterableStream"`
			SinkFlatMapIterable     cfg.SinkStreamConfig             `yaml:"sinkFlatMapIterable" mapstructure:"sinkFlatMapIterable"`

			InputProcess      cfg.InputStreamConfig   `yaml:"inputProcess" mapstructure:"inputProcess"`
			ProcessStream     cfg.ProcessStreamConfig `yaml:"processStream" mapstructure:"processStream"`
			SinkProcess       cfg.SinkStreamConfig    `yaml:"sinkProcess" mapstructure:"sinkProcess"`
			SinkProcessError  cfg.SinkStreamConfig    `yaml:"sinkProcessError" mapstructure:"sinkProcessError"`

			InputSplit  cfg.InputStreamConfig  `yaml:"inputSplit" mapstructure:"inputSplit"`
			SplitStream cfg.SplitStreamConfig  `yaml:"splitStream" mapstructure:"splitStream"`
			SinkSplit1  cfg.SinkStreamConfig   `yaml:"sinkSplit1" mapstructure:"sinkSplit1"`
			SinkSplit2  cfg.SinkStreamConfig   `yaml:"sinkSplit2" mapstructure:"sinkSplit2"`

			InputMerge1  cfg.InputStreamConfig `yaml:"inputMerge1" mapstructure:"inputMerge1"`
			InputMerge2  cfg.InputStreamConfig `yaml:"inputMerge2" mapstructure:"inputMerge2"`
			MergeStream  cfg.MergeStreamConfig `yaml:"mergeStream" mapstructure:"mergeStream"`
			SinkMerge    cfg.SinkStreamConfig  `yaml:"sinkMerge" mapstructure:"sinkMerge"`

			InputCase   cfg.InputStreamConfig `yaml:"inputCase" mapstructure:"inputCase"`
			CaseStream  cfg.CaseStreamConfig  `yaml:"caseStream" mapstructure:"caseStream"`
			WhenA       cfg.WhenStreamConfig  `yaml:"whenA" mapstructure:"whenA"`
			WhenB       cfg.WhenStreamConfig  `yaml:"whenB" mapstructure:"whenB"`
			SinkCaseA   cfg.SinkStreamConfig  `yaml:"sinkCaseA" mapstructure:"sinkCaseA"`
			SinkCaseB   cfg.SinkStreamConfig  `yaml:"sinkCaseB" mapstructure:"sinkCaseB"`

			InputDelay  cfg.InputStreamConfig  `yaml:"inputDelay" mapstructure:"inputDelay"`
			DelayStream cfg.DelayStreamConfig  `yaml:"delayStream" mapstructure:"delayStream"`
			SinkDelay   cfg.SinkStreamConfig   `yaml:"sinkDelay" mapstructure:"sinkDelay"`

			InputKeyBy  cfg.InputStreamConfig  `yaml:"inputKeyBy" mapstructure:"inputKeyBy"`
			KeyByStream cfg.KeyByStreamConfig  `yaml:"keyByStream" mapstructure:"keyByStream"`
			SinkKeyBy   cfg.SinkStreamConfig   `yaml:"sinkKeyBy" mapstructure:"sinkKeyBy"`

			InputJoinLeft   cfg.InputStreamConfig `yaml:"inputJoinLeft" mapstructure:"inputJoinLeft"`
			KeyByJoinLeft   cfg.KeyByStreamConfig `yaml:"keyByJoinLeft" mapstructure:"keyByJoinLeft"`
			InputJoinRight  cfg.InputStreamConfig `yaml:"inputJoinRight" mapstructure:"inputJoinRight"`
			KeyByJoinRight  cfg.KeyByStreamConfig `yaml:"keyByJoinRight" mapstructure:"keyByJoinRight"`
			JoinStream      cfg.JoinStreamConfig  `yaml:"joinStream" mapstructure:"joinStream"`
			SinkJoin        cfg.SinkStreamConfig  `yaml:"sinkJoin" mapstructure:"sinkJoin"`

			InputJoinLLeft  cfg.InputStreamConfig `yaml:"inputJoinLLeft" mapstructure:"inputJoinLLeft"`
			KeyByJoinLLeft  cfg.KeyByStreamConfig `yaml:"keyByJoinLLeft" mapstructure:"keyByJoinLLeft"`
			InputJoinRLeft  cfg.InputStreamConfig `yaml:"inputJoinRLeft" mapstructure:"inputJoinRLeft"`
			KeyByJoinRLeft  cfg.KeyByStreamConfig `yaml:"keyByJoinRLeft" mapstructure:"keyByJoinRLeft"`
			JoinStreamLeft  cfg.JoinStreamConfig  `yaml:"joinStreamLeft" mapstructure:"joinStreamLeft"`
			SinkJoinLeft    cfg.SinkStreamConfig  `yaml:"sinkJoinLeft" mapstructure:"sinkJoinLeft"`

			InputJoinLRight cfg.InputStreamConfig `yaml:"inputJoinLRight" mapstructure:"inputJoinLRight"`
			KeyByJoinLRight cfg.KeyByStreamConfig `yaml:"keyByJoinLRight" mapstructure:"keyByJoinLRight"`
			InputJoinRRight cfg.InputStreamConfig `yaml:"inputJoinRRight" mapstructure:"inputJoinRRight"`
			KeyByJoinRRight cfg.KeyByStreamConfig `yaml:"keyByJoinRRight" mapstructure:"keyByJoinRRight"`
			JoinStreamRight cfg.JoinStreamConfig  `yaml:"joinStreamRight" mapstructure:"joinStreamRight"`
			SinkJoinRight   cfg.SinkStreamConfig  `yaml:"sinkJoinRight" mapstructure:"sinkJoinRight"`

			InputJoinLOuter cfg.InputStreamConfig `yaml:"inputJoinLOuter" mapstructure:"inputJoinLOuter"`
			KeyByJoinLOuter cfg.KeyByStreamConfig `yaml:"keyByJoinLOuter" mapstructure:"keyByJoinLOuter"`
			InputJoinROuter cfg.InputStreamConfig `yaml:"inputJoinROuter" mapstructure:"inputJoinROuter"`
			KeyByJoinROuter cfg.KeyByStreamConfig `yaml:"keyByJoinROuter" mapstructure:"keyByJoinROuter"`
			JoinStreamOuter cfg.JoinStreamConfig  `yaml:"joinStreamOuter" mapstructure:"joinStreamOuter"`
			SinkJoinOuter   cfg.SinkStreamConfig  `yaml:"sinkJoinOuter" mapstructure:"sinkJoinOuter"`

			InputLocalSource       cfg.InputStreamConfig `yaml:"inputLocalSource" mapstructure:"inputLocalSource"`
			SinkLocalSource        cfg.SinkStreamConfig  `yaml:"sinkLocalSource" mapstructure:"sinkLocalSource"`
			InputLocalSourceResult cfg.InputStreamConfig `yaml:"inputLocalSourceResult" mapstructure:"inputLocalSourceResult"`
			MapLocalSourceResult   cfg.MapStreamConfig   `yaml:"mapLocalSourceResult" mapstructure:"mapLocalSourceResult"`

			InputHttpSink cfg.InputStreamConfig `yaml:"inputHttpSink" mapstructure:"inputHttpSink"`
			SinkHttpSink  cfg.SinkStreamConfig  `yaml:"sinkHttpSink" mapstructure:"sinkHttpSink"`

			InputMultiJoin1  cfg.InputStreamConfig      `yaml:"inputMultiJoin1" mapstructure:"inputMultiJoin1"`
			KeyByMultiJoin1  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin1" mapstructure:"keyByMultiJoin1"`
			InputMultiJoin2  cfg.InputStreamConfig      `yaml:"inputMultiJoin2" mapstructure:"inputMultiJoin2"`
			KeyByMultiJoin2  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin2" mapstructure:"keyByMultiJoin2"`
			InputMultiJoin3  cfg.InputStreamConfig      `yaml:"inputMultiJoin3" mapstructure:"inputMultiJoin3"`
			KeyByMultiJoin3  cfg.KeyByStreamConfig      `yaml:"keyByMultiJoin3" mapstructure:"keyByMultiJoin3"`
			MultiJoinStream  cfg.MultiJoinStreamConfig  `yaml:"multiJoinStream" mapstructure:"multiJoinStream"`
			SinkMultiJoin    cfg.SinkStreamConfig       `yaml:"sinkMultiJoin" mapstructure:"sinkMultiJoin"`

			InputHttpResult cfg.InputStreamConfig `yaml:"inputHttpResult" mapstructure:"inputHttpResult"`
			MapHttpResult   cfg.MapStreamConfig   `yaml:"mapHttpResult" mapstructure:"mapHttpResult"`
		}{
			// Map pipeline
			InputMap:  cfg.InputStreamConfig{ID: inputMapId, Name: "InputMap", IdService: operatorServiceId, IdEndpoint: endpointMapId, ValueType: "Message"},
			MapStream: cfg.MapStreamConfig{ID: mapStreamId, Name: "MapStream", IdService: operatorServiceId, IdSource: inputMapId},
			SinkMap:   cfg.SinkStreamConfig{ID: sinkMapId, Name: "SinkMap", IdService: operatorServiceId, IdSource: mapStreamId, IdEndpoint: sinkEndpointMapId},

			// Filter pipeline
			InputFilter:  cfg.InputStreamConfig{ID: inputFilterId, Name: "InputFilter", IdService: operatorServiceId, IdEndpoint: endpointFilterId, ValueType: "Message"},
			FilterStream: cfg.FilterStreamConfig{ID: filterStreamId, Name: "FilterStream", IdService: operatorServiceId, IdSource: inputFilterId},
			SinkFilter:   cfg.SinkStreamConfig{ID: sinkFilterId, Name: "SinkFilter", IdService: operatorServiceId, IdSource: filterStreamId, IdEndpoint: sinkEndpointFilterId},

			// FlatMap pipeline
			InputFlatMap:  cfg.InputStreamConfig{ID: inputFlatMapId, Name: "InputFlatMap", IdService: operatorServiceId, IdEndpoint: endpointFlatMapId, ValueType: "Message"},
			FlatMapStream: cfg.FlatMapStreamConfig{ID: flatMapStreamId, Name: "FlatMapStream", IdService: operatorServiceId, IdSource: inputFlatMapId},
			SinkFlatMap:   cfg.SinkStreamConfig{ID: sinkFlatMapId, Name: "SinkFlatMap", IdService: operatorServiceId, IdSource: flatMapStreamId, IdEndpoint: sinkEndpointFlatMapId},

			// FlatMapIterable pipeline
			InputFlatMapIterable:  cfg.InputStreamConfig{ID: inputFlatMapIterableId, Name: "InputFlatMapIterable", IdService: operatorServiceId, IdEndpoint: endpointFlatMapIterableId, ValueType: "Messages"},
			FlatMapIterableStream: cfg.FlatMapIterableStreamConfig{ID: flatMapIterableStreamId, Name: "FlatMapIterableStream", IdService: operatorServiceId, IdSource: inputFlatMapIterableId},
			SinkFlatMapIterable:   cfg.SinkStreamConfig{ID: sinkFlatMapIterableId, Name: "SinkFlatMapIterable", IdService: operatorServiceId, IdSource: flatMapIterableStreamId, IdEndpoint: sinkEndpointFlatMapIterableId},

			// Process pipeline
			InputProcess:     cfg.InputStreamConfig{ID: inputProcessId, Name: "InputProcess", IdService: operatorServiceId, IdEndpoint: endpointProcessId, ValueType: "Message"},
			ProcessStream:    cfg.ProcessStreamConfig{ID: processStreamId, Name: "ProcessStream", IdService: operatorServiceId, IdSource: inputProcessId},
			SinkProcess:      cfg.SinkStreamConfig{ID: sinkProcessId, Name: "SinkProcess", IdService: operatorServiceId, IdSource: processStreamId, IdEndpoint: sinkEndpointProcessId},
			SinkProcessError: cfg.SinkStreamConfig{ID: sinkProcessErrorId, Name: "SinkProcessError", IdService: operatorServiceId, IdSource: -processStreamId, IdEndpoint: sinkEndpointProcessErrorId},

			// Split pipeline
			InputSplit:  cfg.InputStreamConfig{ID: inputSplitId, Name: "InputSplit", IdService: operatorServiceId, IdEndpoint: endpointSplitId, ValueType: "Message"},
			SplitStream: cfg.SplitStreamConfig{ID: splitStreamId, Name: "SplitStream", IdService: operatorServiceId, IdSource: inputSplitId},
			SinkSplit1:  cfg.SinkStreamConfig{ID: sinkSplit1Id, Name: "SinkSplit1", IdService: operatorServiceId, IdSource: splitStreamId, IdEndpoint: sinkEndpointSplit1Id},
			SinkSplit2:  cfg.SinkStreamConfig{ID: sinkSplit2Id, Name: "SinkSplit2", IdService: operatorServiceId, IdSource: splitStreamId, IdEndpoint: sinkEndpointSplit2Id},

			// Merge pipeline
			InputMerge1: cfg.InputStreamConfig{ID: inputMerge1Id, Name: "InputMerge1", IdService: operatorServiceId, IdEndpoint: endpointMerge1Id, ValueType: "Message"},
			InputMerge2: cfg.InputStreamConfig{ID: inputMerge2Id, Name: "InputMerge2", IdService: operatorServiceId, IdEndpoint: endpointMerge2Id, ValueType: "Message"},
			MergeStream: cfg.MergeStreamConfig{ID: mergeStreamId, Name: "MergeStream", IdService: operatorServiceId, IdSources: []int{inputMerge1Id, inputMerge2Id}},
			SinkMerge:   cfg.SinkStreamConfig{ID: sinkMergeId, Name: "SinkMerge", IdService: operatorServiceId, IdSource: mergeStreamId, IdEndpoint: sinkEndpointMergeId},

			// Case pipeline
			InputCase:  cfg.InputStreamConfig{ID: inputCaseId, Name: "InputCase", IdService: operatorServiceId, IdEndpoint: endpointCaseId, ValueType: "Message"},
			CaseStream: cfg.CaseStreamConfig{ID: caseStreamId, Name: "CaseStream", IdService: operatorServiceId, IdSource: inputCaseId},
			WhenA:      cfg.WhenStreamConfig{ID: whenAId, Name: "WhenA", IdService: operatorServiceId, IdSource: caseStreamId, ValueType: "Message"},
			WhenB:      cfg.WhenStreamConfig{ID: whenBId, Name: "WhenB", IdService: operatorServiceId, IdSource: caseStreamId, ValueType: "Message"},
			SinkCaseA:  cfg.SinkStreamConfig{ID: sinkCaseAId, Name: "SinkCaseA", IdService: operatorServiceId, IdSource: whenAId, IdEndpoint: sinkEndpointCaseAId},
			SinkCaseB:  cfg.SinkStreamConfig{ID: sinkCaseBId, Name: "SinkCaseB", IdService: operatorServiceId, IdSource: whenBId, IdEndpoint: sinkEndpointCaseBId},

			// Delay pipeline
			InputDelay:  cfg.InputStreamConfig{ID: inputDelayId, Name: "InputDelay", IdService: operatorServiceId, IdEndpoint: endpointDelayId, ValueType: "Message"},
			DelayStream: cfg.DelayStreamConfig{ID: delayStreamId, Name: "DelayStream", IdService: operatorServiceId, IdSource: inputDelayId},
			SinkDelay:   cfg.SinkStreamConfig{ID: sinkDelayId, Name: "SinkDelay", IdService: operatorServiceId, IdSource: delayStreamId, IdEndpoint: sinkEndpointDelayId},

			// KeyBy pipeline
			InputKeyBy:  cfg.InputStreamConfig{ID: inputKeyById, Name: "InputKeyBy", IdService: operatorServiceId, IdEndpoint: endpointKeyById, ValueType: "Message"},
			KeyByStream: cfg.KeyByStreamConfig{ID: keyByStreamId, Name: "KeyByStream", IdService: operatorServiceId, IdSource: inputKeyById, KeyType: "string", ValueType: "Message"},
			SinkKeyBy:   cfg.SinkStreamConfig{ID: sinkKeyById, Name: "SinkKeyBy", IdService: operatorServiceId, IdSource: keyByStreamId, IdEndpoint: sinkEndpointKeyById},

			// Inner join pipeline
			InputJoinLeft:  cfg.InputStreamConfig{ID: inputJoinLeftId, Name: "InputJoinLeft", IdService: operatorServiceId, IdEndpoint: endpointJoinLeftId, ValueType: "Message"},
			KeyByJoinLeft:  cfg.KeyByStreamConfig{ID: keyByJoinLeftId, Name: "KeyByJoinLeft", IdService: operatorServiceId, IdSource: inputJoinLeftId, KeyType: "string", ValueType: "Message"},
			InputJoinRight: cfg.InputStreamConfig{ID: inputJoinRightId, Name: "InputJoinRight", IdService: operatorServiceId, IdEndpoint: endpointJoinRightId, ValueType: "Message"},
			KeyByJoinRight: cfg.KeyByStreamConfig{ID: keyByJoinRightId, Name: "KeyByJoinRight", IdService: operatorServiceId, IdSource: inputJoinRightId, KeyType: "string", ValueType: "Message"},
			JoinStream:     cfg.JoinStreamConfig{ID: joinStreamId, Name: "JoinStream", IdService: operatorServiceId, IdSource: keyByJoinLeftId, IdSources: []int{keyByJoinLeftId, keyByJoinRightId}, JoinType: api.JoinTypeInner, JoinStorage: api.JoinStorageTypeHashMap},
			SinkJoin:       cfg.SinkStreamConfig{ID: sinkJoinId, Name: "SinkJoin", IdService: operatorServiceId, IdSource: joinStreamId, IdEndpoint: sinkEndpointJoinId},

			InputJoinLLeft:  cfg.InputStreamConfig{ID: inputJoinLLeftId, Name: "InputJoinLLeft", IdService: operatorServiceId, IdEndpoint: endpointJoinLLeftId, ValueType: "Message"},
			KeyByJoinLLeft:  cfg.KeyByStreamConfig{ID: keyByJoinLLeftId, Name: "KeyByJoinLLeft", IdService: operatorServiceId, IdSource: inputJoinLLeftId, KeyType: "string", ValueType: "Message"},
			InputJoinRLeft:  cfg.InputStreamConfig{ID: inputJoinRLeftId, Name: "InputJoinRLeft", IdService: operatorServiceId, IdEndpoint: endpointJoinRLeftId, ValueType: "Message"},
			KeyByJoinRLeft:  cfg.KeyByStreamConfig{ID: keyByJoinRLeftId, Name: "KeyByJoinRLeft", IdService: operatorServiceId, IdSource: inputJoinRLeftId, KeyType: "string", ValueType: "Message"},
			JoinStreamLeft:  cfg.JoinStreamConfig{ID: joinStreamLeftId, Name: "JoinStreamLeft", IdService: operatorServiceId, IdSource: keyByJoinLLeftId, IdSources: []int{keyByJoinLLeftId, keyByJoinRLeftId}, JoinType: api.JoinTypeLeft, JoinStorage: api.JoinStorageTypeHashMap},
			SinkJoinLeft:    cfg.SinkStreamConfig{ID: sinkJoinLeftId, Name: "SinkJoinLeft", IdService: operatorServiceId, IdSource: joinStreamLeftId, IdEndpoint: sinkEndpointJoinLeftId},

			// Right join pipeline
			InputJoinLRight: cfg.InputStreamConfig{ID: inputJoinLRightId, Name: "InputJoinLRight", IdService: operatorServiceId, IdEndpoint: endpointJoinLRightId, ValueType: "Message"},
			KeyByJoinLRight: cfg.KeyByStreamConfig{ID: keyByJoinLRightId, Name: "KeyByJoinLRight", IdService: operatorServiceId, IdSource: inputJoinLRightId, KeyType: "string", ValueType: "Message"},
			InputJoinRRight: cfg.InputStreamConfig{ID: inputJoinRRightId, Name: "InputJoinRRight", IdService: operatorServiceId, IdEndpoint: endpointJoinRRightId, ValueType: "Message"},
			KeyByJoinRRight: cfg.KeyByStreamConfig{ID: keyByJoinRRightId, Name: "KeyByJoinRRight", IdService: operatorServiceId, IdSource: inputJoinRRightId, KeyType: "string", ValueType: "Message"},
			JoinStreamRight: cfg.JoinStreamConfig{ID: joinStreamRightId, Name: "JoinStreamRight", IdService: operatorServiceId, IdSource: keyByJoinLRightId, IdSources: []int{keyByJoinLRightId, keyByJoinRRightId}, JoinType: api.JoinTypeRight, JoinStorage: api.JoinStorageTypeHashMap},
			SinkJoinRight:   cfg.SinkStreamConfig{ID: sinkJoinRightId, Name: "SinkJoinRight", IdService: operatorServiceId, IdSource: joinStreamRightId, IdEndpoint: sinkEndpointJoinRightId},

			// Outer join pipeline
			InputJoinLOuter: cfg.InputStreamConfig{ID: inputJoinLOuterId, Name: "InputJoinLOuter", IdService: operatorServiceId, IdEndpoint: endpointJoinLOuterId, ValueType: "Message"},
			KeyByJoinLOuter: cfg.KeyByStreamConfig{ID: keyByJoinLOuterId, Name: "KeyByJoinLOuter", IdService: operatorServiceId, IdSource: inputJoinLOuterId, KeyType: "string", ValueType: "Message"},
			InputJoinROuter: cfg.InputStreamConfig{ID: inputJoinROuterId, Name: "InputJoinROuter", IdService: operatorServiceId, IdEndpoint: endpointJoinROuterId, ValueType: "Message"},
			KeyByJoinROuter: cfg.KeyByStreamConfig{ID: keyByJoinROuterId, Name: "KeyByJoinROuter", IdService: operatorServiceId, IdSource: inputJoinROuterId, KeyType: "string", ValueType: "Message"},
			JoinStreamOuter: cfg.JoinStreamConfig{ID: joinStreamOuterId, Name: "JoinStreamOuter", IdService: operatorServiceId, IdSource: keyByJoinLOuterId, IdSources: []int{keyByJoinLOuterId, keyByJoinROuterId}, JoinType: api.JoinTypeOuter, JoinStorage: api.JoinStorageTypeHashMap},
			SinkJoinOuter:   cfg.SinkStreamConfig{ID: sinkJoinOuterId, Name: "SinkJoinOuter", IdService: operatorServiceId, IdSource: joinStreamOuterId, IdEndpoint: sinkEndpointJoinOuterId},

			// LocalSource pipeline (no-result path)
			InputLocalSource: cfg.InputStreamConfig{ID: inputLocalSourceId, Name: "InputLocalSource", IdService: operatorServiceId, IdEndpoint: endpointLocalSourceId, ValueType: "Message"},
			SinkLocalSource:  cfg.SinkStreamConfig{ID: sinkLocalSourceId, Name: "SinkLocalSource", IdService: operatorServiceId, IdSource: inputLocalSourceId, IdEndpoint: sinkEndpointLocalSourceId},

			// LocalSourceResult pipeline (hasResult=true path)
			InputLocalSourceResult: cfg.InputStreamConfig{ID: inputLocalSourceResultId, Name: "InputLocalSourceResult", IdService: operatorServiceId, IdEndpoint: endpointLocalSourceResultId, ValueType: "Message"},
			MapLocalSourceResult:   cfg.MapStreamConfig{ID: mapLocalSourceResultId, Name: "MapLocalSourceResult", IdService: operatorServiceId, IdSource: inputLocalSourceResultId},

			// HttpSink pipeline
			InputHttpSink: cfg.InputStreamConfig{ID: inputHttpSinkId, Name: "InputHttpSink", IdService: operatorServiceId, IdEndpoint: endpointHttpSinkInId, ValueType: "Message"},
			SinkHttpSink:  cfg.SinkStreamConfig{ID: sinkHttpSinkId, Name: "SinkHttpSink", IdService: operatorServiceId, IdSource: inputHttpSinkId, IdEndpoint: sinkEndpointHttpSinkOutId},

			// MultiJoin pipeline
			InputMultiJoin1: cfg.InputStreamConfig{ID: inputMultiJoin1Id, Name: "InputMultiJoin1", IdService: operatorServiceId, IdEndpoint: endpointMultiJoin1Id, ValueType: "Message"},
			KeyByMultiJoin1: cfg.KeyByStreamConfig{ID: keyByMultiJoin1Id, Name: "KeyByMultiJoin1", IdService: operatorServiceId, IdSource: inputMultiJoin1Id, KeyType: "string", ValueType: "Message"},
			InputMultiJoin2: cfg.InputStreamConfig{ID: inputMultiJoin2Id, Name: "InputMultiJoin2", IdService: operatorServiceId, IdEndpoint: endpointMultiJoin2Id, ValueType: "Message"},
			KeyByMultiJoin2: cfg.KeyByStreamConfig{ID: keyByMultiJoin2Id, Name: "KeyByMultiJoin2", IdService: operatorServiceId, IdSource: inputMultiJoin2Id, KeyType: "string", ValueType: "Message"},
			InputMultiJoin3: cfg.InputStreamConfig{ID: inputMultiJoin3Id, Name: "InputMultiJoin3", IdService: operatorServiceId, IdEndpoint: endpointMultiJoin3Id, ValueType: "Message"},
			KeyByMultiJoin3: cfg.KeyByStreamConfig{ID: keyByMultiJoin3Id, Name: "KeyByMultiJoin3", IdService: operatorServiceId, IdSource: inputMultiJoin3Id, KeyType: "string", ValueType: "Message"},
			MultiJoinStream: cfg.MultiJoinStreamConfig{ID: multiJoinStreamId, Name: "MultiJoinStream", IdService: operatorServiceId, IdSource: keyByMultiJoin1Id, IdSources: []int{keyByMultiJoin1Id, keyByMultiJoin2Id, keyByMultiJoin3Id}, JoinStorage: api.JoinStorageTypeHashMap},
			SinkMultiJoin:   cfg.SinkStreamConfig{ID: sinkMultiJoinId, Name: "SinkMultiJoin", IdService: operatorServiceId, IdSource: multiJoinStreamId, IdEndpoint: sinkEndpointMultiJoinId},

			// HttpResult pipeline (HTTP source hasResult=true path)
			InputHttpResult: cfg.InputStreamConfig{ID: inputHttpResultId, Name: "InputHttpResult", IdService: operatorServiceId, IdEndpoint: endpointHttpResultId, ValueType: "Message"},
			MapHttpResult:   cfg.MapStreamConfig{ID: mapHttpResultId, Name: "MapHttpResult", IdService: operatorServiceId, IdSource: inputHttpResultId},
		},
		DataConnectors: struct {
			HttpServer   cfg.HttpDataConnectorConfig   `yaml:"httpServer" mapstructure:"httpServer"`
			CustomSink   cfg.CustomDataConnectorConfig `yaml:"customSink" mapstructure:"customSink"`
			LocalSource  cfg.CustomDataConnectorConfig `yaml:"localSource" mapstructure:"localSource"`
			HttpSink     cfg.HttpDataConnectorConfig   `yaml:"httpSink" mapstructure:"httpSink"`
		}{
			HttpServer: cfg.HttpDataConnectorConfig{
				ID:                   httpServerConnId,
				Name:                 "HttpServer",
				Implementation:       api.DataConnectorImplementationNetHTTP,
				Host:                 "localhost",
				Port:                 8080,
				UseDedicatedListener: false,
			},
			CustomSink: cfg.CustomDataConnectorConfig{
				ID:             CustomSinkConnId,
				Name:           "CustomSink",
				Implementation: api.DataConnectorImplementationFunction,
			},
			LocalSource: cfg.CustomDataConnectorConfig{
				ID:             localSourceConnId,
				Name:           "LocalSource",
				Implementation: api.DataConnectorImplementationFunction,
			},
			HttpSink: cfg.HttpDataConnectorConfig{
				ID:                   HttpSinkConnId,
				Name:                 "HttpSink",
				Implementation:       api.DataConnectorImplementationNetHTTP,
				Host:                 "localhost",
				Port:                 0,
				UseDedicatedListener: false,
			},
		},
		Endpoints: struct {
			Map             cfg.HttpEndpointConfig   `yaml:"map" mapstructure:"map"`
			Filter          cfg.HttpEndpointConfig   `yaml:"filter" mapstructure:"filter"`
			FlatMap         cfg.HttpEndpointConfig   `yaml:"flatMap" mapstructure:"flatMap"`
			FlatMapIterable cfg.HttpEndpointConfig   `yaml:"flatMapIterable" mapstructure:"flatMapIterable"`
			Process         cfg.HttpEndpointConfig   `yaml:"process" mapstructure:"process"`
			Split           cfg.HttpEndpointConfig   `yaml:"split" mapstructure:"split"`
			Merge1          cfg.HttpEndpointConfig   `yaml:"merge1" mapstructure:"merge1"`
			Merge2          cfg.HttpEndpointConfig   `yaml:"merge2" mapstructure:"merge2"`
			Case            cfg.HttpEndpointConfig   `yaml:"case" mapstructure:"case"`
			Delay           cfg.HttpEndpointConfig   `yaml:"delay" mapstructure:"delay"`
			KeyBy           cfg.HttpEndpointConfig   `yaml:"keyBy" mapstructure:"keyBy"`
			JoinLeft        cfg.HttpEndpointConfig   `yaml:"joinLeft" mapstructure:"joinLeft"`
			JoinRight       cfg.HttpEndpointConfig   `yaml:"joinRight" mapstructure:"joinRight"`
			JoinLLeft       cfg.HttpEndpointConfig   `yaml:"joinLLeft" mapstructure:"joinLLeft"`
			JoinRLeft       cfg.HttpEndpointConfig   `yaml:"joinRLeft" mapstructure:"joinRLeft"`
			JoinLRight      cfg.HttpEndpointConfig   `yaml:"joinLRight" mapstructure:"joinLRight"`
			JoinRRight      cfg.HttpEndpointConfig   `yaml:"joinRRight" mapstructure:"joinRRight"`
			JoinLOuter      cfg.HttpEndpointConfig   `yaml:"joinLOuter" mapstructure:"joinLOuter"`
			JoinROuter      cfg.HttpEndpointConfig   `yaml:"joinROuter" mapstructure:"joinROuter"`
			MultiJoin1      cfg.HttpEndpointConfig   `yaml:"multiJoin1" mapstructure:"multiJoin1"`
			MultiJoin2      cfg.HttpEndpointConfig   `yaml:"multiJoin2" mapstructure:"multiJoin2"`
			MultiJoin3      cfg.HttpEndpointConfig   `yaml:"multiJoin3" mapstructure:"multiJoin3"`

			SinkMap             cfg.CustomEndpointConfig `yaml:"sinkMap" mapstructure:"sinkMap"`
			SinkFilter          cfg.CustomEndpointConfig `yaml:"sinkFilter" mapstructure:"sinkFilter"`
			SinkFlatMap         cfg.CustomEndpointConfig `yaml:"sinkFlatMap" mapstructure:"sinkFlatMap"`
			SinkFlatMapIterable cfg.CustomEndpointConfig `yaml:"sinkFlatMapIterable" mapstructure:"sinkFlatMapIterable"`
			SinkProcess         cfg.CustomEndpointConfig `yaml:"sinkProcess" mapstructure:"sinkProcess"`
			SinkProcessError    cfg.CustomEndpointConfig `yaml:"sinkProcessError" mapstructure:"sinkProcessError"`
			SinkSplit1          cfg.CustomEndpointConfig `yaml:"sinkSplit1" mapstructure:"sinkSplit1"`
			SinkSplit2          cfg.CustomEndpointConfig `yaml:"sinkSplit2" mapstructure:"sinkSplit2"`
			SinkMerge           cfg.CustomEndpointConfig `yaml:"sinkMerge" mapstructure:"sinkMerge"`
			SinkCaseA           cfg.CustomEndpointConfig `yaml:"sinkCaseA" mapstructure:"sinkCaseA"`
			SinkCaseB           cfg.CustomEndpointConfig `yaml:"sinkCaseB" mapstructure:"sinkCaseB"`
			SinkDelay           cfg.CustomEndpointConfig `yaml:"sinkDelay" mapstructure:"sinkDelay"`
			SinkKeyBy           cfg.CustomEndpointConfig `yaml:"sinkKeyBy" mapstructure:"sinkKeyBy"`
			SinkJoin            cfg.CustomEndpointConfig `yaml:"sinkJoin" mapstructure:"sinkJoin"`
			SinkJoinLeft        cfg.CustomEndpointConfig `yaml:"sinkJoinLeft" mapstructure:"sinkJoinLeft"`
			SinkJoinRight       cfg.CustomEndpointConfig `yaml:"sinkJoinRight" mapstructure:"sinkJoinRight"`
			SinkJoinOuter       cfg.CustomEndpointConfig `yaml:"sinkJoinOuter" mapstructure:"sinkJoinOuter"`
			LocalSource             cfg.CustomEndpointConfig `yaml:"localSource" mapstructure:"localSource"`
			SinkLocalSource         cfg.CustomEndpointConfig `yaml:"sinkLocalSource" mapstructure:"sinkLocalSource"`
			LocalSourceResult       cfg.CustomEndpointConfig `yaml:"localSourceResult" mapstructure:"localSourceResult"`
			HttpSinkIn              cfg.HttpEndpointConfig   `yaml:"httpSinkIn" mapstructure:"httpSinkIn"`
			SinkHttpSinkOut         cfg.HttpEndpointConfig   `yaml:"sinkHttpSinkOut" mapstructure:"sinkHttpSinkOut"`
			SinkMultiJoin           cfg.CustomEndpointConfig `yaml:"sinkMultiJoin" mapstructure:"sinkMultiJoin"`
			HttpResult              cfg.HttpEndpointConfig   `yaml:"httpResult" mapstructure:"httpResult"`
		}{
			Map:             cfg.HttpEndpointConfig{ID: endpointMapId, Name: "Map", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/map"},
			Filter:          cfg.HttpEndpointConfig{ID: endpointFilterId, Name: "Filter", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/filter"},
			FlatMap:         cfg.HttpEndpointConfig{ID: endpointFlatMapId, Name: "FlatMap", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/flatmap"},
			FlatMapIterable: cfg.HttpEndpointConfig{ID: endpointFlatMapIterableId, Name: "FlatMapIterable", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/flatmap-iterable"},
			Process:         cfg.HttpEndpointConfig{ID: endpointProcessId, Name: "Process", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/process"},
			Split:           cfg.HttpEndpointConfig{ID: endpointSplitId, Name: "Split", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/split"},
			Merge1:          cfg.HttpEndpointConfig{ID: endpointMerge1Id, Name: "Merge1", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/merge1"},
			Merge2:          cfg.HttpEndpointConfig{ID: endpointMerge2Id, Name: "Merge2", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/merge2"},
			Case:            cfg.HttpEndpointConfig{ID: endpointCaseId, Name: "Case", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/case"},
			Delay:           cfg.HttpEndpointConfig{ID: endpointDelayId, Name: "Delay", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/delay"},
			KeyBy:           cfg.HttpEndpointConfig{ID: endpointKeyById, Name: "KeyBy", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/keyby"},
			JoinLeft:        cfg.HttpEndpointConfig{ID: endpointJoinLeftId, Name: "JoinLeft", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-left"},
			JoinRight:       cfg.HttpEndpointConfig{ID: endpointJoinRightId, Name: "JoinRight", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-right"},
			JoinLLeft:       cfg.HttpEndpointConfig{ID: endpointJoinLLeftId, Name: "JoinLLeft", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-left-l"},
			JoinRLeft:       cfg.HttpEndpointConfig{ID: endpointJoinRLeftId, Name: "JoinRLeft", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-right-l"},
			JoinLRight:      cfg.HttpEndpointConfig{ID: endpointJoinLRightId, Name: "JoinLRight", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-left-r"},
			JoinRRight:      cfg.HttpEndpointConfig{ID: endpointJoinRRightId, Name: "JoinRRight", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-right-r"},
			JoinLOuter:      cfg.HttpEndpointConfig{ID: endpointJoinLOuterId, Name: "JoinLOuter", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-left-o"},
			JoinROuter:      cfg.HttpEndpointConfig{ID: endpointJoinROuterId, Name: "JoinROuter", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/join-right-o"},
			MultiJoin1:      cfg.HttpEndpointConfig{ID: endpointMultiJoin1Id, Name: "MultiJoin1", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/multijoin1"},
			MultiJoin2:      cfg.HttpEndpointConfig{ID: endpointMultiJoin2Id, Name: "MultiJoin2", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/multijoin2"},
			MultiJoin3:      cfg.HttpEndpointConfig{ID: endpointMultiJoin3Id, Name: "MultiJoin3", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/multijoin3"},

			SinkMap:             cfg.CustomEndpointConfig{ID: sinkEndpointMapId, Name: "SinkMap", IdDataConnector: CustomSinkConnId, FunctionName: "SinkMap"},
			SinkFilter:          cfg.CustomEndpointConfig{ID: sinkEndpointFilterId, Name: "SinkFilter", IdDataConnector: CustomSinkConnId, FunctionName: "SinkFilter"},
			SinkFlatMap:         cfg.CustomEndpointConfig{ID: sinkEndpointFlatMapId, Name: "SinkFlatMap", IdDataConnector: CustomSinkConnId, FunctionName: "SinkFlatMap"},
			SinkFlatMapIterable: cfg.CustomEndpointConfig{ID: sinkEndpointFlatMapIterableId, Name: "SinkFlatMapIterable", IdDataConnector: CustomSinkConnId, FunctionName: "SinkFlatMapIterable"},
			SinkProcess:         cfg.CustomEndpointConfig{ID: sinkEndpointProcessId, Name: "SinkProcess", IdDataConnector: CustomSinkConnId, FunctionName: "SinkProcess"},
			SinkProcessError:    cfg.CustomEndpointConfig{ID: sinkEndpointProcessErrorId, Name: "SinkProcessError", IdDataConnector: CustomSinkConnId, FunctionName: "SinkProcessError"},
			SinkSplit1:          cfg.CustomEndpointConfig{ID: sinkEndpointSplit1Id, Name: "SinkSplit1", IdDataConnector: CustomSinkConnId, FunctionName: "SinkSplit1"},
			SinkSplit2:          cfg.CustomEndpointConfig{ID: sinkEndpointSplit2Id, Name: "SinkSplit2", IdDataConnector: CustomSinkConnId, FunctionName: "SinkSplit2"},
			SinkMerge:           cfg.CustomEndpointConfig{ID: sinkEndpointMergeId, Name: "SinkMerge", IdDataConnector: CustomSinkConnId, FunctionName: "SinkMerge"},
			SinkCaseA:           cfg.CustomEndpointConfig{ID: sinkEndpointCaseAId, Name: "SinkCaseA", IdDataConnector: CustomSinkConnId, FunctionName: "SinkCaseA"},
			SinkCaseB:           cfg.CustomEndpointConfig{ID: sinkEndpointCaseBId, Name: "SinkCaseB", IdDataConnector: CustomSinkConnId, FunctionName: "SinkCaseB"},
			SinkDelay:           cfg.CustomEndpointConfig{ID: sinkEndpointDelayId, Name: "SinkDelay", IdDataConnector: CustomSinkConnId, FunctionName: "SinkDelay"},
			SinkKeyBy:           cfg.CustomEndpointConfig{ID: sinkEndpointKeyById, Name: "SinkKeyBy", IdDataConnector: CustomSinkConnId, FunctionName: "SinkKeyBy"},
			SinkJoin:            cfg.CustomEndpointConfig{ID: sinkEndpointJoinId, Name: "SinkJoin", IdDataConnector: CustomSinkConnId, FunctionName: "SinkJoin"},
			SinkJoinLeft:        cfg.CustomEndpointConfig{ID: sinkEndpointJoinLeftId, Name: "SinkJoinLeft", IdDataConnector: CustomSinkConnId, FunctionName: "SinkJoinLeft"},
			SinkJoinRight:       cfg.CustomEndpointConfig{ID: sinkEndpointJoinRightId, Name: "SinkJoinRight", IdDataConnector: CustomSinkConnId, FunctionName: "SinkJoinRight"},
			SinkJoinOuter:       cfg.CustomEndpointConfig{ID: sinkEndpointJoinOuterId, Name: "SinkJoinOuter", IdDataConnector: CustomSinkConnId, FunctionName: "SinkJoinOuter"},
			LocalSource:         cfg.CustomEndpointConfig{ID: endpointLocalSourceId, Name: "LocalSource", IdDataConnector: localSourceConnId, FunctionName: "LocalSource"},
			SinkLocalSource:     cfg.CustomEndpointConfig{ID: sinkEndpointLocalSourceId, Name: "SinkLocalSource", IdDataConnector: CustomSinkConnId, FunctionName: "SinkLocalSource"},
			LocalSourceResult:   cfg.CustomEndpointConfig{ID: endpointLocalSourceResultId, Name: "LocalSourceResult", IdDataConnector: localSourceConnId, FunctionName: "LocalSourceResult"},
			HttpSinkIn:          cfg.HttpEndpointConfig{ID: endpointHttpSinkInId, Name: "HttpSinkIn", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/http-sink-in"},
			SinkHttpSinkOut:     cfg.HttpEndpointConfig{ID: sinkEndpointHttpSinkOutId, Name: "SinkHttpSinkOut", IdDataConnector: HttpSinkConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/sink-out"},
			SinkMultiJoin:       cfg.CustomEndpointConfig{ID: sinkEndpointMultiJoinId, Name: "SinkMultiJoin", IdDataConnector: CustomSinkConnId, FunctionName: "SinkMultiJoin"},
			HttpResult:          cfg.HttpEndpointConfig{ID: endpointHttpResultId, Name: "HttpResult", IdDataConnector: httpServerConnId, HttpMethodType: api.HTTPMethodTypePOST, Path: "/http-result"},
		},
	}
}
