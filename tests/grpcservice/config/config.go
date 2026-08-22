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
	grpcServiceId = iota + 1
)

const (
	// gRPC source pipelines (datasource/grpc acts as server)
	inputGrpcSrcUnaryId = iota + 1
	sinkGrpcSrcUnaryId

	inputGrpcSrcServerStreamId
	sinkGrpcSrcServerStreamId

	inputGrpcSrcClientStreamId
	sinkGrpcSrcClientStreamId

	inputGrpcSrcBidiStreamId
	sinkGrpcSrcBidiStreamId

	// gRPC sink pipelines (datasink/grpc acts as client)
	inputGrpcDstUnaryId
	sinkGrpcDstUnaryId

	inputGrpcDstServerStreamId
	sinkGrpcDstServerStreamId

	inputGrpcDstClientStreamId
	sinkGrpcDstClientStreamId

	inputGrpcDstBidiStreamId
	sinkGrpcDstBidiStreamId

	// gRPC source with result (hasResult=true path)
	inputGrpcSrcUnaryResultId
	mapGrpcSrcUnaryResultId
)

const (
	GrpcSourceConnId = iota + 1
	CustomSinkConnId
	LocalSourceConnId
	GrpcSinkConnId
)

const (
	// gRPC source endpoints
	endpointGrpcSrcUnaryId = iota + 1
	endpointGrpcSrcServerStreamId
	endpointGrpcSrcClientStreamId
	endpointGrpcSrcBidiStreamId

	// Custom sink endpoints (collectors for source pipelines)
	sinkEndpointGrpcSrcUnaryId
	sinkEndpointGrpcSrcServerStreamId
	sinkEndpointGrpcSrcClientStreamId
	sinkEndpointGrpcSrcBidiStreamId

	// Local source endpoints (feed values into sink pipelines)
	endpointLocalDstUnaryId
	endpointLocalDstServerStreamId
	endpointLocalDstClientStreamId
	endpointLocalDstBidiStreamId

	// gRPC sink endpoints
	endpointGrpcDstUnaryId
	endpointGrpcDstServerStreamId
	endpointGrpcDstClientStreamId
	endpointGrpcDstBidiStreamId

	// gRPC source with result endpoint
	endpointGrpcSrcUnaryResultId
)

type Config struct {
	Services struct {
		GrpcService cfg.ServiceConfig `yaml:"grpcService" mapstructure:"grpcService"`
	} `yaml:"services" mapstructure:"services"`

	Streams struct {
		// gRPC source pipelines
		InputGrpcSrcUnary cfg.InputStreamConfig `yaml:"inputGrpcSrcUnary" mapstructure:"inputGrpcSrcUnary"`
		SinkGrpcSrcUnary  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcUnary" mapstructure:"sinkGrpcSrcUnary"`

		InputGrpcSrcServerStream cfg.InputStreamConfig `yaml:"inputGrpcSrcServerStream" mapstructure:"inputGrpcSrcServerStream"`
		SinkGrpcSrcServerStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcServerStream" mapstructure:"sinkGrpcSrcServerStream"`

		InputGrpcSrcClientStream cfg.InputStreamConfig `yaml:"inputGrpcSrcClientStream" mapstructure:"inputGrpcSrcClientStream"`
		SinkGrpcSrcClientStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcClientStream" mapstructure:"sinkGrpcSrcClientStream"`

		InputGrpcSrcBidiStream cfg.InputStreamConfig `yaml:"inputGrpcSrcBidiStream" mapstructure:"inputGrpcSrcBidiStream"`
		SinkGrpcSrcBidiStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcBidiStream" mapstructure:"sinkGrpcSrcBidiStream"`

		// gRPC sink pipelines
		InputGrpcDstUnary cfg.InputStreamConfig `yaml:"inputGrpcDstUnary" mapstructure:"inputGrpcDstUnary"`
		SinkGrpcDstUnary  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstUnary" mapstructure:"sinkGrpcDstUnary"`

		InputGrpcDstServerStream cfg.InputStreamConfig `yaml:"inputGrpcDstServerStream" mapstructure:"inputGrpcDstServerStream"`
		SinkGrpcDstServerStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstServerStream" mapstructure:"sinkGrpcDstServerStream"`

		InputGrpcDstClientStream cfg.InputStreamConfig `yaml:"inputGrpcDstClientStream" mapstructure:"inputGrpcDstClientStream"`
		SinkGrpcDstClientStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstClientStream" mapstructure:"sinkGrpcDstClientStream"`

		InputGrpcDstBidiStream cfg.InputStreamConfig `yaml:"inputGrpcDstBidiStream" mapstructure:"inputGrpcDstBidiStream"`
		SinkGrpcDstBidiStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstBidiStream" mapstructure:"sinkGrpcDstBidiStream"`

		// gRPC source with result (hasResult=true path)
		InputGrpcSrcUnaryResult cfg.InputStreamConfig `yaml:"inputGrpcSrcUnaryResult" mapstructure:"inputGrpcSrcUnaryResult"`
		MapGrpcSrcUnaryResult   cfg.MapStreamConfig   `yaml:"mapGrpcSrcUnaryResult" mapstructure:"mapGrpcSrcUnaryResult"`
	} `yaml:"streams" mapstructure:"streams"`

	DataConnectors struct {
		GrpcSource  cfg.GrpcDataConnectorConfig   `yaml:"grpcSource" mapstructure:"grpcSource"`
		CustomSink  cfg.CustomDataConnectorConfig  `yaml:"customSink" mapstructure:"customSink"`
		LocalSource cfg.CustomDataConnectorConfig  `yaml:"localSource" mapstructure:"localSource"`
		GrpcSink    cfg.GrpcDataConnectorConfig    `yaml:"grpcSink" mapstructure:"grpcSink"`
	} `yaml:"dataConnectors" mapstructure:"dataConnectors"`

	Endpoints struct {
		// gRPC source endpoints
		GrpcSrcUnary        cfg.GrpcEndpointConfig   `yaml:"grpcSrcUnary" mapstructure:"grpcSrcUnary"`
		GrpcSrcServerStream cfg.GrpcEndpointConfig   `yaml:"grpcSrcServerStream" mapstructure:"grpcSrcServerStream"`
		GrpcSrcClientStream cfg.GrpcEndpointConfig   `yaml:"grpcSrcClientStream" mapstructure:"grpcSrcClientStream"`
		GrpcSrcBidiStream   cfg.GrpcEndpointConfig   `yaml:"grpcSrcBidiStream" mapstructure:"grpcSrcBidiStream"`

		// Custom sink endpoints
		SinkGrpcSrcUnary        cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcUnary" mapstructure:"sinkGrpcSrcUnary"`
		SinkGrpcSrcServerStream cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcServerStream" mapstructure:"sinkGrpcSrcServerStream"`
		SinkGrpcSrcClientStream cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcClientStream" mapstructure:"sinkGrpcSrcClientStream"`
		SinkGrpcSrcBidiStream   cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcBidiStream" mapstructure:"sinkGrpcSrcBidiStream"`

		// Local source endpoints
		LocalDstUnary        cfg.CustomEndpointConfig `yaml:"localDstUnary" mapstructure:"localDstUnary"`
		LocalDstServerStream cfg.CustomEndpointConfig `yaml:"localDstServerStream" mapstructure:"localDstServerStream"`
		LocalDstClientStream cfg.CustomEndpointConfig `yaml:"localDstClientStream" mapstructure:"localDstClientStream"`
		LocalDstBidiStream   cfg.CustomEndpointConfig `yaml:"localDstBidiStream" mapstructure:"localDstBidiStream"`

		// gRPC sink endpoints
		GrpcDstUnary        cfg.GrpcEndpointConfig `yaml:"grpcDstUnary" mapstructure:"grpcDstUnary"`
		GrpcDstServerStream cfg.GrpcEndpointConfig `yaml:"grpcDstServerStream" mapstructure:"grpcDstServerStream"`
		GrpcDstClientStream cfg.GrpcEndpointConfig `yaml:"grpcDstClientStream" mapstructure:"grpcDstClientStream"`
		GrpcDstBidiStream   cfg.GrpcEndpointConfig `yaml:"grpcDstBidiStream" mapstructure:"grpcDstBidiStream"`

		GrpcSrcUnaryResult cfg.GrpcEndpointConfig `yaml:"grpcSrcUnaryResult" mapstructure:"grpcSrcUnaryResult"`
	} `yaml:"endpoints" mapstructure:"endpoints"`

	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (c *Config) GetProperty(name string) interface{} { return c.Properties[name] }

func (c *Config) GetServices() []*cfg.ServiceConfig {
	return []*cfg.ServiceConfig{&c.Services.GrpcService}
}

func (c *Config) GetStreams() []cfg.StreamConfig {
	return []cfg.StreamConfig{
		&c.Streams.InputGrpcSrcUnary, &c.Streams.SinkGrpcSrcUnary,
		&c.Streams.InputGrpcSrcServerStream, &c.Streams.SinkGrpcSrcServerStream,
		&c.Streams.InputGrpcSrcClientStream, &c.Streams.SinkGrpcSrcClientStream,
		&c.Streams.InputGrpcSrcBidiStream, &c.Streams.SinkGrpcSrcBidiStream,
		&c.Streams.InputGrpcDstUnary, &c.Streams.SinkGrpcDstUnary,
		&c.Streams.InputGrpcDstServerStream, &c.Streams.SinkGrpcDstServerStream,
		&c.Streams.InputGrpcDstClientStream, &c.Streams.SinkGrpcDstClientStream,
		&c.Streams.InputGrpcDstBidiStream, &c.Streams.SinkGrpcDstBidiStream,
		&c.Streams.InputGrpcSrcUnaryResult, &c.Streams.MapGrpcSrcUnaryResult,
	}
}

func (c *Config) GetDataConnectors() []cfg.DataConnectorConfig {
	return []cfg.DataConnectorConfig{
		&c.DataConnectors.GrpcSource,
		&c.DataConnectors.CustomSink,
		&c.DataConnectors.LocalSource,
		&c.DataConnectors.GrpcSink,
	}
}

func (c *Config) GetEndpoints() []cfg.EndpointConfig {
	return []cfg.EndpointConfig{
		&c.Endpoints.GrpcSrcUnary, &c.Endpoints.GrpcSrcServerStream,
		&c.Endpoints.GrpcSrcClientStream, &c.Endpoints.GrpcSrcBidiStream,
		&c.Endpoints.SinkGrpcSrcUnary, &c.Endpoints.SinkGrpcSrcServerStream,
		&c.Endpoints.SinkGrpcSrcClientStream, &c.Endpoints.SinkGrpcSrcBidiStream,
		&c.Endpoints.LocalDstUnary, &c.Endpoints.LocalDstServerStream,
		&c.Endpoints.LocalDstClientStream, &c.Endpoints.LocalDstBidiStream,
		&c.Endpoints.GrpcDstUnary, &c.Endpoints.GrpcDstServerStream,
		&c.Endpoints.GrpcDstClientStream, &c.Endpoints.GrpcDstBidiStream,
		&c.Endpoints.GrpcSrcUnaryResult,
	}
}

func (c *Config) GetPools() []*cfg.PoolConfig    { return nil }
func (c *Config) GetLinks() []*cfg.LinkConfig     { return nil }
func (c *Config) GetModules() []*cfg.ModuleConfig { return nil }
func (c *Config) GetTypes() []*cfg.TypeConfig     { return nil }
func (c *Config) ApplyEnvironment() error         { return nil }

func MakeConfig() *Config {
	funcCall := &cfg.CallSemanticsGroup{FunctionCall: &cfg.FunctionCallSemanticsConfig{}}
	return &Config{
		Services: struct {
			GrpcService cfg.ServiceConfig `yaml:"grpcService" mapstructure:"grpcService"`
		}{
			GrpcService: cfg.ServiceConfig{
				ID:                   grpcServiceId,
				Name:                 "GrpcService",
				GrpcHost:             "localhost",
				GrpcPort:             9401,
				HttpHost:             "localhost",
				HttpPort:             9093,
				ShutdownTimeout:      30000,
				Environment:          "grpc_test",
				DefaultCallSemantics: funcCall,
			},
		},
		Streams: struct {
			InputGrpcSrcUnary cfg.InputStreamConfig `yaml:"inputGrpcSrcUnary" mapstructure:"inputGrpcSrcUnary"`
			SinkGrpcSrcUnary  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcUnary" mapstructure:"sinkGrpcSrcUnary"`

			InputGrpcSrcServerStream cfg.InputStreamConfig `yaml:"inputGrpcSrcServerStream" mapstructure:"inputGrpcSrcServerStream"`
			SinkGrpcSrcServerStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcServerStream" mapstructure:"sinkGrpcSrcServerStream"`

			InputGrpcSrcClientStream cfg.InputStreamConfig `yaml:"inputGrpcSrcClientStream" mapstructure:"inputGrpcSrcClientStream"`
			SinkGrpcSrcClientStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcClientStream" mapstructure:"sinkGrpcSrcClientStream"`

			InputGrpcSrcBidiStream cfg.InputStreamConfig `yaml:"inputGrpcSrcBidiStream" mapstructure:"inputGrpcSrcBidiStream"`
			SinkGrpcSrcBidiStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcSrcBidiStream" mapstructure:"sinkGrpcSrcBidiStream"`

			InputGrpcDstUnary cfg.InputStreamConfig `yaml:"inputGrpcDstUnary" mapstructure:"inputGrpcDstUnary"`
			SinkGrpcDstUnary  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstUnary" mapstructure:"sinkGrpcDstUnary"`

			InputGrpcDstServerStream cfg.InputStreamConfig `yaml:"inputGrpcDstServerStream" mapstructure:"inputGrpcDstServerStream"`
			SinkGrpcDstServerStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstServerStream" mapstructure:"sinkGrpcDstServerStream"`

			InputGrpcDstClientStream cfg.InputStreamConfig `yaml:"inputGrpcDstClientStream" mapstructure:"inputGrpcDstClientStream"`
			SinkGrpcDstClientStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstClientStream" mapstructure:"sinkGrpcDstClientStream"`

			InputGrpcDstBidiStream cfg.InputStreamConfig `yaml:"inputGrpcDstBidiStream" mapstructure:"inputGrpcDstBidiStream"`
			SinkGrpcDstBidiStream  cfg.SinkStreamConfig  `yaml:"sinkGrpcDstBidiStream" mapstructure:"sinkGrpcDstBidiStream"`

			// gRPC source with result (hasResult=true path)
			InputGrpcSrcUnaryResult cfg.InputStreamConfig `yaml:"inputGrpcSrcUnaryResult" mapstructure:"inputGrpcSrcUnaryResult"`
			MapGrpcSrcUnaryResult   cfg.MapStreamConfig   `yaml:"mapGrpcSrcUnaryResult" mapstructure:"mapGrpcSrcUnaryResult"`
		}{
			// gRPC source pipelines
			InputGrpcSrcUnary: cfg.InputStreamConfig{ID: inputGrpcSrcUnaryId, Name: "InputGrpcSrcUnary", IdService: grpcServiceId, IdEndpoint: endpointGrpcSrcUnaryId, ValueType: "Message"},
			SinkGrpcSrcUnary:  cfg.SinkStreamConfig{ID: sinkGrpcSrcUnaryId, Name: "SinkGrpcSrcUnary", IdService: grpcServiceId, IdSource: inputGrpcSrcUnaryId, IdEndpoint: sinkEndpointGrpcSrcUnaryId},

			InputGrpcSrcServerStream: cfg.InputStreamConfig{ID: inputGrpcSrcServerStreamId, Name: "InputGrpcSrcServerStream", IdService: grpcServiceId, IdEndpoint: endpointGrpcSrcServerStreamId, ValueType: "Message"},
			SinkGrpcSrcServerStream:  cfg.SinkStreamConfig{ID: sinkGrpcSrcServerStreamId, Name: "SinkGrpcSrcServerStream", IdService: grpcServiceId, IdSource: inputGrpcSrcServerStreamId, IdEndpoint: sinkEndpointGrpcSrcServerStreamId},

			InputGrpcSrcClientStream: cfg.InputStreamConfig{ID: inputGrpcSrcClientStreamId, Name: "InputGrpcSrcClientStream", IdService: grpcServiceId, IdEndpoint: endpointGrpcSrcClientStreamId, ValueType: "Message"},
			SinkGrpcSrcClientStream:  cfg.SinkStreamConfig{ID: sinkGrpcSrcClientStreamId, Name: "SinkGrpcSrcClientStream", IdService: grpcServiceId, IdSource: inputGrpcSrcClientStreamId, IdEndpoint: sinkEndpointGrpcSrcClientStreamId},

			InputGrpcSrcBidiStream: cfg.InputStreamConfig{ID: inputGrpcSrcBidiStreamId, Name: "InputGrpcSrcBidiStream", IdService: grpcServiceId, IdEndpoint: endpointGrpcSrcBidiStreamId, ValueType: "Message"},
			SinkGrpcSrcBidiStream:  cfg.SinkStreamConfig{ID: sinkGrpcSrcBidiStreamId, Name: "SinkGrpcSrcBidiStream", IdService: grpcServiceId, IdSource: inputGrpcSrcBidiStreamId, IdEndpoint: sinkEndpointGrpcSrcBidiStreamId},

			// gRPC sink pipelines
			InputGrpcDstUnary: cfg.InputStreamConfig{ID: inputGrpcDstUnaryId, Name: "InputGrpcDstUnary", IdService: grpcServiceId, IdEndpoint: endpointLocalDstUnaryId, ValueType: "Message"},
			SinkGrpcDstUnary:  cfg.SinkStreamConfig{ID: sinkGrpcDstUnaryId, Name: "SinkGrpcDstUnary", IdService: grpcServiceId, IdSource: inputGrpcDstUnaryId, IdEndpoint: endpointGrpcDstUnaryId},

			InputGrpcDstServerStream: cfg.InputStreamConfig{ID: inputGrpcDstServerStreamId, Name: "InputGrpcDstServerStream", IdService: grpcServiceId, IdEndpoint: endpointLocalDstServerStreamId, ValueType: "Message"},
			SinkGrpcDstServerStream:  cfg.SinkStreamConfig{ID: sinkGrpcDstServerStreamId, Name: "SinkGrpcDstServerStream", IdService: grpcServiceId, IdSource: inputGrpcDstServerStreamId, IdEndpoint: endpointGrpcDstServerStreamId},

			InputGrpcDstClientStream: cfg.InputStreamConfig{ID: inputGrpcDstClientStreamId, Name: "InputGrpcDstClientStream", IdService: grpcServiceId, IdEndpoint: endpointLocalDstClientStreamId, ValueType: "Message"},
			SinkGrpcDstClientStream:  cfg.SinkStreamConfig{ID: sinkGrpcDstClientStreamId, Name: "SinkGrpcDstClientStream", IdService: grpcServiceId, IdSource: inputGrpcDstClientStreamId, IdEndpoint: endpointGrpcDstClientStreamId},

			InputGrpcDstBidiStream: cfg.InputStreamConfig{ID: inputGrpcDstBidiStreamId, Name: "InputGrpcDstBidiStream", IdService: grpcServiceId, IdEndpoint: endpointLocalDstBidiStreamId, ValueType: "Message"},
			SinkGrpcDstBidiStream:  cfg.SinkStreamConfig{ID: sinkGrpcDstBidiStreamId, Name: "SinkGrpcDstBidiStream", IdService: grpcServiceId, IdSource: inputGrpcDstBidiStreamId, IdEndpoint: endpointGrpcDstBidiStreamId},

			InputGrpcSrcUnaryResult: cfg.InputStreamConfig{ID: inputGrpcSrcUnaryResultId, Name: "InputGrpcSrcUnaryResult", IdService: grpcServiceId, IdEndpoint: endpointGrpcSrcUnaryResultId, ValueType: "Message"},
			MapGrpcSrcUnaryResult:   cfg.MapStreamConfig{ID: mapGrpcSrcUnaryResultId, Name: "MapGrpcSrcUnaryResult", IdService: grpcServiceId, IdSource: inputGrpcSrcUnaryResultId},
		},
		DataConnectors: struct {
			GrpcSource  cfg.GrpcDataConnectorConfig   `yaml:"grpcSource" mapstructure:"grpcSource"`
			CustomSink  cfg.CustomDataConnectorConfig  `yaml:"customSink" mapstructure:"customSink"`
			LocalSource cfg.CustomDataConnectorConfig  `yaml:"localSource" mapstructure:"localSource"`
			GrpcSink    cfg.GrpcDataConnectorConfig    `yaml:"grpcSink" mapstructure:"grpcSink"`
		}{
			GrpcSource: cfg.GrpcDataConnectorConfig{
				ID:             GrpcSourceConnId,
				Name:           "GrpcSource",
				Implementation: api.DataConnectorImplementationGoogleGRPC,
			},
			CustomSink: cfg.CustomDataConnectorConfig{
				ID:             CustomSinkConnId,
				Name:           "CustomSink",
				Implementation: api.DataConnectorImplementationFunction,
			},
			LocalSource: cfg.CustomDataConnectorConfig{
				ID:             LocalSourceConnId,
				Name:           "LocalSource",
				Implementation: api.DataConnectorImplementationFunction,
			},
			GrpcSink: cfg.GrpcDataConnectorConfig{
				ID:             GrpcSinkConnId,
				Name:           "GrpcSink",
				Implementation: api.DataConnectorImplementationGoogleGRPC,
			},
		},
		Endpoints: struct {
			GrpcSrcUnary        cfg.GrpcEndpointConfig   `yaml:"grpcSrcUnary" mapstructure:"grpcSrcUnary"`
			GrpcSrcServerStream cfg.GrpcEndpointConfig   `yaml:"grpcSrcServerStream" mapstructure:"grpcSrcServerStream"`
			GrpcSrcClientStream cfg.GrpcEndpointConfig   `yaml:"grpcSrcClientStream" mapstructure:"grpcSrcClientStream"`
			GrpcSrcBidiStream   cfg.GrpcEndpointConfig   `yaml:"grpcSrcBidiStream" mapstructure:"grpcSrcBidiStream"`

			SinkGrpcSrcUnary        cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcUnary" mapstructure:"sinkGrpcSrcUnary"`
			SinkGrpcSrcServerStream cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcServerStream" mapstructure:"sinkGrpcSrcServerStream"`
			SinkGrpcSrcClientStream cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcClientStream" mapstructure:"sinkGrpcSrcClientStream"`
			SinkGrpcSrcBidiStream   cfg.CustomEndpointConfig `yaml:"sinkGrpcSrcBidiStream" mapstructure:"sinkGrpcSrcBidiStream"`

			LocalDstUnary        cfg.CustomEndpointConfig `yaml:"localDstUnary" mapstructure:"localDstUnary"`
			LocalDstServerStream cfg.CustomEndpointConfig `yaml:"localDstServerStream" mapstructure:"localDstServerStream"`
			LocalDstClientStream cfg.CustomEndpointConfig `yaml:"localDstClientStream" mapstructure:"localDstClientStream"`
			LocalDstBidiStream   cfg.CustomEndpointConfig `yaml:"localDstBidiStream" mapstructure:"localDstBidiStream"`

			GrpcDstUnary        cfg.GrpcEndpointConfig `yaml:"grpcDstUnary" mapstructure:"grpcDstUnary"`
			GrpcDstServerStream cfg.GrpcEndpointConfig `yaml:"grpcDstServerStream" mapstructure:"grpcDstServerStream"`
			GrpcDstClientStream cfg.GrpcEndpointConfig `yaml:"grpcDstClientStream" mapstructure:"grpcDstClientStream"`
			GrpcDstBidiStream   cfg.GrpcEndpointConfig `yaml:"grpcDstBidiStream" mapstructure:"grpcDstBidiStream"`

			GrpcSrcUnaryResult cfg.GrpcEndpointConfig `yaml:"grpcSrcUnaryResult" mapstructure:"grpcSrcUnaryResult"`
		}{
			GrpcSrcUnary:        cfg.GrpcEndpointConfig{ID: endpointGrpcSrcUnaryId, Name: "GrpcSrcUnary", IdDataConnector: GrpcSourceConnId, GrpcMethodType: api.GrpcMethodTypeNoStreaming, MethodName: "Unary"},
			GrpcSrcServerStream: cfg.GrpcEndpointConfig{ID: endpointGrpcSrcServerStreamId, Name: "GrpcSrcServerStream", IdDataConnector: GrpcSourceConnId, GrpcMethodType: api.GrpcMethodTypeServerStreaming, MethodName: "ServerStream"},
			GrpcSrcClientStream: cfg.GrpcEndpointConfig{ID: endpointGrpcSrcClientStreamId, Name: "GrpcSrcClientStream", IdDataConnector: GrpcSourceConnId, GrpcMethodType: api.GrpcMethodTypeClientStreaming, MethodName: "ClientStream"},
			GrpcSrcBidiStream:   cfg.GrpcEndpointConfig{ID: endpointGrpcSrcBidiStreamId, Name: "GrpcSrcBidiStream", IdDataConnector: GrpcSourceConnId, GrpcMethodType: api.GrpcMethodTypeBidirectionalStreaming, MethodName: "BidiStream"},

			SinkGrpcSrcUnary:        cfg.CustomEndpointConfig{ID: sinkEndpointGrpcSrcUnaryId, Name: "SinkGrpcSrcUnary", IdDataConnector: CustomSinkConnId, FunctionName: "SinkGrpcSrcUnary"},
			SinkGrpcSrcServerStream: cfg.CustomEndpointConfig{ID: sinkEndpointGrpcSrcServerStreamId, Name: "SinkGrpcSrcServerStream", IdDataConnector: CustomSinkConnId, FunctionName: "SinkGrpcSrcServerStream"},
			SinkGrpcSrcClientStream: cfg.CustomEndpointConfig{ID: sinkEndpointGrpcSrcClientStreamId, Name: "SinkGrpcSrcClientStream", IdDataConnector: CustomSinkConnId, FunctionName: "SinkGrpcSrcClientStream"},
			SinkGrpcSrcBidiStream:   cfg.CustomEndpointConfig{ID: sinkEndpointGrpcSrcBidiStreamId, Name: "SinkGrpcSrcBidiStream", IdDataConnector: CustomSinkConnId, FunctionName: "SinkGrpcSrcBidiStream"},

			LocalDstUnary:        cfg.CustomEndpointConfig{ID: endpointLocalDstUnaryId, Name: "LocalDstUnary", IdDataConnector: LocalSourceConnId, FunctionName: "LocalDstUnary"},
			LocalDstServerStream: cfg.CustomEndpointConfig{ID: endpointLocalDstServerStreamId, Name: "LocalDstServerStream", IdDataConnector: LocalSourceConnId, FunctionName: "LocalDstServerStream"},
			LocalDstClientStream: cfg.CustomEndpointConfig{ID: endpointLocalDstClientStreamId, Name: "LocalDstClientStream", IdDataConnector: LocalSourceConnId, FunctionName: "LocalDstClientStream"},
			LocalDstBidiStream:   cfg.CustomEndpointConfig{ID: endpointLocalDstBidiStreamId, Name: "LocalDstBidiStream", IdDataConnector: LocalSourceConnId, FunctionName: "LocalDstBidiStream"},

			GrpcDstUnary:        cfg.GrpcEndpointConfig{ID: endpointGrpcDstUnaryId, Name: "GrpcDstUnary", IdDataConnector: GrpcSinkConnId, GrpcMethodType: api.GrpcMethodTypeNoStreaming, MethodName: "Unary"},
			GrpcDstServerStream: cfg.GrpcEndpointConfig{ID: endpointGrpcDstServerStreamId, Name: "GrpcDstServerStream", IdDataConnector: GrpcSinkConnId, GrpcMethodType: api.GrpcMethodTypeServerStreaming, MethodName: "ServerStream"},
			GrpcDstClientStream: cfg.GrpcEndpointConfig{ID: endpointGrpcDstClientStreamId, Name: "GrpcDstClientStream", IdDataConnector: GrpcSinkConnId, GrpcMethodType: api.GrpcMethodTypeClientStreaming, MethodName: "ClientStream"},
			GrpcDstBidiStream:   cfg.GrpcEndpointConfig{ID: endpointGrpcDstBidiStreamId, Name: "GrpcDstBidiStream", IdDataConnector: GrpcSinkConnId, GrpcMethodType: api.GrpcMethodTypeBidirectionalStreaming, MethodName: "BidiStream"},

			GrpcSrcUnaryResult: cfg.GrpcEndpointConfig{ID: endpointGrpcSrcUnaryResultId, Name: "GrpcSrcUnaryResult", IdDataConnector: GrpcSourceConnId, GrpcMethodType: api.GrpcMethodTypeNoStreaming, MethodName: "UnaryResult"},
		},
	}
}
