package config

import (
	"github.com/gorundebug/servicelib/api"

	cfg "github.com/gorundebug/servicelib/runtime/config"
)

// ID constants
const (
	incomeServiceId = iota + 1
)

const (
	inputRequestId = iota + 1
	sinkId
	inputRequestId3
	sinkId4
	inputRequestSyncId
	sinkSyncId
)

const (
	httpServerConnId = iota + 1
	customSinkConnId
)

const (
	dataEndpointId = iota + 1
	dataEndpoint3Id
	dataEndpointSyncId
	sinkEndpointId
	sink4EndpointId
	sinkSyncEndpointId
)

const (
	DefaultTaskPoolName     = "TaskPool"
	DefaultPriorityPoolName = "PriorityTaskPool"
)

type Config struct {
	Services struct {
		IncomeService cfg.ServiceConfig `yaml:"incomeService" mapstructure:"incomeService"`
	} `yaml:"services" mapstructure:"services"`

	Streams struct {
		InputRequest     cfg.InputStreamConfig `yaml:"inputRequest" mapstructure:"inputRequest"`
		Sink             cfg.SinkStreamConfig  `yaml:"sink" mapstructure:"sink"`
		InputRequest3    cfg.InputStreamConfig `yaml:"inputRequest3" mapstructure:"inputRequest3"`
		Sink4            cfg.SinkStreamConfig  `yaml:"sink4" mapstructure:"sink4"`
		InputRequestSync cfg.InputStreamConfig `yaml:"inputRequestSync" mapstructure:"inputRequestSync"`
		SinkSync         cfg.SinkStreamConfig  `yaml:"sinkSync" mapstructure:"sinkSync"`
	} `yaml:"streams" mapstructure:"streams"`

	DataConnectors struct {
		HttpServer          cfg.HttpDataConnectorConfig   `yaml:"httpServer" mapstructure:"httpServer"`
		CustomSinkConnector cfg.CustomDataConnectorConfig `yaml:"customSinkConnector" mapstructure:"customSinkConnector"`
	} `yaml:"dataConnectors" mapstructure:"dataConnectors"`

	Endpoints struct {
		Data     cfg.HttpEndpointConfig   `yaml:"data" mapstructure:"data"`
		Data3    cfg.HttpEndpointConfig   `yaml:"data3" mapstructure:"data3"`
		DataSync cfg.HttpEndpointConfig   `yaml:"dataSync" mapstructure:"dataSync"`
		Sink     cfg.CustomEndpointConfig `yaml:"sink" mapstructure:"sink"`
		Sink4    cfg.CustomEndpointConfig `yaml:"sink4" mapstructure:"sink4"`
		SinkSync cfg.CustomEndpointConfig `yaml:"sinkSync" mapstructure:"sinkSync"`
	} `yaml:"endpoints" mapstructure:"endpoints"`

	Pools struct {
		TaskPool         cfg.PoolConfig `yaml:"taskPool" mapstructure:"taskPool"`
		PriorityTaskPool cfg.PoolConfig `yaml:"priorityTaskPool" mapstructure:"priorityTaskPool"`
	} `yaml:"pools" mapstructure:"pools"`

	Links struct {
		InputRequestToSink         cfg.LinkConfig `yaml:"inputRequestToSink" mapstructure:"inputRequestToSink"`
		InputRequest3ToSink4       cfg.LinkConfig `yaml:"inputRequest3ToSink4" mapstructure:"inputRequest3ToSink4"`
		InputRequestSyncToSinkSync cfg.LinkConfig `yaml:"inputRequestSyncToSinkSync" mapstructure:"inputRequestSyncToSinkSync"`
	} `yaml:"links" mapstructure:"links"`

	Modules struct {
		Model cfg.ModuleConfig `yaml:"model" mapstructure:"model"`
	} `yaml:"modules" mapstructure:"modules"`

	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (c *Config) GetProperty(name string) interface{} {
	return c.Properties[name]
}

func (c *Config) GetServices() []*cfg.ServiceConfig {
	return []*cfg.ServiceConfig{
		&c.Services.IncomeService,
	}
}

func (c *Config) GetStreams() []cfg.StreamConfig {
	return []cfg.StreamConfig{
		&c.Streams.InputRequest,
		&c.Streams.Sink,
		&c.Streams.InputRequest3,
		&c.Streams.Sink4,
		&c.Streams.InputRequestSync,
		&c.Streams.SinkSync,
	}
}

func (c *Config) GetDataConnectors() []cfg.DataConnectorConfig {
	return []cfg.DataConnectorConfig{
		&c.DataConnectors.HttpServer,
		&c.DataConnectors.CustomSinkConnector,
	}
}

func (c *Config) GetEndpoints() []cfg.EndpointConfig {
	return []cfg.EndpointConfig{
		&c.Endpoints.Data,
		&c.Endpoints.Data3,
		&c.Endpoints.DataSync,
		&c.Endpoints.Sink,
		&c.Endpoints.Sink4,
		&c.Endpoints.SinkSync,
	}
}

func (c *Config) GetPools() []*cfg.PoolConfig {
	return []*cfg.PoolConfig{
		&c.Pools.TaskPool, &c.Pools.PriorityTaskPool,
	}
}

func (c *Config) GetLinks() []*cfg.LinkConfig {
	return []*cfg.LinkConfig{
		&c.Links.InputRequestToSink,
		&c.Links.InputRequest3ToSink4,
		&c.Links.InputRequestSyncToSinkSync,
	}
}

func (c *Config) GetModules() []*cfg.ModuleConfig {
	return []*cfg.ModuleConfig{
		&c.Modules.Model,
	}
}

func (c *Config) ApplyEnvironment() error {
	return nil
}

func MakeConfig() *Config {
	return &Config{
		Services: struct {
			IncomeService cfg.ServiceConfig `yaml:"incomeService" mapstructure:"incomeService"`
		}{
			IncomeService: cfg.ServiceConfig{
				ID:              incomeServiceId,
				Name:            "IncomeService",
				Color:           "#D2E5FF",
				DelayExecutors:  1,
				GrpcHost:        "localhost",
				GrpcPort:        9201,
				HttpHost:        "localhost",
				HttpPort:        9091,
				MetricsHandler:  "metrics",
				ShutdownTimeout: 30000,
				StatusHandler:   "status",
				Environment:     "nethttp_test",
			},
		},
		Streams: struct {
			InputRequest     cfg.InputStreamConfig `yaml:"inputRequest" mapstructure:"inputRequest"`
			Sink             cfg.SinkStreamConfig  `yaml:"sink" mapstructure:"sink"`
			InputRequest3    cfg.InputStreamConfig `yaml:"inputRequest3" mapstructure:"inputRequest3"`
			Sink4            cfg.SinkStreamConfig  `yaml:"sink4" mapstructure:"sink4"`
			InputRequestSync cfg.InputStreamConfig `yaml:"inputRequestSync" mapstructure:"inputRequestSync"`
			SinkSync         cfg.SinkStreamConfig  `yaml:"sinkSync" mapstructure:"sinkSync"`
		}{
			InputRequest: cfg.InputStreamConfig{
				ID:         inputRequestId,
				Name:       "InputRequest",
				IdService:  incomeServiceId,
				XPos:       -1,
				YPos:       36,
				IdEndpoint: dataEndpointId,
				ValueType:  "RequestData",
			},
			Sink: cfg.SinkStreamConfig{
				ID:         sinkId,
				IdEndpoint: sinkEndpointId,
				Name:       "sink",
				IdService:  incomeServiceId,
				IdSource:   inputRequestId,
				XPos:       -1285,
				YPos:       -62,
				ValueType:  "error",
			},
			InputRequest3: cfg.InputStreamConfig{
				ID:         inputRequestId3,
				Name:       "InputRequest",
				IdService:  incomeServiceId,
				XPos:       -1,
				YPos:       36,
				IdEndpoint: dataEndpoint3Id,
				ValueType:  "RequestData",
			},
			Sink4: cfg.SinkStreamConfig{
				ID:         sinkId4,
				IdEndpoint: sink4EndpointId,
				Name:       "Sink4",
				IdService:  incomeServiceId,
				IdSource:   inputRequestId3,
				XPos:       -1285,
				YPos:       -62,
			},
			InputRequestSync: cfg.InputStreamConfig{
				ID:         inputRequestSyncId,
				Name:       "InputRequestSync",
				IdService:  incomeServiceId,
				XPos:       -1,
				YPos:       36,
				IdEndpoint: dataEndpointSyncId,
				ValueType:  "RequestData",
			},
			SinkSync: cfg.SinkStreamConfig{
				ID:         sinkSyncId,
				IdEndpoint: sinkSyncEndpointId,
				Name:       "SinkSync",
				IdService:  incomeServiceId,
				IdSource:   inputRequestSyncId,
				XPos:       -1285,
				YPos:       -62,
			},
		},

		DataConnectors: struct {
			HttpServer          cfg.HttpDataConnectorConfig   `yaml:"httpServer" mapstructure:"httpServer"`
			CustomSinkConnector cfg.CustomDataConnectorConfig `yaml:"customSinkConnector" mapstructure:"customSinkConnector"`
		}{
			HttpServer: cfg.HttpDataConnectorConfig{
				ID:                   httpServerConnId,
				Name:                 "HttpServer",
				Implementation:       api.DataConnectorImplementationNetHTTP,
				Host:                 "localhost",
				Port:                 8080,
				UseDedicatedListener: false,
			},
			CustomSinkConnector: cfg.CustomDataConnectorConfig{
				ID:             customSinkConnId,
				Name:           "CustomSink",
				Implementation: api.DataConnectorImplementationFunction,
			},
		},
		Endpoints: struct {
			Data     cfg.HttpEndpointConfig   `yaml:"data" mapstructure:"data"`
			Data3    cfg.HttpEndpointConfig   `yaml:"data3" mapstructure:"data3"`
			DataSync cfg.HttpEndpointConfig   `yaml:"dataSync" mapstructure:"dataSync"`
			Sink     cfg.CustomEndpointConfig `yaml:"sink" mapstructure:"sink"`
			Sink4    cfg.CustomEndpointConfig `yaml:"sink4" mapstructure:"sink4"`
			SinkSync cfg.CustomEndpointConfig `yaml:"sinkSync" mapstructure:"sinkSync"`
		}{
			Data: cfg.HttpEndpointConfig{
				ID:              dataEndpointId,
				Name:            "Data",
				IdDataConnector: httpServerConnId,
				HttpMethodType:  api.HTTPMethodTypePOST,
				Path:            "/data",
			},
			Data3: cfg.HttpEndpointConfig{
				ID:              dataEndpoint3Id,
				Name:            "Data3",
				IdDataConnector: httpServerConnId,
				HttpMethodType:  api.HTTPMethodTypePOST,
				Path:            "/data3",
			},
			DataSync: cfg.HttpEndpointConfig{
				ID:              dataEndpointSyncId,
				Name:            "DataSync",
				IdDataConnector: httpServerConnId,
				HttpMethodType:  api.HTTPMethodTypePOST,
				Path:            "/dataSync",
			},
			Sink: cfg.CustomEndpointConfig{
				ID:              sinkEndpointId,
				Name:            "Sink",
				IdDataConnector: customSinkConnId,
				FunctionName:    "SinkFunction",
			},
			Sink4: cfg.CustomEndpointConfig{
				ID:              sink4EndpointId,
				Name:            "Sink",
				IdDataConnector: customSinkConnId,
				FunctionName:    "SinkFunction",
			},
			SinkSync: cfg.CustomEndpointConfig{
				ID:              sinkSyncEndpointId,
				Name:            "Sink",
				IdDataConnector: customSinkConnId,
				FunctionName:    "SinkFunction",
			},
		},
		Pools: struct {
			TaskPool         cfg.PoolConfig `yaml:"taskPool" mapstructure:"taskPool"`
			PriorityTaskPool cfg.PoolConfig `yaml:"priorityTaskPool" mapstructure:"priorityTaskPool"`
		}{
			TaskPool: cfg.PoolConfig{
				Name:           DefaultTaskPoolName,
				ExecutorsCount: 8,
			},
			PriorityTaskPool: cfg.PoolConfig{
				Name:           DefaultPriorityPoolName,
				ExecutorsCount: 8,
			},
		},
		Links: struct {
			InputRequestToSink         cfg.LinkConfig `yaml:"inputRequestToSink" mapstructure:"inputRequestToSink"`
			InputRequest3ToSink4       cfg.LinkConfig `yaml:"inputRequest3ToSink4" mapstructure:"inputRequest3ToSink4"`
			InputRequestSyncToSinkSync cfg.LinkConfig `yaml:"inputRequestSyncToSinkSync" mapstructure:"inputRequestSyncToSinkSync"`
		}{
			InputRequestToSink: cfg.LinkConfig{
				From: inputRequestId,
				To:   sinkId,
				CallSemantics: &cfg.CallSemanticsGroup{
					PriorityTaskPool: &cfg.PriorityTaskPoolCallSemanticsConfig{
						PoolName: DefaultPriorityPoolName,
						Priority: 0,
					},
				},
			},
			InputRequest3ToSink4: cfg.LinkConfig{
				From: inputRequestId3,
				To:   sinkId4,
				CallSemantics: &cfg.CallSemanticsGroup{
					TaskPool: &cfg.TaskPoolCallSemanticsConfig{
						PoolName: DefaultTaskPoolName,
					},
				},
			},
			InputRequestSyncToSinkSync: cfg.LinkConfig{
				From: inputRequestSyncId,
				To:   sinkSyncId,
				CallSemantics: &cfg.CallSemanticsGroup{
					FunctionCall: &cfg.FunctionCallSemanticsConfig{},
				},
			},
		},
		Modules: struct {
			Model cfg.ModuleConfig `yaml:"model" mapstructure:"model"`
		}{
			Model: cfg.ModuleConfig{
				Name: "Model",
				Path: "service1.com/model",
			},
		},
	}
}
