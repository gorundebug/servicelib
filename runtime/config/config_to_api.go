/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import "github.com/gorundebug/servicelib/api"

// ConfigToStreamApp reconstructs an api.StreamApp graph from a runtime Config.
func ConfigToStreamApp(config Config) *api.StreamApp {
	app := &api.StreamApp{}

	for _, svc := range config.GetServices() {
		app.Services = append(app.Services, ServiceConfigToAPI(svc))
	}
	for _, sc := range config.GetStreams() {
		app.Streams = append(app.Streams, StreamConfigToAPI(sc))
	}
	for _, dc := range config.GetDataConnectors() {
		app.DataConnectors = append(app.DataConnectors, DataConnectorConfigToAPI(dc))
	}
	for _, ep := range config.GetEndpoints() {
		app.Endpoints = append(app.Endpoints, EndpointConfigToAPI(ep))
	}
	for _, p := range config.GetPools() {
		pool := api.Pool{
			Name:           p.Name,
			ExecutorsCount: p.ExecutorsCount,
		}
		if p.QueueCapacity > 0 {
			pool.QueueCapacity = &p.QueueCapacity
		}
		app.Pools = append(app.Pools, pool)
	}
	for _, l := range config.GetLinks() {
		app.Links = append(app.Links, LinkConfigToAPI(l))
	}
	for _, t := range config.GetTypes() {
		app.Types = append(app.Types, typeConfigToAPI(t))
	}
	if modules := config.GetModules(); len(modules) > 0 {
		apiModules := make([]api.Module, 0, len(modules))
		for _, m := range modules {
			apiModules = append(apiModules, api.Module{
				Name:       m.Name,
				ModulePath: m.Path,
			})
		}
		app.Modules = &apiModules
	}

	return app
}

func ServiceConfigToAPI(s *ServiceConfig) api.Service {
	svc := api.Service{
		Id:                     s.ID,
		Name:                   s.Name,
		Color:                  s.Color,
		DefaultCallSemantics:   callSemanticsFromGroup(s.DefaultCallSemantics),
		DefaultGrpcTimeout:     s.DefaultGrpcTimeout,
		Environment:            s.Environment,
		GrpcHost:               s.GrpcHost,
		GrpcPort:               s.GrpcPort,
		HttpHost:               s.HttpHost,
		HttpPort:               s.HttpPort,
		MetricsHandler:         s.MetricsHandler,
		StartupHandler:         s.StartupHandler,
		ReadinessHandler:       s.ReadinessHandler,
		LivenessHandler:        s.LivenessHandler,
		KubernetesWorkloadType: s.KubernetesWorkloadType,
		ModulePath:             s.ModulePath,
		ProgrammingLanguage:    api.ProgrammingLanguageGoLang,
		ShutdownTimeout:        s.ShutdownTimeout,
		StatusHandler:          s.StatusHandler,
	}
	if s.GolangVersion != "" {
		v := s.GolangVersion
		svc.GolangVersion = &v
	}
	if s.LogLevel != "" {
		ll := s.LogLevel
		svc.LogLevel = &ll
	}
	return svc
}

func callSemanticsFromGroup(g *CallSemanticsGroup) api.CallSemantics {
	if g == nil {
		return api.CallSemanticsInherited
	}
	cs := g.Get()
	if cs == nil {
		return api.CallSemanticsInherited
	}
	return cs.GetType()
}

func StreamConfigToAPI(sc StreamConfig) api.Stream {
	s := api.Stream{
		Id:        sc.GetID(),
		Name:      sc.GetName(),
		Type:      sc.GetType(),
		IdService: sc.GetIdService(),
		IdSource:  sc.GetIdSource(),
		XPos:      float32(sc.GetXPos()),
		YPos:      float32(sc.GetYPos()),
	}
	if p := sc.GetPipeline(); p != "" {
		s.Pipeline = &p
	}
	if sources := sc.GetIdSources(); len(sources) > 0 {
		cp := make([]int, len(sources))
		copy(cp, sources)
		s.IdSources = &cp
	}

	switch c := sc.(type) {
	case *InputStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.IdEndpoint = intOptPtr(c.IdEndpoint)
	case *MapStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
	case *FilterStreamConfig:
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
	case *JoinStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
		if c.JoinType != 0 {
			jt := c.JoinType
			s.JoinType = &jt
		}
		if c.JoinStorage != 0 {
			js := c.JoinStorage
			s.JoinStorage = &js
		}
		s.Ttl = intOptPtr(c.Ttl)
		s.RenewTTL = boolOptPtr(c.RenewTTL)
	case *MultiJoinStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
		if c.JoinStorage != 0 {
			js := c.JoinStorage
			s.JoinStorage = &js
		}
		s.Ttl = intOptPtr(c.Ttl)
		s.RenewTTL = boolOptPtr(c.RenewTTL)
	case *ProcessStreamConfig:
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
		if c.Pattern != 0 {
			p := c.Pattern
			s.Pattern = &p
		}
	case *DelayStreamConfig:
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
		s.Duration = intOptPtr(c.Duration)
	case *FlatMapStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
	case *FlatMapIterableStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
	case *KeyByStreamConfig:
		s.KeyType = strOptPtr(c.KeyType)
		s.ValueType = strOptPtr(c.ValueType)
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
	case *CaseStreamConfig:
		s.FunctionName = strOptPtr(c.FunctionName)
		s.FunctionPackage = strOptPtr(c.FunctionPackage)
		s.FunctionModule = strOptPtr(c.FunctionModule)
		s.FunctionDescription = strOptPtr(c.FunctionDescription)
		s.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		s.PublicFunction = boolOptPtr(c.PublicFunction)
	case *WhenStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
	case *SinkStreamConfig:
		s.ValueType = strOptPtr(c.ValueType)
		s.IdEndpoint = intOptPtr(c.IdEndpoint)
	}

	return s
}

func DataConnectorConfigToAPI(dc DataConnectorConfig) api.DataConnector {
	implementation := dc.GetImplementation()
	d := api.DataConnector{
		Id:   dc.GetID(),
		Name: dc.GetName(),
		Type: dc.GetType(),
	}
	if d.Type == api.DataConnectorTypeCustom {
		d.Implementation = &implementation
	} else {
		d.GoImplementation = &implementation
	}
	switch c := dc.(type) {
	case *HttpDataConnectorConfig:
		d.Host = strOptPtr(c.Host)
		d.Module = strOptPtr(c.Module)
		d.Port = intOptPtr(c.Port)
		d.UseDedicatedListener = boolOptPtr(c.UseDedicatedListener)
	case *GrpcDataConnectorConfig:
		d.Address = strOptPtr(c.Address)
		d.Module = strOptPtr(c.Module)
		if c.ConnectionsCount > 0 {
			connectionsCount := c.ConnectionsCount
			d.ConnectionsCount = &connectionsCount
		}
	case *KafkaDataConnectorConfig:
		d.Brokers = strOptPtr(c.Brokers)
		d.Version = strOptPtr(c.Version)
		if c.DialTimeout != 0 {
			dt := c.DialTimeout
			d.DialTimeout = &dt
		}
		d.UsePartitioner = boolOptPtr(c.UsePartitioner)
		d.Async = boolOptPtr(c.Async)
		d.SecurityProtocol = &c.SecurityProtocol
		d.SaslMechanism = &c.SaslMechanism
		d.Username = strOptPtr(c.Username)
		d.Password = strOptPtr(c.Password)
	case *TemporalDataConnectorConfig:
		d.Address = strOptPtr(c.Address)
		d.Namespace = strOptPtr(c.Namespace)
		d.Identity = strOptPtr(c.Identity)
		d.ApiKey = strOptPtr(c.APIKey)
		d.TlsEnabled = boolOptPtr(c.TLSEnabled)
		d.TlsServerName = strOptPtr(c.TLSServerName)
		d.TlsCaFile = strOptPtr(c.TLSCAFile)
		d.TlsCertFile = strOptPtr(c.TLSCertFile)
		d.TlsKeyFile = strOptPtr(c.TLSKeyFile)
		d.MaxConcurrentActivities = intOptPtr(c.MaxConcurrentActivities)
		d.MaxConcurrentWorkflows = intOptPtr(c.MaxConcurrentWorkflows)
	}
	return d
}

func EndpointConfigToAPI(ep EndpointConfig) api.Endpoint {
	e := api.Endpoint{
		Id:              ep.GetID(),
		Name:            ep.GetName(),
		IdDataConnector: ep.GetIdDataConnector(),
	}
	switch c := ep.(type) {
	case *HttpEndpointConfig:
		if c.HttpMethodType != "" {
			m := c.HttpMethodType
			e.HttpMethodType = &m
		}
		e.Path = strOptPtr(c.Path)
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
	case *GrpcEndpointConfig:
		if c.GrpcMethodType != 0 {
			m := c.GrpcMethodType
			e.GrpcMethodType = &m
		}
		e.MethodName = strOptPtr(c.MethodName)
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
	case *KafkaEndpointConfig:
		e.Enabled = boolOptPtr(c.Enabled)
		e.Topic = strOptPtr(c.Topic)
		e.ConsumerGroup = strOptPtr(c.ConsumerGroup)
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
		e.CreateTopic = boolOptPtr(c.CreateTopic)
		e.Partitions = intOptPtr(c.Partitions)
		e.ReplicationFactor = intOptPtr(c.ReplicationFactor)
	case *CronEndpointConfig:
		e.Enabled = boolOptPtr(c.Enabled)
		e.Schedule = strOptPtr(c.Schedule)
		e.Timezone = strOptPtr(c.Timezone)
		if c.OverlapPolicy != "" {
			p := c.OverlapPolicy
			e.OverlapPolicy = &p
		}
		if c.MissedRunPolicy != "" {
			p := c.MissedRunPolicy
			e.MissedRunPolicy = &p
		}
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
	case *TemporalEndpointConfig:
		e.Enabled = boolOptPtr(c.Enabled)
		e.TaskQueue = strOptPtr(c.TaskQueue)
		e.Schedule = strOptPtr(c.Schedule)
		e.ScheduleId = strOptPtr(c.ScheduleID)
		e.Timezone = strOptPtr(c.Timezone)
		if c.OverlapPolicy != "" {
			p := c.OverlapPolicy
			e.OverlapPolicy = &p
		}
		if c.MissedRunPolicy != "" {
			p := c.MissedRunPolicy
			e.MissedRunPolicy = &p
		}
		e.WorkflowExecutionTimeout = intOptPtr(c.WorkflowExecutionTimeout)
		e.ActivityStartToCloseTimeout = intOptPtr(c.ActivityStartToCloseTimeout)
		e.ActivityHeartbeatTimeout = intOptPtr(c.ActivityHeartbeatTimeout)
		e.MaximumAttempts = intOptPtr(c.MaximumAttempts)
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
	case *CustomEndpointConfig:
		e.FunctionName = strOptPtr(c.FunctionName)
		e.FunctionPackage = strOptPtr(c.FunctionPackage)
		e.FunctionModule = strOptPtr(c.FunctionModule)
		e.FunctionDescription = strOptPtr(c.FunctionDescription)
		e.FunctionInitializerGroup = strOptPtr(c.FunctionInitializerGroup)
		e.PublicFunction = boolOptPtr(c.PublicFunction)
	}
	return e
}

func LinkConfigToAPI(l *LinkConfig) api.Link {
	link := api.Link{
		From:          l.From,
		To:            l.To,
		CallSemantics: api.CallSemanticsInherited,
	}
	if l.CallSemantics != nil {
		if cs := l.CallSemantics.Get(); cs != nil {
			link.CallSemantics = cs.GetType()
		}
		switch {
		case l.CallSemantics.TaskPool != nil:
			link.PoolName = strOptPtr(l.CallSemantics.TaskPool.PoolName)
		case l.CallSemantics.PriorityTaskPool != nil:
			link.PoolName = strOptPtr(l.CallSemantics.PriorityTaskPool.PoolName)
			p := l.CallSemantics.PriorityTaskPool.Priority
			link.Priority = &p
		case l.CallSemantics.DurableCall != nil:
			durable := l.CallSemantics.DurableCall
			id := durable.IdDataConnector
			link.IdDataConnector = &id
			link.TaskQueue = strOptPtr(durable.TaskQueue)
			link.WorkflowExecutionTimeout = &durable.WorkflowExecutionTimeout
			link.ActivityStartToCloseTimeout = &durable.ActivityStartToCloseTimeout
			link.ActivityHeartbeatTimeout = &durable.ActivityHeartbeatTimeout
			link.MaximumAttempts = &durable.MaximumAttempts
		}
	}
	return link
}

func typeConfigToAPI(t *TypeConfig) api.Type {
	at := api.Type{
		Name: t.Name,
		Type: t.Type,
	}
	at.TypeDefinitionLang1 = strOptPtr(t.TypeDefinition)
	at.TypeImportLang1 = strOptPtr(t.TypeImport)
	at.ValueType = strOptPtr(t.ValueType)
	at.KeyType = strOptPtr(t.KeyType)
	at.Package = strOptPtr(t.Package)
	at.Module = strOptPtr(t.Module)
	if t.DefinitionFormat != 0 {
		df := t.DefinitionFormat
		at.DefinitionFormat = &df
	}
	at.PublicType = boolOptPtr(t.PublicType)
	at.TransferByValue = boolOptPtr(t.TransferByValue)
	at.UseAlias = boolOptPtr(t.UseAlias)
	return at
}

func strOptPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func intOptPtr(i int) *int {
	if i == 0 {
		return nil
	}
	return &i
}

func boolOptPtr(b bool) *bool {
	if !b {
		return nil
	}
	return &b
}
