/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/gorundebug/servicelib/api"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

var (
	appEnums             map[string]map[int]string
	englishTitleCaserCfg = cases.Title(language.English)
)

func init() {
	appEnums = buildReverseEnumMap(parseSpecEnums(api.Spec()))
}

func parseSpecEnums(data []byte) map[string]map[string]int {
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Type          string   `yaml:"type"`
				Enum          []any    `yaml:"enum"`
				XEnumVarnames []string `yaml:"x-enum-varnames"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		panic("serviceapi.yaml parse error: " + err.Error())
	}
	result := make(map[string]map[string]int)
	for schemaName, schema := range spec.Components.Schemas {
		if schema.Type != "integer" || len(schema.XEnumVarnames) == 0 {
			continue
		}
		m := make(map[string]int, len(schema.Enum))
		for i, name := range schema.XEnumVarnames {
			if name == "Undefined" || i >= len(schema.Enum) {
				continue
			}
			intVal, ok := schema.Enum[i].(int)
			if !ok {
				continue
			}
			m[name] = intVal
		}
		result[schemaName] = m
	}
	return result
}

func buildReverseEnumMap(fwd map[string]map[string]int) map[string]map[int]string {
	rev := make(map[string]map[int]string, len(fwd))
	for schema, m := range fwd {
		r := make(map[int]string, len(m))
		for name, val := range m {
			r[val] = name
		}
		rev[schema] = r
	}
	return rev
}

func enumNameFromMap(rev map[string]map[int]string, schema string, val int) string {
	if m, ok := rev[schema]; ok {
		if name, ok2 := m[val]; ok2 {
			return name
		}
	}
	return fmt.Sprintf("%d", val)
}

func splitWordsForKey(s string) []string {
	var words []string
	var current []rune
	runes := []rune(s)
	n := len(runes)
	for i := 0; i < n; i++ {
		ch := runes[i]
		if unicode.IsSpace(ch) || ch == '_' || ch == '-' {
			if len(current) > 0 {
				words = append(words, string(current))
				current = current[:0]
			}
			continue
		}
		if len(current) > 0 && unicode.IsUpper(ch) {
			prev := current[len(current)-1]
			if !unicode.IsUpper(prev) {
				words = append(words, string(current))
				current = current[:0]
			} else if i+1 < n && unicode.IsLower(runes[i+1]) {
				words = append(words, string(current))
				current = current[:0]
			}
		}
		current = append(current, ch)
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	return words
}

func ToCamelCaseFirstLower(text string) string {
	words := splitWordsForKey(text)
	for i := range words {
		words[i] = strings.ToLower(words[i])
		if i > 0 {
			words[i] = englishTitleCaserCfg.String(words[i])
		}
	}
	return strings.Join(words, "")
}

// AppToYaml serialises a *api.StreamApp back to the hierarchical YAML format
// understood by the designer (same format as the frontend-produced config files).
func AppToYaml(app *api.StreamApp) ([]byte, error) {
	rev := appEnums

	streamKey := make(map[int]string, len(app.Streams))
	for i := range app.Streams {
		s := &app.Streams[i]
		streamKey[s.Id] = ToCamelCaseFirstLower(s.Name)
	}

	epKey := make(map[int]string, len(app.Endpoints))
	dcKey := make(map[int]string, len(app.DataConnectors))
	for i := range app.DataConnectors {
		dc := &app.DataConnectors[i]
		dcKey[dc.Id] = ToCamelCaseFirstLower(dc.Name)
	}
	dcEndpoints := make(map[int][]api.Endpoint)
	for i := range app.Endpoints {
		ep := &app.Endpoints[i]
		epKey[ep.Id] = ToCamelCaseFirstLower(ep.Name)
		dcEndpoints[ep.IdDataConnector] = append(dcEndpoints[ep.IdDataConnector], *ep)
	}

	typeKey := make(map[string]string, len(app.Types))
	for i := range app.Types {
		t := &app.Types[i]
		typeKey[t.Name] = ToCamelCaseFirstLower(t.Name)
	}

	consumers := make(map[int][]int)
	for i := range app.Streams {
		s := &app.Streams[i]
		if s.IdSource != 0 {
			consumers[s.IdSource] = append(consumers[s.IdSource], s.Id)
		}
		if s.IdSources != nil {
			for _, src := range *s.IdSources {
				consumers[src] = append(consumers[src], s.Id)
			}
		}
	}
	errorStreamKey := make(map[int]string)
	for i := range app.Streams {
		s := &app.Streams[i]
		if s.Type == api.TransformationTypeError && s.IdSource != 0 {
			errorStreamKey[s.IdSource] = streamKey[s.Id]
		}
	}

	doc := make(map[string]interface{})

	// settings
	settingsNode := map[string]interface{}{
		"name": app.Settings.Name,
	}
	if app.Settings.ModuleVersion != nil && *app.Settings.ModuleVersion != "" {
		settingsNode["moduleVersion"] = *app.Settings.ModuleVersion
	}
	if app.Settings.RepoPath != nil && *app.Settings.RepoPath != "" {
		settingsNode["repoPath"] = *app.Settings.RepoPath
	}
	doc["settings"] = settingsNode

	// modules
	if app.Modules != nil && len(*app.Modules) > 0 {
		modulesNode := make(map[string]interface{})
		for _, m := range *app.Modules {
			mNode := map[string]interface{}{
				"name":       m.Name,
				"modulePath": m.ModulePath,
			}
			if m.GolangVersion != "" {
				mNode["golangVersion"] = m.GolangVersion
			}
			modulesNode[ToCamelCaseFirstLower(m.Name)] = mNode
		}
		doc["modules"] = modulesNode
	}

	// pools
	if len(app.Pools) > 0 {
		poolsNode := make(map[string]interface{})
		for _, p := range app.Pools {
			poolNode := map[string]interface{}{
				"name":           p.Name,
				"executorsCount": p.ExecutorsCount,
			}
			if p.QueueCapacity != nil && *p.QueueCapacity > 0 {
				poolNode["queueCapacity"] = *p.QueueCapacity
			}
			poolsNode[ToCamelCaseFirstLower(p.Name)] = poolNode
		}
		doc["pools"] = poolsNode
	}

	// types
	if len(app.Types) > 0 {
		typesNode := make(map[string]interface{})
		for _, t := range app.Types {
			tNode := map[string]interface{}{}
			if t.Name != ToCamelCaseFirstLower(t.Name) {
				tNode["name"] = t.Name
			}
			if string(t.Type) != "" {
				tNode["type"] = string(t.Type)
			}
			if t.DefinitionFormat != nil {
				tNode["definitionFormat"] = enumNameFromMap(rev, "TypeDefinitionFormat", int(*t.DefinitionFormat))
			}
			if t.Module != nil && *t.Module != "" {
				tNode["module"] = *t.Module
			}
			if t.Package != nil && *t.Package != "" {
				tNode["package"] = *t.Package
			}
			if t.TransferByValue != nil {
				tNode["transferByValue"] = *t.TransferByValue
			}
			if t.UseAlias != nil {
				tNode["useAlias"] = *t.UseAlias
			}
			if t.PublicType != nil {
				tNode["publicType"] = *t.PublicType
			}
			if t.Description != nil && *t.Description != "" {
				tNode["description"] = *t.Description
			}
			if t.ValueType != nil {
				tNode["valueType"] = *t.ValueType
			}
			if t.KeyType != nil {
				tNode["keyType"] = *t.KeyType
			}
			if t.TypeDefinitionLang1 != nil {
				tNode["typeDefinitionLang1"] = *t.TypeDefinitionLang1
			}
			if t.TypeDefinitionLang2 != nil {
				tNode["typeDefinitionLang2"] = *t.TypeDefinitionLang2
			}
			if t.TypeImportLang1 != nil {
				tNode["typeImportLang1"] = *t.TypeImportLang1
			}
			if t.TypeImportLang2 != nil {
				tNode["typeImportLang2"] = *t.TypeImportLang2
			}
			typesNode[ToCamelCaseFirstLower(t.Name)] = tNode
		}
		doc["types"] = typesNode
	}

	// dataConnectors with nested endpoints
	if len(app.DataConnectors) > 0 {
		dcNode := make(map[string]interface{})
		for _, dc := range app.DataConnectors {
			dcObj := map[string]interface{}{
				"name": dc.Name,
				"type": enumNameFromMap(rev, "DataConnectorType", int(dc.Type)),
			}
			if dc.Implementation != nil {
				dcObj["implementation"] = string(*dc.Implementation)
			}
			if dc.GoImplementation != nil {
				dcObj["goImplementation"] = string(*dc.GoImplementation)
			}
			if dc.CppUserverImplementation != nil {
				dcObj["cppUserverImplementation"] = string(*dc.CppUserverImplementation)
			}
			if dc.CppBoostImplementation != nil {
				dcObj["cppBoostImplementation"] = string(*dc.CppBoostImplementation)
			}
			if dc.PythonImplementation != nil {
				dcObj["pythonImplementation"] = string(*dc.PythonImplementation)
			}
			if dc.RustImplementation != nil {
				dcObj["rustImplementation"] = string(*dc.RustImplementation)
			}
			if dc.TypeScriptImplementation != nil {
				dcObj["typeScriptImplementation"] = string(*dc.TypeScriptImplementation)
			}
			if dc.Host != nil {
				dcObj["host"] = *dc.Host
			}
			if dc.Port != nil {
				dcObj["port"] = *dc.Port
			}
			if dc.Address != nil {
				dcObj["address"] = *dc.Address
			}
			if dc.ConnectionsCount != nil {
				dcObj["connectionsCount"] = *dc.ConnectionsCount
			}
			if dc.Brokers != nil {
				dcObj["brokers"] = *dc.Brokers
			}
			if dc.Version != nil {
				dcObj["version"] = *dc.Version
			}
			if dc.DialTimeout != nil {
				dcObj["dialTimeout"] = *dc.DialTimeout
			}
			if dc.UsePartitioner != nil {
				dcObj["usePartitioner"] = *dc.UsePartitioner
			}
			if dc.Async != nil {
				dcObj["async"] = *dc.Async
			}
			if dc.SecurityProtocol != nil {
				dcObj["securityProtocol"] = *dc.SecurityProtocol
			}
			if dc.SaslMechanism != nil {
				dcObj["saslMechanism"] = *dc.SaslMechanism
			}
			if dc.Username != nil {
				dcObj["username"] = *dc.Username
			}
			if dc.Password != nil {
				dcObj["password"] = *dc.Password
			}
			if dc.UseDedicatedListener != nil {
				dcObj["useDedicatedListener"] = *dc.UseDedicatedListener
			}
			if dc.Module != nil {
				dcObj["module"] = *dc.Module
			}
			if dc.Namespace != nil {
				dcObj["namespace"] = *dc.Namespace
			}
			if dc.Identity != nil {
				dcObj["identity"] = *dc.Identity
			}
			if dc.ApiKey != nil {
				dcObj["apiKey"] = *dc.ApiKey
			}
			if dc.TlsEnabled != nil {
				dcObj["tlsEnabled"] = *dc.TlsEnabled
			}
			if dc.TlsServerName != nil {
				dcObj["tlsServerName"] = *dc.TlsServerName
			}
			if dc.TlsCaFile != nil {
				dcObj["tlsCaFile"] = *dc.TlsCaFile
			}
			if dc.TlsCertFile != nil {
				dcObj["tlsCertFile"] = *dc.TlsCertFile
			}
			if dc.TlsKeyFile != nil {
				dcObj["tlsKeyFile"] = *dc.TlsKeyFile
			}
			if dc.MaxConcurrentActivities != nil {
				dcObj["maxConcurrentActivities"] = *dc.MaxConcurrentActivities
			}
			if dc.MaxConcurrentWorkflows != nil {
				dcObj["maxConcurrentWorkflows"] = *dc.MaxConcurrentWorkflows
			}
			if eps, ok := dcEndpoints[dc.Id]; ok && len(eps) > 0 {
				epsNode := make(map[string]interface{})
				for _, ep := range eps {
					epObj := map[string]interface{}{
						"name": ep.Name,
					}
					if ep.GrpcMethodType != nil {
						epObj["grpcMethodType"] = enumNameFromMap(rev, "GrpcMethodType", int(*ep.GrpcMethodType))
					}
					if ep.HttpMethodType != nil {
						epObj["httpMethodType"] = string(*ep.HttpMethodType)
					}
					if ep.Path != nil {
						epObj["path"] = *ep.Path
					}
					if ep.Topic != nil {
						epObj["topic"] = *ep.Topic
					}
					if ep.Enabled != nil {
						epObj["enabled"] = *ep.Enabled
					}
					if ep.Partitions != nil {
						epObj["partitions"] = *ep.Partitions
					}
					if ep.CreateTopic != nil {
						epObj["createTopic"] = *ep.CreateTopic
					}
					if ep.ReplicationFactor != nil {
						epObj["replicationFactor"] = *ep.ReplicationFactor
					}
					if ep.ConsumerGroup != nil {
						epObj["consumerGroup"] = *ep.ConsumerGroup
					}
					if ep.MethodName != nil {
						epObj["methodName"] = *ep.MethodName
					}
					if ep.Schedule != nil {
						epObj["schedule"] = *ep.Schedule
					}
					if ep.ScheduleId != nil {
						epObj["scheduleId"] = *ep.ScheduleId
					}
					if ep.Timezone != nil {
						epObj["timezone"] = *ep.Timezone
					}
					if ep.OverlapPolicy != nil {
						epObj["overlapPolicy"] = string(*ep.OverlapPolicy)
					}
					if ep.MissedRunPolicy != nil {
						epObj["missedRunPolicy"] = string(*ep.MissedRunPolicy)
					}
					if ep.TaskQueue != nil {
						epObj["taskQueue"] = *ep.TaskQueue
					}
					if ep.WorkflowExecutionTimeout != nil {
						epObj["workflowExecutionTimeout"] = *ep.WorkflowExecutionTimeout
					}
					if ep.ActivityStartToCloseTimeout != nil {
						epObj["activityStartToCloseTimeout"] = *ep.ActivityStartToCloseTimeout
					}
					if ep.ActivityHeartbeatTimeout != nil {
						epObj["activityHeartbeatTimeout"] = *ep.ActivityHeartbeatTimeout
					}
					if ep.MaximumAttempts != nil {
						epObj["maximumAttempts"] = *ep.MaximumAttempts
					}
					if ep.FunctionName != nil {
						epObj["functionName"] = *ep.FunctionName
					}
					if ep.FunctionPackage != nil {
						epObj["functionPackage"] = *ep.FunctionPackage
					}
					if ep.PublicFunction != nil {
						epObj["publicFunction"] = *ep.PublicFunction
					}
					if ep.FunctionDescription != nil {
						epObj["functionDescription"] = *ep.FunctionDescription
					}
					if ep.FunctionInitializerGroup != nil {
						epObj["functionInitializerGroup"] = *ep.FunctionInitializerGroup
					}
					if ep.FunctionModule != nil {
						epObj["functionModule"] = *ep.FunctionModule
					}
					epsNode[epKey[ep.Id]] = epObj
				}
				dcObj["endpoints"] = epsNode
			}
			dcNode[ToCamelCaseFirstLower(dc.Name)] = dcObj
		}
		doc["dataConnectors"] = dcNode
	}

	// services with pipelines and links
	{
		streamsBySvcPipeline := make(map[int]map[string][]api.Stream)
		for i := range app.Streams {
			s := &app.Streams[i]
			pipe := ""
			if s.Pipeline != nil {
				pipe = *s.Pipeline
			}
			if streamsBySvcPipeline[s.IdService] == nil {
				streamsBySvcPipeline[s.IdService] = make(map[string][]api.Stream)
			}
			streamsBySvcPipeline[s.IdService][pipe] = append(streamsBySvcPipeline[s.IdService][pipe], *s)
		}
		streamSvc := make(map[int]int)
		for i := range app.Streams {
			s := &app.Streams[i]
			streamSvc[s.Id] = s.IdService
		}
		linksBySvc := make(map[int][]api.Link)
		for i := range app.Links {
			l := &app.Links[i]
			svcId := streamSvc[l.From]
			linksBySvc[svcId] = append(linksBySvc[svcId], *l)
		}

		svcsNode := make(map[string]interface{})
		for _, svc := range app.Services {
			svcObj := map[string]interface{}{
				"name":                   svc.Name,
				"defaultCallSemantics":   enumNameFromMap(rev, "CallSemantics", int(svc.DefaultCallSemantics)),
				"programmingLanguage":    enumNameFromMap(rev, "ProgrammingLanguage", int(svc.ProgrammingLanguage)),
				"modulePath":             svc.ModulePath,
				"httpHost":               svc.HttpHost,
				"httpPort":               svc.HttpPort,
				"grpcHost":               svc.GrpcHost,
				"grpcPort":               svc.GrpcPort,
				"shutdownTimeout":        svc.ShutdownTimeout,
				"environment":            svc.Environment,
				"color":                  svc.Color,
				"statusHandler":          svc.StatusHandler,
				"metricsHandler":         svc.MetricsHandler,
				"startupHandler":         svc.StartupHandler,
				"readinessHandler":       svc.ReadinessHandler,
				"livenessHandler":        svc.LivenessHandler,
				"kubernetesWorkloadType": svc.KubernetesWorkloadType,
			}
			if svc.GolangVersion != nil {
				svcObj["golangVersion"] = *svc.GolangVersion
			}
			if svc.DefaultGrpcTimeout != 0 {
				svcObj["defaultGrpcTimeout"] = svc.DefaultGrpcTimeout
			}
			if svc.LogLevel != nil {
				svcObj["logLevel"] = string(*svc.LogLevel)
			}

			pipelinesNode := make(map[string]interface{})
			if pipes, ok := streamsBySvcPipeline[svc.Id]; ok {
				pipeNames := make([]string, 0, len(pipes))
				for p := range pipes {
					pipeNames = append(pipeNames, p)
				}
				sort.Strings(pipeNames)
				for _, pipeName := range pipeNames {
					pipeStreams := pipes[pipeName]
					pipeNode := make(map[string]interface{})
					for _, s := range pipeStreams {
						sNode := map[string]interface{}{
							"type": enumNameFromMap(rev, "TransformationType", int(s.Type)),
							"name": s.Name,
							"xPos": s.XPos,
							"yPos": s.YPos,
						}
						if s.IdSource != 0 {
							sNode["source"] = streamKey[s.IdSource]
						}
						if s.IdSources != nil && len(*s.IdSources) > 0 {
							keys := make([]string, 0, len(*s.IdSources))
							for _, id := range *s.IdSources {
								keys = append(keys, streamKey[id])
							}
							sNode["sources"] = keys
						}
						if s.IdEndpoint != nil {
							sNode["endpoint"] = epKey[*s.IdEndpoint]
						}
						if s.ValueType != nil {
							sNode["valueType"] = typeKey[*s.ValueType]
							if sNode["valueType"] == "" {
								sNode["valueType"] = *s.ValueType
							}
						}
						if s.KeyType != nil {
							sNode["keyType"] = typeKey[*s.KeyType]
							if sNode["keyType"] == "" {
								sNode["keyType"] = *s.KeyType
							}
						}
						if esk, ok := errorStreamKey[s.Id]; ok {
							sNode["errorStream"] = esk
						}
						if s.JoinType != nil {
							sNode["joinType"] = enumNameFromMap(rev, "JoinType", int(*s.JoinType))
						}
						if s.JoinStorage != nil {
							sNode["joinStorage"] = enumNameFromMap(rev, "JoinStorageType", int(*s.JoinStorage))
						}
						if s.Pattern != nil {
							sNode["pattern"] = enumNameFromMap(rev, "ProcessPattern", int(*s.Pattern))
						}
						if s.FunctionName != nil {
							sNode["functionName"] = *s.FunctionName
						}
						if s.FunctionPackage != nil {
							sNode["functionPackage"] = *s.FunctionPackage
						}
						if s.PublicFunction != nil {
							sNode["publicFunction"] = *s.PublicFunction
						}
						if s.FunctionDescription != nil {
							sNode["functionDescription"] = *s.FunctionDescription
						}
						if s.FunctionInitializerGroup != nil {
							sNode["functionInitializerGroup"] = *s.FunctionInitializerGroup
						}
						if s.FunctionModule != nil {
							sNode["functionModule"] = *s.FunctionModule
						}
						if s.Ttl != nil {
							sNode["ttl"] = *s.Ttl
						}
						if s.RenewTTL != nil {
							sNode["renewTTL"] = *s.RenewTTL
						}
						if s.Duration != nil {
							sNode["duration"] = *s.Duration
						}
						pipeNode[streamKey[s.Id]] = sNode
					}
					pipelinesNode[pipeName] = pipeNode
				}
			}
			svcObj["pipelines"] = pipelinesNode

			if links, ok := linksBySvc[svc.Id]; ok && len(links) > 0 {
				linksNode := make(map[string]interface{})
				for i, l := range links {
					lNode := map[string]interface{}{
						"from": streamKey[l.From],
						"to":   streamKey[l.To],
					}
					if l.CallSemantics != 0 {
						lNode["callSemantics"] = enumNameFromMap(rev, "CallSemantics", int(l.CallSemantics))
					}
					if l.PoolName != nil {
						lNode["poolName"] = *l.PoolName
					}
					if l.Priority != nil {
						lNode["priority"] = *l.Priority
					}
					if l.IdDataConnector != nil {
						lNode["dataConnector"] = dcKey[*l.IdDataConnector]
					}
					if l.TaskQueue != nil {
						lNode["taskQueue"] = *l.TaskQueue
					}
					if l.WorkflowExecutionTimeout != nil {
						lNode["workflowExecutionTimeout"] = *l.WorkflowExecutionTimeout
					}
					if l.ActivityStartToCloseTimeout != nil {
						lNode["activityStartToCloseTimeout"] = *l.ActivityStartToCloseTimeout
					}
					if l.ActivityHeartbeatTimeout != nil {
						lNode["activityHeartbeatTimeout"] = *l.ActivityHeartbeatTimeout
					}
					if l.MaximumAttempts != nil {
						lNode["maximumAttempts"] = *l.MaximumAttempts
					}
					linksNode[fmt.Sprintf("link%d", i+1)] = lNode
				}
				svcObj["links"] = linksNode
			}

			svcsNode[ToCamelCaseFirstLower(svc.Name)] = svcObj
		}
		doc["services"] = svcsNode
	}

	return yaml.Marshal(doc)
}
