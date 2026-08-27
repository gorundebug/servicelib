/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"reflect"
	"sort"
	"strings"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

func (app *ServiceApp) RuntimeToStreamApp() *api.StreamApp {
	rc := app.RuntimeConfig()
	cfg := rc.GetConfig()

	// Services and Pools have no runtime equivalent — take from config.
	services := make([]api.Service, 0, len(cfg.GetServices()))
	for _, svc := range cfg.GetServices() {
		services = append(services, config.ServiceConfigToAPI(svc))
	}
	pools := make([]api.Pool, 0, len(app.taskPools)+len(app.priorityTaskPools))
	for _, p := range app.taskPools {
		pools = append(pools, api.Pool{Name: p.GetName(), ExecutorsCount: p.GetExecutorsCount()})
	}
	for _, p := range app.priorityTaskPools {
		pools = append(pools, api.Pool{Name: p.GetName(), ExecutorsCount: p.GetExecutorsCount()})
	}

	registeredStreams := make(map[int]bool, len(app.streams))
	for id := range app.streams {
		registeredStreams[id] = true
	}
	// Also register virtual IDs of error streams (-producerID) so error links pass the registeredStreams check.
	for _, runtimeStream := range app.streams {
		if ec := runtimeStream.GetErrorConsumer(); ec != nil && len(ec.GetConsumers()) > 0 {
			registeredStreams[ec.Stream().GetID()] = true
		}
	}
	// errorLinkSources maps (errorStream.GetID(), consumer.GetID()) → virtual error stream Id (-producer.Id).
	// ErrorStream.GetID() returns -producerID, which matches virtualID = -s.Id computed below.
	errorLinkSources := make(map[config.LinkID]int)
	// consumerOverrideIdSource maps consumer stream ID → virtual error stream ID.
	// Used to patch IdSource for streams that feed from an error stream but have IdSource=0 in config
	// (because the error stream has no config entry of its own).
	consumerOverrideIdSource := make(map[int]int)

	typeMap := make(map[string]api.Type)
	modulesByPath := make(map[string]api.Module)

	// mainModulePath is the inferred module root of the running service, derived from the
	// concrete environment type via reflection. Used to distinguish internal vs external types.
	mainModulePath := ""
	if app.environment != nil {
		t := reflect.TypeOf(app.environment)
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		mainModulePath = inferModulePath(t.PkgPath())
	}

	streams := make([]api.Stream, 0, len(app.streams))
	for _, runtimeStream := range app.streams {
		s := config.StreamConfigToAPI(runtimeStream.Stream().GetConfig())
		applyFunctionImpl(runtimeStream.FunctionImplementation(), &s.FunctionName, &s.FunctionPackage, &s.FunctionModule, &s.PublicFunction, mainModulePath, modulesByPath)

		streamName := runtimeStream.Stream().GetName()
		if name, dt, pkg, isPtr := typeInfoFromReflect(runtimeStream.GetValueType()); dt != api.DataTypeUndefined {
			if name != "" {
				s.ValueType = &name
			}
			typeName := name
			if typeName == "" {
				if s.ValueType != nil && *s.ValueType != "" {
					typeName = *s.ValueType
				} else {
					typeName = streamName
					s.ValueType = &typeName
				}
			}
			addTypeEntry(typeName, dt, pkg, isPtr, typeMap, modulesByPath, mainModulePath)
		}
		if name, dt, pkg, isPtr := typeInfoFromReflect(runtimeStream.GetKeyType()); dt != api.DataTypeUndefined {
			if name != "" {
				s.KeyType = &name
			}
			typeName := name
			if typeName == "" {
				if s.KeyType != nil && *s.KeyType != "" {
					typeName = *s.KeyType
				} else {
					typeName = streamName + "Key"
					s.KeyType = &typeName
				}
			}
			addTypeEntry(typeName, dt, pkg, isPtr, typeMap, modulesByPath, mainModulePath)
		}

		streams = append(streams, s)

		ec := runtimeStream.GetErrorConsumer()
		if ec == nil || len(ec.GetConsumers()) == 0 {
			continue
		}
		virtualID := -s.Id
		errStream := api.Stream{
			Id:        virtualID,
			Type:      api.TransformationTypeError,
			IdService: s.IdService,
			IdSource:  s.Id,
			Name:      s.Name + " Error",
			Pipeline:  s.Pipeline,
		}
		if name, dt, pkg, isPtr := typeInfoFromReflect(ec.GetValueType()); dt != api.DataTypeUndefined {
			if name != "" {
				errStream.ValueType = &name
			}
			typeName := name
			if typeName == "" {
				if errStream.ValueType != nil && *errStream.ValueType != "" {
					typeName = *errStream.ValueType
				} else {
					typeName = streamName
					errStream.ValueType = &typeName
				}
			}
			addTypeEntry(typeName, dt, pkg, isPtr, typeMap, modulesByPath, mainModulePath)
		}
		streams = append(streams, errStream)
		for _, consumer := range ec.GetConsumers() {
			consumerOverrideIdSource[consumer.GetID()] = virtualID
			errorLinkSources[config.LinkID{From: ec.Stream().GetID(), To: consumer.GetID()}] = virtualID
		}
	}

	// Patch IdSource for streams that consume an error stream: their config has IdSource=0
	// because the error stream has no config entry.
	for i := range streams {
		if override, ok := consumerOverrideIdSource[streams[i].Id]; ok {
			streams[i].IdSource = override
		}
	}

	types := make([]api.Type, 0, len(typeMap))
	for _, t := range typeMap {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })

	var modules *[]api.Module
	if len(modulesByPath) > 0 {
		m := make([]api.Module, 0, len(modulesByPath))
		for _, mod := range modulesByPath {
			m = append(m, mod)
		}
		sort.Slice(m, func(i, j int) bool { return m[i].Name < m[j].Name })
		modules = &m
	}

	// DataConnectors and Endpoints from registered runtime data sources/sinks.
	dataConnectors := make([]api.DataConnector, 0, len(app.dataSources)+len(app.dataSinks))
	var endpoints []api.Endpoint
	for _, ds := range app.dataSources {
		dataConnectors = append(dataConnectors, config.DataConnectorConfigToAPI(ds.GetConfig()))
		eps := ds.GetEndpoints()
		for i := 0; i < eps.Len(); i++ {
			ep := config.EndpointConfigToAPI(eps.At(i).GetConfig())
			if ec, ok := app.endpointConsumers[ep.Id]; ok {
				applyEndpointFunctionImpl(ec.FunctionImplementation(), &ep, mainModulePath, modulesByPath)
			}
			endpoints = append(endpoints, ep)
		}
	}
	for _, ds := range app.dataSinks {
		dataConnectors = append(dataConnectors, config.DataConnectorConfigToAPI(ds.GetConfig()))
		eps := ds.GetEndpoints()
		for i := 0; i < eps.Len(); i++ {
			ep := config.EndpointConfigToAPI(eps.At(i).GetConfig())
			if ec, ok := app.endpointConsumers[ep.Id]; ok {
				applyEndpointFunctionImpl(ec.FunctionImplementation(), &ep, mainModulePath, modulesByPath)
			}
			endpoints = append(endpoints, ep)
		}
	}

	// Links: only non-default call semantics; topology is in IdSource/IdSources.
	// Semantics come from runtime callers (resolved in MakeCaller), not from config.
	defaultCS := api.CallSemanticsInherited
	if svcCfg := app.ServiceConfig(); svcCfg != nil && svcCfg.DefaultCallSemantics != nil {
		if cs := svcCfg.DefaultCallSemantics.Get(); cs != nil {
			defaultCS = cs.GetType()
		}
	}
	var links []api.Link
	for _, li := range app.runtimeLinks {
		if !registeredStreams[li.From] || !registeredStreams[li.To] {
			continue
		}
		cs := li.CallSemantics.GetType()
		if cs == api.CallSemanticsInherited || cs == defaultCS {
			continue
		}
		from := li.From
		if virtualID, ok := errorLinkSources[config.LinkID{From: li.From, To: li.To}]; ok {
			from = virtualID
		}
		link := api.Link{From: from, To: li.To, CallSemantics: cs}
		switch c := li.CallSemantics.(type) {
		case *config.TaskPoolCallSemanticsConfig:
			link.PoolName = &c.PoolName
		case *config.PriorityTaskPoolCallSemanticsConfig:
			link.PoolName = &c.PoolName
			link.Priority = &c.Priority
		}
		links = append(links, link)
	}

	return &api.StreamApp{
		Services:       services,
		Pools:          pools,
		Streams:        streams,
		Types:          types,
		Modules:        modules,
		DataConnectors: dataConnectors,
		Endpoints:      endpoints,
		Links:          links,
	}
}

// inferModulePath returns the module root path for a Go package path.
// External modules (first path segment contains a dot, e.g. "github.com") use
// the first three path segments as the module root per standard Go conventions.
// Stdlib packages (no dot in first segment, e.g. "net/http") return "".
func inferModulePath(pkgPath string) string {
	if pkgPath == "" {
		return ""
	}
	parts := strings.Split(pkgPath, "/")
	if !strings.Contains(parts[0], ".") {
		return ""
	}
	n := 3
	if len(parts) < n {
		n = len(parts)
	}
	return strings.Join(parts[:n], "/")
}

// kindToDataType maps reflect.Kind to api.DataType.
func kindToDataType(kind reflect.Kind) api.DataType {
	switch kind {
	case reflect.Bool:
		return api.DataTypeBoolean
	case reflect.Int:
		return api.DataTypeInt
	case reflect.Int8:
		return api.DataTypeInt8
	case reflect.Int16:
		return api.DataTypeInt16
	case reflect.Int32:
		return api.DataTypeInt32
	case reflect.Int64:
		return api.DataTypeInt64
	case reflect.Uint:
		return api.DataTypeUint
	case reflect.Uint8:
		return api.DataTypeUint8
	case reflect.Uint16:
		return api.DataTypeUint16
	case reflect.Uint32:
		return api.DataTypeUint32
	case reflect.Uint64:
		return api.DataTypeUint64
	case reflect.Float32:
		return api.DataTypeFloat
	case reflect.Float64:
		return api.DataTypeDouble
	case reflect.String:
		return api.DataTypeString
	case reflect.Interface:
		return api.DataTypeCustom
	case reflect.Slice, reflect.Array:
		return api.DataTypeArray
	case reflect.Map:
		return api.DataTypeMap
	case reflect.Struct:
		return api.DataTypeStruct
	default:
		return api.DataTypeUndefined
	}
}

// typeInfoFromReflect extracts (name, DataType, pkgPath, isPtr) from a reflect.Type.
// isPtr is true when the original type was a pointer. Returns empty name for
// primitives/built-ins (no package) so the caller can skip the type entry.
func typeInfoFromReflect(t reflect.Type) (string, api.DataType, string, bool) {
	if t == nil {
		return "", api.DataTypeUndefined, "", false
	}
	isPtr := t.Kind() == reflect.Pointer
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	typeName := t.Name()
	if typeName == "" || strings.Contains(typeName, "[") {
		return "", api.DataTypeUndefined, "", false
	}
	dt := kindToDataType(t.Kind())
	if dt == api.DataTypeUndefined {
		return "", api.DataTypeUndefined, "", false
	}
	pkgPath := t.PkgPath()
	if pkgPath == "" {
		return "", dt, "", isPtr
	}
	return typeName, dt, pkgPath, isPtr
}

// funcPkgLabel strips the module path prefix and "internal/" from pkgPath.
func funcPkgLabel(pkgPath, modPath string) string {
	rel := strings.TrimPrefix(strings.TrimPrefix(pkgPath, modPath), "/")
	return strings.TrimPrefix(rel, "internal/")
}

// typePkgLabel strips the module path prefix and "pkg/" from pkgPath.
func typePkgLabel(pkgPath, modPath string) string {
	rel := strings.TrimPrefix(strings.TrimPrefix(pkgPath, modPath), "/")
	return strings.TrimPrefix(rel, "pkg/")
}

// addTypeEntry registers a named type in typeMap and its owning module in modulesByPath.
// No-ops if the type is already registered.
// A type is public when its inferred module path differs from mainModulePath.
func addTypeEntry(name string, dt api.DataType, pkgPath string, isPtr bool,
	typeMap map[string]api.Type, modulesByPath map[string]api.Module, mainModulePath string) {
	if name == "" || dt == api.DataTypeUndefined {
		return
	}
	if _, exists := typeMap[name]; exists {
		return
	}
	entry := api.Type{
		Name: name,
		Type: dt,
	}
	if pkgPath != "" {
		modPath := inferModulePath(pkgPath)
		isPublic := mainModulePath != "" && modPath != "" && modPath != mainModulePath
		publicType := isPublic
		entry.PublicType = &publicType
		if modPath != "" {
			modParts := strings.Split(modPath, "/")
			modName := modParts[len(modParts)-1]
			label := typePkgLabel(pkgPath, modPath)
			if label != "types" {
				entry.Package = &label
			}
			if isPublic {
				entry.Module = &modName
				if _, exists := modulesByPath[modPath]; !exists {
					modulesByPath[modPath] = api.Module{
						ModulePath: modPath,
						Name:       modName,
					}
				}
			}
		} else {
			parts := strings.Split(pkgPath, "/")
			label := parts[len(parts)-1]
			if label != "types" {
				entry.Package = &label
			}
		}
		switch dt {
		case api.DataTypeStruct:
			var defFmt api.TypeDefinitionFormat
			if strings.Contains(pkgPath, ".generated.proto") {
				defFmt = api.TypeDefinitionFormatProtobuf
			} else if strings.Contains(pkgPath, ".generated.openapi") {
				defFmt = api.TypeDefinitionFormatOpenAPI
			} else {
				defFmt = api.TypeDefinitionFormatNative
			}
			entry.DefinitionFormat = &defFmt
			if !isPtr {
				transferByValue := true
				entry.TransferByValue = &transferByValue
			}
		case api.DataTypeCustom:
			ifaceDef := "interface"
			entry.TypeDefinitionLang1 = &ifaceDef
		}
	}
	typeMap[name] = entry
}

func applyEndpointFunctionImpl(impl interface{}, endpoint *api.Endpoint, mainModulePath string, modules map[string]api.Module) {
	functionName := &endpoint.FunctionName
	applyFunctionImpl(impl, &functionName, &endpoint.FunctionPackage, &endpoint.FunctionModule, &endpoint.PublicFunction, mainModulePath, modules)
	if functionName != nil {
		endpoint.FunctionName = *functionName
	}
}

// applyFunctionImpl extracts the type name and package path from impl
// (via reflection) and writes them into the provided pointer-to-pointer fields.
// No-ops when impl is nil or the type is generic (contains "[").
// PublicFunction and FunctionModule are set only when the function lives in an external module.
func applyFunctionImpl(impl interface{}, fnName, fnPkg, fnModule **string, fnPublic **bool,
	mainModulePath string, modulesByPath map[string]api.Module) {
	if impl == nil {
		return
	}
	t := reflect.TypeOf(impl)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := t.Name()
	if name == "" || strings.Contains(name, "[") {
		return
	}
	*fnName = &name
	pkgPath := t.PkgPath()
	if pkgPath != "" {
		modPath := inferModulePath(pkgPath)
		label := funcPkgLabel(pkgPath, modPath)
		if label != "functions" {
			*fnPkg = &label
		}
		isPublic := mainModulePath != "" && modPath != "" && modPath != mainModulePath
		*fnPublic = &isPublic
		if isPublic {
			modParts := strings.Split(modPath, "/")
			modName := modParts[len(modParts)-1]
			*fnModule = &modName
			if _, exists := modulesByPath[modPath]; !exists {
				modulesByPath[modPath] = api.Module{
					ModulePath: modPath,
					Name:       modName,
				}
			}
		}
	}
}
