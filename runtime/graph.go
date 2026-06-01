/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"reflect"
	"runtime/debug"
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
	// errorConsumerFrom maps errorConsumer.Id → virtual error stream Id (-producer.Id),
	// so that links to error consumers are rewritten to originate from the virtual node.
	errorConsumerFrom := make(map[int]int)

	typeMap := make(map[string]api.Type)
	modulesByPath := make(map[string]api.Module)
	modulePathMap := buildModulePathMap()

	streams := make([]api.Stream, 0, len(app.streams))
	for _, runtimeStream := range app.streams {
		s := config.StreamConfigToAPI(runtimeStream.Stream().GetConfig())
		applyFunctionImpl(runtimeStream.FunctionImplementation(), &s.FunctionName, &s.FunctionPackage, &s.FunctionModule)

		if name, dt, pkg := typeInfoFromReflect(runtimeStream.GetValueType()); dt != api.DataTypeUndefined {
			typeName := name
			if typeName == "" {
				typeName = config.ToCamelCaseFirstLower(s.Name)
			}
			s.ValueType = &typeName
			addTypeEntry(typeName, dt, pkg, typeMap, modulesByPath, modulePathMap)
		}
		if name, dt, pkg := typeInfoFromReflect(runtimeStream.GetKeyType()); dt != api.DataTypeUndefined {
			typeName := name
			if typeName == "" {
				typeName = config.ToCamelCaseFirstLower(s.Name) + "Key"
			}
			s.KeyType = &typeName
			addTypeEntry(typeName, dt, pkg, typeMap, modulesByPath, modulePathMap)
		}

		streams = append(streams, s)

		ec := runtimeStream.GetErrorConsumer()
		if ec == nil || len(ec.GetConsumers()) == 0 {
			continue
		}
		errStream := api.Stream{
			Id:        -s.Id,
			Type:      api.TransformationTypeError,
			IdService: s.IdService,
			IdSource:  s.Id,
		}
		if name, dt, pkg := typeInfoFromReflect(ec.GetValueType()); dt != api.DataTypeUndefined {
			typeName := name
			if typeName == "" {
				typeName = config.ToCamelCaseFirstLower(s.Name) + "Error"
			}
			errStream.ValueType = &typeName
			addTypeEntry(typeName, dt, pkg, typeMap, modulesByPath, modulePathMap)
		}
		streams = append(streams, errStream)
		errorConsumerFrom[ec.Stream().GetID()] = -s.Id
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
				applyFunctionImpl(ec.FunctionImplementation(), &ep.FunctionName, &ep.FunctionPackage, &ep.FunctionModule)
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
				applyFunctionImpl(ec.FunctionImplementation(), &ep.FunctionName, &ep.FunctionPackage, &ep.FunctionModule)
			}
			endpoints = append(endpoints, ep)
		}
	}

	// Links: only non-default call semantics; topology is in IdSource/IdSources.
	// Semantics come from runtime callers (resolved in MakeCaller), not from config.
	var defaultCS api.CallSemantics = api.CallSemanticsInherited
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
		if virtualID, ok := errorConsumerFrom[li.To]; ok {
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

// buildModulePathMap returns known Go module paths from the binary's build info.
func buildModulePathMap() map[string]string {
	result := make(map[string]string)
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return result
	}
	if info.Main.Path != "" {
		result[info.Main.Path] = info.Main.Path
	}
	for _, dep := range info.Deps {
		result[dep.Path] = dep.Path
	}
	return result
}

// findModulePath returns the longest known module path that is a prefix of pkgPath.
func findModulePath(pkgPath string, modulePathMap map[string]string) string {
	best := ""
	for modPath := range modulePathMap {
		if strings.HasPrefix(pkgPath, modPath) && len(modPath) > len(best) {
			best = modPath
		}
	}
	return best
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
		return api.DataTypeAny
	case reflect.Slice, reflect.Array:
		return api.DataTypeArray
	case reflect.Map:
		return api.DataTypeMap
	case reflect.Struct:
		return api.DataTypeCustom
	default:
		return api.DataTypeUndefined
	}
}

// typeInfoFromReflect extracts (camelCaseName, DataType, pkgPath) from a reflect.Type.
// For types with no package (primitives, built-ins) returns empty name so the caller
// can use the stream name instead.
func typeInfoFromReflect(t reflect.Type) (string, api.DataType, string) {
	if t == nil {
		return "", api.DataTypeUndefined, ""
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	typeName := t.Name()
	if typeName == "" || strings.Contains(typeName, "[") {
		return "", api.DataTypeUndefined, ""
	}
	dt := kindToDataType(t.Kind())
	if dt == api.DataTypeUndefined {
		return "", api.DataTypeUndefined, ""
	}
	pkgPath := t.PkgPath()
	if pkgPath == "" {
		// Primitive or built-in: no package; caller uses stream name as type name.
		return "", dt, ""
	}
	return config.ToCamelCaseFirstLower(typeName), dt, pkgPath
}

// addTypeEntry registers a named type in typeMap and its owning module in modulesByPath.
// No-ops if the type is already registered.
func addTypeEntry(name string, dt api.DataType, pkgPath string,
	typeMap map[string]api.Type, modulesByPath map[string]api.Module,
	modulePathMap map[string]string) {
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
		entry.TypeImportLang1 = &pkgPath
		parts := strings.Split(pkgPath, "/")
		pkg := parts[len(parts)-1]
		entry.Package = &pkg
		modPath := findModulePath(pkgPath, modulePathMap)
		if modPath != "" {
			modParts := strings.Split(modPath, "/")
			modName := modParts[len(modParts)-1]
			entry.Module = &modName
			if _, exists := modulesByPath[modPath]; !exists {
				modulesByPath[modPath] = api.Module{
					ModulePath: modPath,
					Name:       modName,
				}
			}
		}
	}
	typeMap[name] = entry
}

// applyFunctionImpl extracts a camelCase type name and package path from impl
// (via reflection) and writes them into the provided pointer-to-pointer fields.
// No-ops when impl is nil or the type is generic (contains "[").
func applyFunctionImpl(impl interface{}, fnName, fnPkg, fnModule **string) {
	if impl == nil {
		return
	}
	t := reflect.TypeOf(impl)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	name := t.Name()
	if name == "" || strings.Contains(name, "[") {
		return
	}
	n := config.ToCamelCaseFirstLower(name)
	*fnName = &n
	pkgPath := t.PkgPath()
	if pkgPath != "" {
		parts := strings.Split(pkgPath, "/")
		pkg := parts[len(parts)-1]
		*fnPkg = &pkg
		*fnModule = &pkgPath
	}
}
