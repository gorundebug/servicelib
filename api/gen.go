// Package api provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen/v2 version v2.1.0 DO NOT EDIT.
package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

// Defines values for CallSemantics.
const (
	FunctionCall     CallSemantics = 1
	Inherited        CallSemantics = 0
	PriorityTaskPool CallSemantics = 3
	TaskPool         CallSemantics = 2
)

// Defines values for CommunicationProtocol.
const (
	GRPC CommunicationProtocol = 2
	HTTP CommunicationProtocol = 1
)

// Defines values for DataFormat.
const (
	Gorillaschema DataFormat = "gorilla/schema"
	Json          DataFormat = "json"
)

// Defines values for DataSourceImplementation.
const (
	FastHTTP DataSourceImplementation = "FastHTTP"
	Nethttp  DataSourceImplementation = "net/http"
)

// Defines values for DataSourceType.
const (
	GRPCServer    DataSourceType = 2
	HTTPServer    DataSourceType = 1
	KafkaConsumer DataSourceType = 3
)

// Defines values for DataType.
const (
	DataTypeArray         DataType = "array"
	DataTypeBoolean       DataType = "boolean"
	DataTypeByte          DataType = "byte"
	DataTypeChar          DataType = "char"
	DataTypeCustom        DataType = "custom"
	DataTypeDouble        DataType = "double"
	DataTypeFloat         DataType = "float"
	DataTypeInt           DataType = "int"
	DataTypeInt16         DataType = "int16"
	DataTypeInt32         DataType = "int32"
	DataTypeInt64         DataType = "int64"
	DataTypeInt8          DataType = "int8"
	DataTypeMap           DataType = "map"
	DataTypeString        DataType = "string"
	DataTypeStruct        DataType = "struct"
	DataTypeUint          DataType = "uint"
	DataTypeUint16        DataType = "uint16"
	DataTypeUint32        DataType = "uint32"
	DataTypeUint64        DataType = "uint64"
	DataTypeUint8         DataType = "uint8"
	DataTypeUnicodeChar   DataType = "unicode char"
	DataTypeUnicodeString DataType = "unicode string"
)

// Defines values for JoinStorageType.
const (
	Aerospike JoinStorageType = 3
	HashMap   JoinStorageType = 1
	RocksDB   JoinStorageType = 2
)

// Defines values for JoinType.
const (
	Inner JoinType = 1
	Left  JoinType = 2
	Outer JoinType = 4
	Right JoinType = 3
)

// Defines values for ProgrammingLanguage.
const (
	Cpp    ProgrammingLanguage = 2
	GoLang ProgrammingLanguage = 1
)

// Defines values for TransformationType.
const (
	TransformationTypeAppLink         TransformationType = 14
	TransformationTypeCycleLink       TransformationType = 15
	TransformationTypeExternalLink    TransformationType = 13
	TransformationTypeFilter          TransformationType = 3
	TransformationTypeFlatMap         TransformationType = 7
	TransformationTypeFlatMapIterable TransformationType = 8
	TransformationTypeForEach         TransformationType = 6
	TransformationTypeInput           TransformationType = 1
	TransformationTypeJoin            TransformationType = 4
	TransformationTypeKeyBy           TransformationType = 9
	TransformationTypeMap             TransformationType = 2
	TransformationTypeMerge           TransformationType = 10
	TransformationTypeMultiJoin       TransformationType = 5
	TransformationTypeParallels       TransformationType = 12
	TransformationTypeSplit           TransformationType = 11
)

// Defines values for TypeDefinitionFormat.
const (
	CapNProto   TypeDefinitionFormat = 4
	FlatBuffers TypeDefinitionFormat = 3
	Native      TypeDefinitionFormat = 1
	Protobuf    TypeDefinitionFormat = 2
)

// CallSemantics defines model for CallSemantics.
type CallSemantics int

// CommunicationProtocol defines model for CommunicationProtocol.
type CommunicationProtocol int

// DataFormat defines model for DataFormat.
type DataFormat string

// DataSource defines model for DataSource.
type DataSource struct {
	Id                  int                      `json:"id"`
	Implementation      DataSourceImplementation `json:"implementation"`
	Ip                  *string                  `json:"ip,omitempty"`
	Name                string                   `json:"name"`
	Port                *int                     `json:"port,omitempty"`
	ProgrammingLanguage ProgrammingLanguage      `json:"programmingLanguage"`
	Type                DataSourceType           `json:"type"`
}

// DataSourceImplementation defines model for DataSourceImplementation.
type DataSourceImplementation string

// DataSourceType defines model for DataSourceType.
type DataSourceType int

// DataType defines model for DataType.
type DataType string

// Endpoint defines model for Endpoint.
type Endpoint struct {
	Format       *DataFormat `json:"format,omitempty"`
	Id           int         `json:"id"`
	IdDataSource int         `json:"idDataSource"`
	Method       *string     `json:"method,omitempty"`
	Name         string      `json:"name"`
	Param        *string     `json:"param,omitempty"`
	Path         *string     `json:"path,omitempty"`
}

// JoinStorageType defines model for JoinStorageType.
type JoinStorageType int

// JoinType defines model for JoinType.
type JoinType int

// Link defines model for Link.
type Link struct {
	CallSemantics          CallSemantics          `json:"callSemantics"`
	CommunicationProtocol  *CommunicationProtocol `json:"communicationProtocol,omitempty"`
	From                   int                    `json:"from"`
	InheritedCallSemantics bool                   `json:"inheritedCallSemantics"`
	PoolName               *string                `json:"poolName,omitempty"`
	Priority               *int                   `json:"priority,omitempty"`
	To                     int                    `json:"to"`
}

// ProgrammingLanguage defines model for ProgrammingLanguage.
type ProgrammingLanguage int

// ProjectSettings defines model for ProjectSettings.
type ProjectSettings struct {
	GolangVersion string `json:"golangVersion"`
	ModulePath    string `json:"modulePath"`
	Name          string `json:"name"`
}

// Service defines model for Service.
type Service struct {
	Color                *string             `json:"color,omitempty"`
	DefaultCallSemantics CallSemantics       `json:"defaultCallSemantics"`
	Id                   int                 `json:"id"`
	MonitoringIp         string              `json:"monitoringIp"`
	MonitoringPort       int                 `json:"monitoringPort"`
	Name                 string              `json:"name"`
	ProgrammingLanguage  ProgrammingLanguage `json:"programmingLanguage"`
}

// Stream defines model for Stream.
type Stream struct {
	FunctionName    *string            `json:"functionName,omitempty"`
	FunctionPackage *string            `json:"functionPackage,omitempty"`
	Id              int                `json:"id"`
	IdEndpoint      *int               `json:"idEndpoint,omitempty"`
	IdService       int                `json:"idService"`
	IdSource        int                `json:"idSource"`
	IdSources       []int              `json:"idSources"`
	JoinStorage     *JoinStorageType   `json:"joinStorage,omitempty"`
	JoinType        *JoinType          `json:"joinType,omitempty"`
	KeyType         *string            `json:"keyType,omitempty"`
	Name            string             `json:"name"`
	PublicFunction  *bool              `json:"publicFunction,omitempty"`
	Type            TransformationType `json:"type"`
	ValueType       *string            `json:"valueType,omitempty"`
	XPos            *int               `json:"xPos,omitempty"`
	YPos            *int               `json:"yPos,omitempty"`
}

// StreamApp defines model for StreamApp.
type StreamApp struct {
	DataSources []DataSource    `json:"dataSources"`
	Endpoints   []Endpoint      `json:"endpoints"`
	Links       []Link          `json:"links"`
	Services    []Service       `json:"services"`
	Settings    ProjectSettings `json:"settings"`
	Streams     []Stream        `json:"streams"`
	Types       []Type          `json:"types"`
}

// StringBinary defines model for StringBinary.
type StringBinary = openapi_types.File

// TransformationType defines model for TransformationType.
type TransformationType int

// Type defines model for Type.
type Type struct {
	DefinitionFormat    *TypeDefinitionFormat `json:"definitionFormat,omitempty"`
	KeyType             *string               `json:"keyType,omitempty"`
	Name                string                `json:"name"`
	Package             *string               `json:"package,omitempty"`
	PublicType          *bool                 `json:"publicType,omitempty"`
	TransferByValue     *bool                 `json:"transferByValue,omitempty"`
	Type                DataType              `json:"type"`
	TypeDefinitionLang1 *string               `json:"typeDefinitionLang1,omitempty"`
	TypeDefinitionLang2 *string               `json:"typeDefinitionLang2,omitempty"`
	TypeImportLang1     *string               `json:"typeImportLang1,omitempty"`
	TypeImportLang2     *string               `json:"typeImportLang2,omitempty"`
	UseAlias            *bool                 `json:"useAlias,omitempty"`
	ValueType           *string               `json:"valueType,omitempty"`
}

// TypeDefinitionFormat defines model for TypeDefinitionFormat.
type TypeDefinitionFormat int

// PostGenerateCodeJSONRequestBody defines body for PostGenerateCode for application/json ContentType.
type PostGenerateCodeJSONRequestBody = StreamApp

// ServerInterface represents all server handlers.
type ServerInterface interface {
	// Generate project code.
	// (POST /generateCode)
	PostGenerateCode(w http.ResponseWriter, r *http.Request)
}

// ServerInterfaceWrapper converts contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler            ServerInterface
	HandlerMiddlewares []MiddlewareFunc
	ErrorHandlerFunc   func(w http.ResponseWriter, r *http.Request, err error)
}

type MiddlewareFunc func(http.Handler) http.Handler

// PostGenerateCode operation middleware
func (siw *ServerInterfaceWrapper) PostGenerateCode(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	handler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		siw.Handler.PostGenerateCode(w, r)
	}))

	for _, middleware := range siw.HandlerMiddlewares {
		handler = middleware(handler)
	}

	handler.ServeHTTP(w, r.WithContext(ctx))
}

type UnescapedCookieParamError struct {
	ParamName string
	Err       error
}

func (e *UnescapedCookieParamError) Error() string {
	return fmt.Sprintf("error unescaping cookie parameter '%s'", e.ParamName)
}

func (e *UnescapedCookieParamError) Unwrap() error {
	return e.Err
}

type UnmarshalingParamError struct {
	ParamName string
	Err       error
}

func (e *UnmarshalingParamError) Error() string {
	return fmt.Sprintf("Error unmarshaling parameter %s as JSON: %s", e.ParamName, e.Err.Error())
}

func (e *UnmarshalingParamError) Unwrap() error {
	return e.Err
}

type RequiredParamError struct {
	ParamName string
}

func (e *RequiredParamError) Error() string {
	return fmt.Sprintf("Query argument %s is required, but not found", e.ParamName)
}

type RequiredHeaderError struct {
	ParamName string
	Err       error
}

func (e *RequiredHeaderError) Error() string {
	return fmt.Sprintf("Header parameter %s is required, but not found", e.ParamName)
}

func (e *RequiredHeaderError) Unwrap() error {
	return e.Err
}

type InvalidParamFormatError struct {
	ParamName string
	Err       error
}

func (e *InvalidParamFormatError) Error() string {
	return fmt.Sprintf("Invalid format for parameter %s: %s", e.ParamName, e.Err.Error())
}

func (e *InvalidParamFormatError) Unwrap() error {
	return e.Err
}

type TooManyValuesForParamError struct {
	ParamName string
	Count     int
}

func (e *TooManyValuesForParamError) Error() string {
	return fmt.Sprintf("Expected one value for %s, got %d", e.ParamName, e.Count)
}

// Handler creates http.Handler with routing matching OpenAPI spec.
func Handler(si ServerInterface) http.Handler {
	return HandlerWithOptions(si, GorillaServerOptions{})
}

type GorillaServerOptions struct {
	BaseURL          string
	BaseRouter       *mux.Router
	Middlewares      []MiddlewareFunc
	ErrorHandlerFunc func(w http.ResponseWriter, r *http.Request, err error)
}

// HandlerFromMux creates http.Handler with routing matching OpenAPI spec based on the provided mux.
func HandlerFromMux(si ServerInterface, r *mux.Router) http.Handler {
	return HandlerWithOptions(si, GorillaServerOptions{
		BaseRouter: r,
	})
}

func HandlerFromMuxWithBaseURL(si ServerInterface, r *mux.Router, baseURL string) http.Handler {
	return HandlerWithOptions(si, GorillaServerOptions{
		BaseURL:    baseURL,
		BaseRouter: r,
	})
}

// HandlerWithOptions creates http.Handler with additional options
func HandlerWithOptions(si ServerInterface, options GorillaServerOptions) http.Handler {
	r := options.BaseRouter

	if r == nil {
		r = mux.NewRouter()
	}
	if options.ErrorHandlerFunc == nil {
		options.ErrorHandlerFunc = func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
	wrapper := ServerInterfaceWrapper{
		Handler:            si,
		HandlerMiddlewares: options.Middlewares,
		ErrorHandlerFunc:   options.ErrorHandlerFunc,
	}

	r.HandleFunc(options.BaseURL+"/generateCode", wrapper.PostGenerateCode).Methods("POST")

	return r
}
