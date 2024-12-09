// Package api provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen/v2 version v2.1.0 DO NOT EDIT.
package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

// Defines values for AppInputType.
const (
	AppInputTypeFunction    AppInputType = 0
	AppInputTypeGrpcHandler AppInputType = 2
	AppInputTypeHttpHandler AppInputType = 1
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
	CommunicationProtocolGRPC CommunicationProtocol = 2
	CommunicationProtocolHTTP CommunicationProtocol = 1
)

// Defines values for DataConnectorImplementation.
const (
	DataConnectorImplementationAiohttp  DataConnectorImplementation = "aiohttp"
	DataConnectorImplementationFastHTTP DataConnectorImplementation = "FastHTTP"
	DataConnectorImplementationFunction DataConnectorImplementation = "function"
	DataConnectorImplementationNethttp  DataConnectorImplementation = "net/http"
	DataConnectorImplementationSarama   DataConnectorImplementation = "sarama"
)

// Defines values for DataConnectorType.
const (
	DataConnectorTypeCustom DataConnectorType = 4
	DataConnectorTypeGRPC   DataConnectorType = 2
	DataConnectorTypeHTTP   DataConnectorType = 1
	DataConnectorTypeKafka  DataConnectorType = 3
	DataConnectorTypeN5     DataConnectorType = 5
)

// Defines values for DataFormat.
const (
	DataFormatCustom DataFormat = "custom"
	DataFormatForm   DataFormat = "form"
	DataFormatJson   DataFormat = "json"
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

// Defines values for GrpcMethodType.
const (
	BidirectionalStreaming GrpcMethodType = 4
	ClientStreaming        GrpcMethodType = 1
	NoStreaming            GrpcMethodType = 0
	ServerStreaming        GrpcMethodType = 2
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

// Defines values for LogLevel.
const (
	CRITICAL LogLevel = "CRITICAL"
	DEBUG    LogLevel = "DEBUG"
	ERROR    LogLevel = "ERROR"
	FATAL    LogLevel = "FATAL"
	INFO     LogLevel = "INFO"
	WARNING  LogLevel = "WARNING"
)

// Defines values for ProgrammingLanguage.
const (
	Cpp    ProgrammingLanguage = 2
	GoLang ProgrammingLanguage = 1
	Python ProgrammingLanguage = 3
)

// Defines values for TransformationType.
const (
	AppInput        TransformationType = 17
	AppSink         TransformationType = 14
	CycleLink       TransformationType = 15
	Delay           TransformationType = 16
	Filter          TransformationType = 3
	FlatMap         TransformationType = 7
	FlatMapIterable TransformationType = 8
	ForEach         TransformationType = 6
	Input           TransformationType = 1
	Join            TransformationType = 4
	KeyBy           TransformationType = 9
	Map             TransformationType = 2
	Merge           TransformationType = 10
	MultiJoin       TransformationType = 5
	Parallels       TransformationType = 12
	Sink            TransformationType = 13
	Split           TransformationType = 11
)

// Defines values for TypeDefinitionFormat.
const (
	CapNProto   TypeDefinitionFormat = 4
	FlatBuffers TypeDefinitionFormat = 3
	Native      TypeDefinitionFormat = 1
	Protobuf    TypeDefinitionFormat = 2
)

// AppInputType defines model for AppInputType.
type AppInputType int

// CallSemantics defines model for CallSemantics.
type CallSemantics int

// CommunicationProtocol defines model for CommunicationProtocol.
type CommunicationProtocol int

// DataConnector defines model for DataConnector.
type DataConnector struct {
	Async               *bool                       `json:"async,omitempty"`
	Brokers             *string                     `json:"brokers,omitempty"`
	DialTimeout         *float32                    `json:"dialTimeout,omitempty"`
	Host                *string                     `json:"host,omitempty"`
	Id                  int                         `json:"id"`
	Implementation      DataConnectorImplementation `json:"implementation"`
	Name                string                      `json:"name"`
	Port                *int                        `json:"port,omitempty"`
	ProgrammingLanguage *ProgrammingLanguage        `json:"programmingLanguage,omitempty"`
	Type                DataConnectorType           `json:"type"`
	UsePartitioner      *bool                       `json:"usePartitioner,omitempty"`
	Version             *string                     `json:"version,omitempty"`
}

// DataConnectorImplementation defines model for DataConnectorImplementation.
type DataConnectorImplementation string

// DataConnectorType defines model for DataConnectorType.
type DataConnectorType int

// DataFormat defines model for DataFormat.
type DataFormat string

// DataType defines model for DataType.
type DataType string

// Endpoint defines model for Endpoint.
type Endpoint struct {
	ConsumerGroup       *string     `json:"consumerGroup,omitempty"`
	CreateTopic         *bool       `json:"createTopic,omitempty"`
	Delay               *int        `json:"delay,omitempty"`
	Format              *DataFormat `json:"format,omitempty"`
	FunctionDescription *string     `json:"functionDescription,omitempty"`
	FunctionName        *string     `json:"functionName,omitempty"`
	FunctionPackage     *string     `json:"functionPackage,omitempty"`
	Id                  int         `json:"id"`
	IdDataConnector     int         `json:"idDataConnector"`
	Method              *string     `json:"method,omitempty"`
	Name                string      `json:"name"`
	Partitions          *int        `json:"partitions,omitempty"`
	Path                *string     `json:"path,omitempty"`
	PublicFunction      *bool       `json:"publicFunction,omitempty"`
	ReplicationFactor   *int        `json:"replicationFactor,omitempty"`
	Topic               *string     `json:"topic,omitempty"`
	UseHandler          *bool       `json:"useHandler,omitempty"`
}

// GrpcMethodType defines model for GrpcMethodType.
type GrpcMethodType int

// JoinStorageType defines model for JoinStorageType.
type JoinStorageType int

// JoinType defines model for JoinType.
type JoinType int

// Link defines model for Link.
type Link struct {
	CallSemantics                CallSemantics          `json:"callSemantics"`
	CommunicationProtocol        *CommunicationProtocol `json:"communicationProtocol,omitempty"`
	From                         int                    `json:"from"`
	IncomeCallSemantics          *CallSemantics         `json:"incomeCallSemantics,omitempty"`
	IncomeInheritedCallSemantics *bool                  `json:"incomeInheritedCallSemantics,omitempty"`
	IncomePoolName               *string                `json:"incomePoolName,omitempty"`
	IncomePriority               *int                   `json:"incomePriority,omitempty"`
	InheritedCallSemantics       bool                   `json:"inheritedCallSemantics"`
	MethodName                   *string                `json:"methodName,omitempty"`
	PoolName                     *string                `json:"poolName,omitempty"`
	Priority                     *int                   `json:"priority,omitempty"`
	Timeout                      *int                   `json:"timeout,omitempty"`
	To                           int                    `json:"to"`
}

// LogLevel defines model for LogLevel.
type LogLevel string

// Pool defines model for Pool.
type Pool struct {
	ExecutorsCount int    `json:"executorsCount"`
	Name           string `json:"name"`
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
	Color                string              `json:"color"`
	DefaultCallSemantics CallSemantics       `json:"defaultCallSemantics"`
	DefaultGrpcTimeout   int                 `json:"defaultGrpcTimeout"`
	DelayExecutors       int                 `json:"delayExecutors"`
	Environment          string              `json:"environment"`
	GrpcHost             string              `json:"grpcHost"`
	GrpcPort             int                 `json:"grpcPort"`
	HttpHost             string              `json:"httpHost"`
	HttpPort             int                 `json:"httpPort"`
	Id                   int                 `json:"id"`
	LogLevel             *LogLevel           `json:"logLevel,omitempty"`
	MetricsHandler       string              `json:"metricsHandler"`
	Name                 string              `json:"name"`
	ProgrammingLanguage  ProgrammingLanguage `json:"programmingLanguage"`
	ShutdownTimeout      int                 `json:"shutdownTimeout"`
	StatusHandler        string              `json:"statusHandler"`
}

// Stream defines model for Stream.
type Stream struct {
	AppInputType        *AppInputType      `json:"appInputType,omitempty"`
	Duration            *int               `json:"duration,omitempty"`
	FunctionDescription *string            `json:"functionDescription,omitempty"`
	FunctionName        *string            `json:"functionName,omitempty"`
	FunctionPackage     *string            `json:"functionPackage,omitempty"`
	GrpcMethodType      *GrpcMethodType    `json:"grpcMethodType,omitempty"`
	Id                  int                `json:"id"`
	IdEndpoint          *int               `json:"idEndpoint,omitempty"`
	IdService           int                `json:"idService"`
	IdSource            int                `json:"idSource"`
	IdSources           []int              `json:"idSources"`
	JoinStorage         *JoinStorageType   `json:"joinStorage,omitempty"`
	JoinType            *JoinType          `json:"joinType,omitempty"`
	KeyType             *string            `json:"keyType,omitempty"`
	Name                string             `json:"name"`
	Path                *string            `json:"path,omitempty"`
	PublicFunction      *bool              `json:"publicFunction,omitempty"`
	RenewTTL            *bool              `json:"renewTTL,omitempty"`
	Ttl                 *int               `json:"ttl,omitempty"`
	Type                TransformationType `json:"type"`
	ValueType           *string            `json:"valueType,omitempty"`
	XPos                int                `json:"xPos"`
	YPos                int                `json:"yPos"`
}

// StreamApp defines model for StreamApp.
type StreamApp struct {
	DataConnectors []DataConnector `json:"dataConnectors"`
	Endpoints      []Endpoint      `json:"endpoints"`
	Links          []Link          `json:"links"`
	Pools          []Pool          `json:"pools"`
	Services       []Service       `json:"services"`
	Settings       ProjectSettings `json:"settings"`
	Streams        []Stream        `json:"streams"`
	Types          []Type          `json:"types"`
}

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
