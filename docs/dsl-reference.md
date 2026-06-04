# Service Topology DSL Reference

The Service Topology DSL is a YAML file that describes a complete distributed streaming service application. It is the single source of truth for the GoRunDebug toolchain: the code generator reads it to produce typed Go stubs, the runtime loads it at startup to wire up the execution graph, and the visual designer renders it as an interactive dataflow diagram. A DSL file defines which microservices exist, how stream-processing nodes are connected inside each service, what external systems (HTTP, gRPC, Kafka, custom) they integrate with, what data types flow through the graph, and how messages are delivered between nodes.

---

## Top-Level Structure

A DSL file is a single YAML mapping. All top-level keys are maps (not arrays) keyed by a camelCase identifier derived from the entity name.

```yaml
settings:     # ProjectSettings — global project metadata (required)
services:     # map[key]Service  — microservices in the topology (required)
streams:      # (internal) — stream nodes live inside services.*.pipelines.*
dataConnectors: # map[key]DataConnector — external system connections (required)
endpoints:    # (internal) — endpoints live inside dataConnectors.*.endpoints.*
types:        # map[key]Type  — named data types (required)
pools:        # map[key]Pool  — worker pools for async delivery (optional)
modules:      # map[key]Module — shared Go modules (optional)
```

> Note on keys: every entity key is the camelCase form of the entity `name` field, computed by splitting on spaces, underscores, hyphens, and CamelCase boundaries, then lowercasing the first word and title-casing the rest (e.g., `"Hotel Search HTTP"` → `hotelSearchHttp`). Stream nodes and endpoints are nested inside their owning entities rather than listed as flat top-level arrays.

---

## settings

Global metadata for the project.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable project name. Used in generated file headers. |
| `moduleVersion` | string | no | Semantic version tag for module publishing (e.g. `v0.1.0`). Defaults to `v0.0.1`. |
| `repoPath` | string | no | Git repository path for the project root (e.g. `github.com/example/myproject`). When set, `make git-push` also pushes the project root. |

Example:
```yaml
settings:
    name: otp
    moduleVersion: v0.0.1
```

---

## services

A map of microservices. Each service owns a set of stream nodes (grouped into pipelines) and optionally a set of links between those nodes.

### Service fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable service name. Becomes the root directory for generated code. |
| `programmingLanguage` | enum | yes | Target language for code generation. See [ProgrammingLanguage](#programminglanguage). |
| `modulePath` | string | yes | Go module path (e.g. `github.com/example/myservice`). Base import path for generated packages. |
| `golangVersion` | string | no | Minimum Go version for the generated module (e.g. `1.22`). |
| `defaultCallSemantics` | enum | yes | Default delivery semantics for links that do not override it. See [CallSemantics](#callsemantics). |
| `httpHost` | string | yes | Host address for the service HTTP server. |
| `httpPort` | integer | yes | Port for the service HTTP server (0–65535). |
| `grpcHost` | string | yes | Host address for the service gRPC server. |
| `grpcPort` | integer | yes | Port for the service gRPC server (80–65535). |
| `defaultGrpcTimeout` | integer | no | Default gRPC call timeout in milliseconds. |
| `shutdownTimeout` | integer | yes | Maximum milliseconds to wait for graceful shutdown before forcing termination. |
| `delayExecutors` | integer | yes | Number of goroutines dedicated to Delay operator timers (minimum 1). |
| `environment` | string | yes | Deployment environment identifier (e.g. `production`, `staging`). Exposed in generated config. |
| `color` | string | yes | Hex color code for the visual designer (e.g. `#4A90D9`). |
| `statusHandler` | string | yes | URL path for the live topology visualization endpoint (e.g. `status`). Empty to disable. |
| `metricsHandler` | string | yes | URL path for the Prometheus metrics endpoint (e.g. `metrics`). Empty to disable. |
| `logLevel` | enum | no | Logging verbosity. See [LogLevel](#loglevel). |
| `pipelines` | map | yes | Named groups of stream nodes belonging to this service. Keys are pipeline names; values are maps of stream node keys to stream node objects. |
| `links` | map | no | Explicit cross-pipeline or cross-node links within this service. Keys are arbitrary (e.g. `link1`); values are Link objects. |

Example:
```yaml
services:
    hotelSearch:
        color: '#B29BDC'
        defaultCallSemantics: FunctionCall
        defaultGrpcTimeout: 5000
        delayExecutors: 1
        environment: ""
        golangVersion: 1.25.0
        grpcHost: 0.0.0.0
        grpcPort: 9201
        httpHost: 0.0.0.0
        httpPort: 8080
        metricsHandler: metrics
        modulePath: github.com/gorundebug/hotelsearch
        name: HotelSearch
        pipelines:
            requestValidation:
                searchInput:
                    type: Input
                    endpoint: searchHotels
                    ...
        shutdownTimeout: 30000
        statusHandler: status
```

### ProgrammingLanguage

| Value | Integer | Description |
|---|---|---|
| `GoLang` | 1 | Go (only fully supported language at this time) |
| `Cpp` | 2 | C++ (reserved) |
| `Python` | 3 | Python (reserved) |

### LogLevel

| Value | Description |
|---|---|
| `CRITICAL` | Critical errors only |
| `FATAL` | Fatal errors only |
| `ERROR` | Errors |
| `WARNING` | Warnings and above |
| `INFO` | Informational messages and above |
| `DEBUG` | All messages including debug |

---

## Stream nodes (inside services.*.pipelines.*)

Stream nodes are the operators of the dataflow graph. They are not a top-level map; instead, each node lives inside `services.<serviceKey>.pipelines.<pipelineName>.<nodeKey>`.

The node key is the camelCase form of the node's `name` field. A node in the YAML is referenced by other nodes via this key in their `source` / `sources` fields, and by Input/Sink nodes in their `endpoint` field.

### Common stream node fields

These fields apply to all (or most) node types:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable node name shown in the visual designer. |
| `type` | enum | yes | Operator type. See [TransformationType](#transformationtype). |
| `source` | string | no | Key of the single upstream node. Used by operators with one input (Map, Filter, FlatMap, Process, KeyBy, Split, Case, Delay, Sink, Error, When). |
| `sources` | []string | no | Keys of multiple upstream nodes. Used by Merge and MultiJoin. Mutually exclusive with `source` for those types. |
| `valueType` | string | no | Key of the output message type for this node (refers to a key in the top-level `types` map). Required for most operators; not used by Filter, Split, Case, CycleLink. |
| `keyType` | string | no | Key of the key type for keyed nodes (KeyBy, Join, MultiJoin). Must be a comparable Go type. |
| `errorStream` | string | no | Key of the companion Error node for Process and Input nodes that declare an error output stream. |
| `endpoint` | string | no | Key (within the owning data connector's `endpoints` map) of the endpoint used by Input and Sink nodes. |
| `functionName` | string | no | Name of the generated Go struct for the business-logic implementation. |
| `functionPackage` | string | no | Subdirectory under the functions package where the generated stub is placed. |
| `publicFunction` | boolean | no | When true, the stub is placed in `pkg/functions` (shared); when false, in `internal/functions`. |
| `functionDescription` | string | no | Doc comment added to the generated struct. |
| `functionInitializerGroup` | string | no | Dependency-injection initializer group name. |
| `functionModule` | string | no | Name of a module from the top-level `modules` map. When set with `publicFunction: true`, the stub is placed in that module instead of the service module. |
| `xPos` | number | yes | Horizontal canvas position in the visual designer. |
| `yPos` | number | yes | Vertical canvas position in the visual designer. |

### TransformationType

| Value | Description |
|---|---|
| `Input` | Ingests data from an external source via a data connector endpoint. |
| `Map` | Transforms each element T → R. |
| `Filter` | Passes elements satisfying a predicate; discards others. |
| `Join` | Combines two keyed streams (left `source` + right via `sources[0]`): `KeyValue[K,T1] + KeyValue[K,T2] → R`. |
| `MultiJoin` | Combines N keyed streams into a single output. |
| `Process` | Transforms T → R with a side-output error stream E. |
| `FlatMap` | Expands one element T into zero or more R via a user function. |
| `FlatMapIterable` | Expands T into an iterable of R without a user function (input must be iterable). |
| `KeyBy` | Partitions a stream into keyed sub-streams: T → `KeyValue[K,V]`. |
| `Merge` | Merges multiple streams of the same type T into one. |
| `Split` | Broadcasts one stream to multiple downstream consumers without user logic. |
| `Case` | Routes each element to one of N typed branches (switch/case). The branches are `When` nodes that reference this node as their `source`. |
| `When` | A typed branch of a `Case` node. References its parent Case node via `source`. |
| `Sink` | Sends data to an external destination via a data connector endpoint. |
| `CycleLink` | Feeds output back into an earlier node in the graph (creates a feedback loop). |
| `Error` | Receives error elements from a Process node's error output or an Input node that has an error stream. References the producer node via `source`. |
| `Delay` | Defers downstream delivery by a computed or fixed duration. |

### Type-specific fields

#### Input

| Field | Type | Description |
|---|---|---|
| `endpoint` | string | Required. Key of the endpoint in the data connector's `endpoints` map. |
| `valueType` | string | Type of the message produced by this input. |
| `errorStream` | string | Key of a companion Error node for parse/transport errors. |
| `source` | string | Key of the upstream node that receives the result after a round-trip (used when the input is also a sink result consumer in bidirectional patterns). |

Example (from `otp.yaml`):
```yaml
searchInput:
    endpoint: searchHotels
    errorStream: searchInputError
    name: Search Input
    source: joinSearchApiResponse
    type: Input
    valueType: searchRequest
    xPos: 3529
    yPos: -667
```

#### Map

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `valueType` | string | Output type name. |
| `functionName` | string | Generated struct name. |

#### Filter

No type-specific fields beyond the common ones. `valueType` is not applicable (output type equals input type). Example:
```yaml
validateSearchParams:
    functionName: ValidateSearchParams
    name: Validate Search Params
    source: searchInput
    type: Filter
    xPos: 224
    yPos: 150
```

#### FlatMap

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `valueType` | string | Output element type. |
| `functionName` | string | Generated struct name. |

#### FlatMapIterable

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `valueType` | string | Output element type. No `functionName` — no user function needed. |

#### Process

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `errorStream` | string | Key of the companion Error node. |
| `pattern` | enum | Execution pattern. See [ProcessPattern](#processpattern). |
| `functionName` | string | Generated struct name. |

#### ProcessPattern

| Value | Description |
|---|---|
| `Execute` | Handler performs a side effect and may emit zero or more results synchronously. |
| `Collect` | Handler accumulates results and emits them in batch. |

#### KeyBy

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `keyType` | string | Required. Key type name (must be comparable). |
| `valueType` | string | Value type name (the V in `KeyValue[K,V]`). |
| `functionName` | string | Generated struct name for the key-extraction function. |

#### Join

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Key of the left (primary) upstream KeyBy node. |
| `sources` | []string | Required. One-element list: key of the right upstream KeyBy node. |
| `valueType` | string | Output type after joining. |
| `joinType` | enum | Join semantics. See [JoinType](#jointype). |
| `joinStorage` | enum | State storage backend. See [JoinStorageType](#joinstoragetype). |
| `ttl` | integer | State time-to-live in milliseconds. After TTL expires without a match, state is discarded. |
| `renewTTL` | boolean | When true, TTL resets each time a new element arrives for a key. |
| `functionName` | string | Generated struct name for the combine function. |

Example:
```yaml
joinInventoryApiResponse:
    functionName: JoinInventoryApiResponse
    joinStorage: HashMap
    joinType: Left
    name: Join Inventory API Response
    renewTTL: false
    source: keyByInventoryResponse
    sources:
        - keyByInventoryError
    ttl: 0
    type: Join
    valueType: availabilityApiResponse
    xPos: 1833
    yPos: 858
```

#### JoinType

| Value | Description |
|---|---|
| `Inner` | Output only when both sides have a match for the same key. |
| `Left` | Output when the left side arrives; right side is optional. |
| `Right` | Output when the right side arrives; left side is optional. |
| `Outer` | Output when either side arrives. |

#### JoinStorageType

| Value | Description |
|---|---|
| `HashMap` | In-memory hash map. Fast; lost on restart. |
| `RocksDB` | Persistent RocksDB store. Survives restarts. |
| `Aerospike` | Distributed Aerospike store. |

#### MultiJoin

Same fields as Join, but `sources` contains keys for all right-side inputs (one or more). Does not have a `joinType` field (always outer by position).

#### Merge

| Field | Type | Description |
|---|---|---|
| `sources` | []string | Required. Two or more upstream node keys. All sources must produce the same type. |

No `functionName` — no user function needed. Example:
```yaml
mergeInventoryErrors:
    name: Merge Inventory Errors
    sources:
        - mapAvailabilityErrorToResponse
        - mapLoyaltyErrorToResponse
    type: Merge
    xPos: 1448
    yPos: 607
```

#### Split

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |

No `functionName`. All downstream links from a Split node receive the same element.

#### Case

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `functionName` | string | Generated struct name for the branch-selector function. Returns an integer index selecting the branch. |

Branches are `When` nodes that reference the `Case` node as their `source`. The function returns 0 for branch 0, 1 for branch 1, etc. Example:
```yaml
routeByRoomCategory:
    functionName: RouteByRoomCategory
    name: Route By Room Category
    source: normalizeSearchRequest
    type: Case
    xPos: 708
    yPos: 320
```

#### When

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Key of the parent Case node. |
| `valueType` | string | Type of messages routed to this branch. |

No `functionName`. Example:
```yaml
standardCategoryRoute:
    name: Standard Category Route
    source: routeByRoomCategory
    type: When
    valueType: normalizedSearchRequest
    xPos: 946
    yPos: 201
```

#### Sink

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `endpoint` | string | Required. Key of the endpoint in the data connector's `endpoints` map. |
| `valueType` | string | Type of the response fed back into the pipeline (for sinks that return a result, e.g. gRPC calls). |
| `errorStream` | string | Key of a companion Error node for transport errors. |

Example:
```yaml
sendToInventory:
    endpoint: searchRooms
    errorStream: inventoryCallError
    name: Send To Inventory
    source: mergeRoomQueries
    type: Sink
    valueType: availabilityApiResponse
    xPos: 1628
    yPos: 278
```

#### CycleLink

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node whose output feeds back into an earlier node via a Link. |

No `functionName`. The actual cycle is declared in the `links` section of the service.

#### Error

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Key of the Process or Input node that produces errors. |
| `valueType` | string | Error type name. |

No `functionName`. Declared by the parent node's `errorStream` field pointing to this node's key.

#### Delay

| Field | Type | Description |
|---|---|---|
| `source` | string | Required. Upstream node key. |
| `duration` | integer | Fixed delay in milliseconds. Used when delay is not computed per-element. |
| `functionName` | string | Generated struct name for the per-element delay-computation function. |

---

## Service links (inside services.*.links)

Links are explicit directed edges. Within-pipeline ordering is usually inferred from `source`/`sources` references, so explicit `links` are only needed for cross-pipeline edges, cycle links, or when delivery semantics differ from the service default.

| Field | Type | Required | Description |
|---|---|---|---|
| `from` | string | yes | Key of the upstream node. |
| `to` | string | yes | Key of the downstream node. |
| `callSemantics` | enum/object | no | Delivery semantics for this edge. See [CallSemantics](#callsemantics). |

### CallSemantics

In the YAML format, `callSemantics` inside a link is a nested object selecting exactly one delivery mode, not a plain string:

```yaml
callSemantics:
    functionCall: {}          # synchronous in-goroutine call

callSemantics:
    taskPool:
        poolName: myPool      # FIFO worker pool

callSemantics:
    priorityTaskPool:
        poolName: myPool      # priority worker pool
        priority: 10          # higher value = processed first
```

The service-level `defaultCallSemantics` is a plain enum string (`FunctionCall`, `TaskPool`, `PriorityTaskPool`).

| Mode | Description |
|---|---|
| `FunctionCall` | Synchronous in-goroutine call; lowest latency, no buffering. |
| `TaskPool` | Enqueue to a named FIFO worker pool; decouples producer from consumer. |
| `PriorityTaskPool` | Enqueue to a named priority worker pool; higher-priority messages are processed first. |

---

## dataConnectors

A map of external system connections. Each data connector represents one external system; specific routes, topics, or methods are defined as endpoints nested inside it.

### DataConnector common fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable connector name. Used in generated server/client struct names. |
| `type` | enum | yes | External system type. See [DataConnectorType](#dataconnectortype). |
| `implementation` | enum | yes | Client/server library. See [DataConnectorImplementation](#dataconnectorimplementation). |
| `module` | string | no | Go module name containing generated gRPC protobuf types (gRPC connectors) or shared API types (HTTP connectors). |
| `endpoints` | map | no | Named endpoint objects nested under the connector. Keys are camelCase endpoint names. |

### DataConnectorType

| Value | Description |
|---|---|
| `HTTP` | net/http server or client. |
| `gRPC` | gRPC server or client. |
| `Kafka` | Kafka cluster (Sarama). |
| `Custom` | In-process local connector for testing or internal pipelines. |

### DataConnectorImplementation

| Value | Description |
|---|---|
| `net/http` | Go standard library HTTP. |
| `google/grpc` | Official gRPC-Go library. |
| `IBM/Sarama` | Sarama Kafka client. |
| `function` | In-process custom connector (no network). |

### HTTP connector additional fields

| Field | Type | Description |
|---|---|---|
| `host` | string | Hostname or IP address for outgoing HTTP connections. |
| `port` | integer | Port for outgoing HTTP connections (80–65535). |
| `useDedicatedListener` | boolean | When true, this connector starts its own `net/http` listener. When false (default), it shares the service's default HTTP mux. |

### gRPC connector additional fields

| Field | Type | Description |
|---|---|---|
| `address` | string | gRPC connection address (e.g. `dns:///localhost:9202`). |

### Kafka connector additional fields

| Field | Type | Description |
|---|---|---|
| `brokers` | string | Comma-separated Kafka broker addresses (e.g. `kafka1:9092,kafka2:9092`). |
| `version` | string | Kafka protocol version expected by the broker (e.g. `2.8.0`). |
| `dialTimeout` | number | Connection dial timeout in milliseconds. |
| `usePartitioner` | boolean | When true, a custom partitioner function is generated for produced messages. |
| `async` | boolean | When true, the Kafka producer operates in async mode. |

Example:
```yaml
dataConnectors:
    hotelInventoryGrpc:
        address: dns:///localhost:9202
        implementation: google/grpc
        module: hotelsearch_api
        name: Hotel Inventory GRPC
        type: gRPC
        endpoints:
            searchRooms:
                grpcMethodType: NoStreaming
                methodName: SearchRooms
                functionName: SearchRoomsEndpoint
                name: Search Rooms
                publicFunction: false
    hotelSearchHttp:
        implementation: net/http
        module: hotelsearch_api
        name: Hotel Search HTTP
        type: HTTP
        useDedicatedListener: false
        endpoints:
            searchHotels:
                httpMethodType: POST
                name: Search Hotels
                path: /v1/hotels/search
                functionName: SearchHotelsHandler
                publicFunction: false
```

---

## Endpoints (inside dataConnectors.*.endpoints)

Endpoints are specific entry or exit points on a data connector: an HTTP route, a Kafka topic, or a gRPC method. They are referenced by `Input` and `Sink` stream nodes via the `endpoint` field.

### Endpoint common fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable endpoint name shown in the visual designer. |
| `functionName` | string | no | Name of the generated Go handler struct for this endpoint. |
| `functionPackage` | string | no | Subdirectory under the functions package for the generated handler. |
| `publicFunction` | boolean | no | When true, the handler is placed in `pkg/functions` (shared). |
| `functionDescription` | string | no | Doc comment for the generated handler struct. |
| `functionInitializerGroup` | string | no | Dependency-injection initializer group for the generated handler. |
| `functionModule` | string | no | Name of a module from the top-level `modules` map. |

### HTTP endpoint additional fields

| Field | Type | Description |
|---|---|---|
| `httpMethodType` | enum | HTTP verb: `GET` or `POST`. |
| `path` | string | URL path (e.g. `/v1/hotels/search`). |

### gRPC endpoint additional fields

| Field | Type | Description |
|---|---|---|
| `grpcMethodType` | enum | gRPC streaming mode. See [GrpcMethodType](#grpcmethodtype). |
| `methodName` | string | gRPC method name as defined in the proto file. |

#### GrpcMethodType

| Value | Description |
|---|---|
| `NoStreaming` | Unary RPC: one request, one response. |
| `ClientStreaming` | Client sends a stream; server replies once. |
| `ServerStreaming` | Client sends once; server replies with a stream. |
| `BidirectionalStreaming` | Both sides stream concurrently. |

### Kafka endpoint additional fields

| Field | Type | Description |
|---|---|---|
| `topic` | string | Kafka topic name. |
| `consumerGroup` | string | Kafka consumer group ID (source endpoints). |
| `createTopic` | boolean | When true, the service attempts to create the topic on startup if it does not exist. |
| `partitions` | integer | Number of partitions (used when `createTopic` is true). |
| `replicationFactor` | integer | Replication factor (used when `createTopic` is true). |

---

## types

A map of named data types. Types are referenced by stream nodes via `valueType` and `keyType`. The code generator uses type definitions to produce correct Go type references and import statements.

The key of each entry in the `types` map is the camelCase form of the type `name`.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Canonical type name referenced by nodes. Only required in YAML when it differs from the camelCase key (i.e. when the name contains special casing not recoverable from the key). |
| `type` | enum | yes | Primitive or composite data type. See [DataType](#datatype). |
| `description` | string | no | Human-readable description; used as a doc comment in generated code. |
| `definitionFormat` | enum | no | Serialization format. See [TypeDefinitionFormat](#typedefinitionformat). |
| `module` | string | no | Name of a module from the top-level `modules` map that owns this type's definition. |
| `package` | string | no | Go package name alias when it differs from the last path component of the import. |
| `transferByValue` | boolean | no | When true, the type is passed by value in generated signatures. Default false (passed as pointer). |
| `useAlias` | boolean | no | When true, the generated Go code uses a type alias instead of a full struct definition. |
| `publicType` | boolean | no | When true, this type is shared across services and placed in the shared types package. |
| `valueType` | string | no | Element type for `array`; value type for `map` composite types. |
| `keyType` | string | no | Key type for `map` composite types. |
| `typeDefinitionLang1` | string | no | Inline Go type definition (e.g. `interface{}`, `map[string]int`). Used when the type has no external package. |
| `typeDefinitionLang2` | string | no | Inline C++ type definition. |
| `typeImportLang1` | string | no | Go import path for the package defining this type (e.g. `github.com/example/types`). |
| `typeImportLang2` | string | no | C++ include path. |

### DataType

| Value | Description |
|---|---|
| `int`, `uint`, `byte`, `char` | Basic integer types |
| `boolean` | Boolean |
| `string`, `unicode string` | String types |
| `float`, `double` | Floating-point types |
| `int8`..`int64`, `uint8`..`uint64` | Sized integer types |
| `any` | Untyped / `interface{}` |
| `error` | Go `error` interface |
| `array` | Slice; set `valueType` for the element type |
| `map` | Map; set `keyType` and `valueType` |
| `struct` | Named struct defined externally or inline |
| `custom` | User-defined type; reference via `typeImportLang1` or `module` |

### TypeDefinitionFormat

| Value | Description |
|---|---|
| `Native` | Language-native encoding (Go encoding/json, etc.) |
| `Protobuf` | Protocol Buffers |
| `FlatBuffers` | FlatBuffers |
| `CapNProto` | Cap'n Proto |
| `OpenAPI` | JSON encoding for types generated from an OpenAPI spec (oapi-codegen) |

Example:
```yaml
types:
    searchRequest:
        definitionFormat: Native
        description: 'Hotel room search request: check_in, check_out (YYYY-MM-DD strings), ...'
        module: model
        name: SearchRequest
        publicType: false
        transferByValue: false
        type: struct
        useAlias: false
    requestId:
        definitionFormat: Native
        module: model
        name: RequestID
        publicType: true
        type: string
```

---

## pools

A map of named worker pools for async message delivery. Referenced by links with `callSemantics: taskPool` or `callSemantics: priorityTaskPool`.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Pool name. Referenced in link `callSemantics.taskPool.poolName` or `callSemantics.priorityTaskPool.poolName`. |
| `executorsCount` | integer | yes | Number of worker goroutines in the pool (minimum 1). |

Example:
```yaml
pools:
    defaultPool:
        name: defaultPool
        executorsCount: 4
```

---

## modules

A map of additional Go modules used to host public (shared) functions or types. When a stream node or endpoint has `publicFunction: true` and references a module via `functionModule`, the generated stub is placed in that module's package tree rather than the service's own module.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Module identifier. Referenced by `functionModule` in stream nodes and endpoints, and by `module` in types. |
| `modulePath` | string | yes | Go module path (e.g. `github.com/example/shared`). Base import path for generated packages. |
| `golangVersion` | string | yes | Minimum Go version for this module (e.g. `1.22`). |

Example:
```yaml
modules:
    hotelsearchApi:
        modulePath: github.com/gorundebug/hotelsearch_api
        name: hotelsearch_api
    model:
        modulePath: github.com/gorundebug/hotel_model
        name: model
```

---

## Cross-references between entities

| Reference | From | Target |
|---|---|---|
| `services.<key>.pipelines.<pipeline>.<nodeKey>.source` | Stream node | Key of another stream node in the same service (any pipeline) |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.sources[]` | Stream node | Keys of other stream nodes in the same service |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.errorStream` | Stream node (Process, Input) | Key of a companion Error node in the same service |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.endpoint` | Input / Sink node | Key of an endpoint within `dataConnectors.<dcKey>.endpoints` |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.valueType` | Stream node | camelCase key within the top-level `types` map |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.keyType` | KeyBy / Join / MultiJoin node | camelCase key within the top-level `types` map |
| `services.<key>.pipelines.<pipeline>.<nodeKey>.functionModule` | Stream node | `name` value of an entry in the top-level `modules` map |
| `services.<key>.links.<linkKey>.from` / `.to` | Link | Keys of stream nodes within the same service |
| `services.<key>.links.<linkKey>.callSemantics.taskPool.poolName` | Link | `name` of an entry in the top-level `pools` map |
| `services.<key>.links.<linkKey>.callSemantics.priorityTaskPool.poolName` | Link | `name` of an entry in the top-level `pools` map |
| `dataConnectors.<key>.endpoints.<epKey>` | Endpoint (nested) | Owned by the enclosing data connector |
| `dataConnectors.<key>.endpoints.<epKey>.functionModule` | Endpoint | `name` of an entry in the top-level `modules` map |
| `dataConnectors.<key>.module` | DataConnector | `name` of an entry in the top-level `modules` map |
| `types.<key>.module` | Type | `name` of an entry in the top-level `modules` map |
| `types.<key>.valueType` / `.keyType` | Composite type | Name of another type (raw name, not necessarily a key in the `types` map) |
