# servicelib — Architecture Reference
Module: `github.com/gorundebug/servicelib`

## Details

### Package Map

```
servicelib/
├── api/                    OpenAPI-generated enums and config models
│                           (DataConnectorType, DataFormat, HTTPMethodType, …)
│
├── operators/              Stream operator implementations + function interfaces
│                           FilterFunction, MapFunction, FlatMapFunction,
│                           JoinFunction, MultiJoinFunction, ProcessFunction,
│                           DelayFunction, BuildSwitchFunction, When
│                           MakeMapStream, MakeFilterStream, MakeSinkStream, …
│
├── transformation/         Public facade — import this, not operators or runtime
│                           Re-exports all operators.* types via Go 1.24 generic aliases
│                           Factory functions: Map, Filter, FlatMap, Process, Input,
│                           Join, MultiJoin, KeyBy, Merge, Link, Split, Delay,
│                           CaseStream, WhenStream, Sink, SinkWithResult
│
├── runtime/                Core execution engine
│   ├── common.go           All stream interface definitions (see Stream Types below)
│   ├── collector.go        Collect[T] interface + CollectFunc[T]
│   ├── datasource.go       InputDataSource, DataSourceEndpoint, DataSourceEndpointConsumer[T,R,E]
│   ├── datasink.go         OutputDataSink, DataSinkEndpoint, DataSinkEndpointConsumer[T,R]
│   ├── serviceapp.go       ServiceApp — main entry point, HTTP mux, lifecycle
│   ├── runtime.go          ServiceExecutionRuntime
│   ├── config/             Typed config structs for all stream and connector types
│   │   ├── config.go       RuntimeConfig, transformationNameMap
│   │   ├── stream_types.go *StreamConfig per transformation type
│   │   ├── endpoint_types.go HttpEndpointConfig, GrpcEndpointConfig, KafkaEndpointConfig, CustomEndpointConfig
│   │   └── dataconnector_types.go HttpDataConnectorConfig, GrpcDataConnectorConfig, KafkaDataConnectorConfig, …
│   ├── datastruct/         KeyValue[K,V]
│   ├── environment/        ServiceEnvironment, ServiceDependencies, metrics/logging interfaces
│   ├── logging/logrus/     Logrus logger adapter
│   ├── telemetry/prometheus/ Prometheus metrics adapter
│   ├── pool/               TaskPoolImpl (FIFO) + PriorityTaskPoolImpl (heap)
│   │                       Both fixed: Stop() always broadcasts to all workers
│   ├── serde/              Serializer interface + implementations
│   └── store/              RotatingMap[K,V] (two-bucket GC-friendly map), JoinStore
│
├── datasource/             Incoming data → pipeline
│   ├── grpc/               nostreaming, serverstreaming, clientstreaming, bidistreaming
│   ├── http/               nethttp.go — HTTP server (dedicated or shared mux)
│   ├── kafka/              sarama.go — Sarama Kafka consumer
│   └── localsource/        custom.go — in-process source
│
├── datasink/               Pipeline → outgoing data
│   ├── grpc/               nostreaming, serverstreaming, clientstreaming, bidistreaming
│   ├── http/               nethttp.go — outgoing HTTP client (NEW)
│   ├── kafka/              sarama.go — Sarama Kafka producer
│   ├── localsink/          custom.go — in-process sink
│   └── datasink.go         Facade: one function per sink variant, import only this
│
└── tests/mockservice/      Integration test service
```

---

### Core Stream Type Hierarchy

```
Stream                              — name, id, config, env, Build()
└── TypedSerializedStream[T]        — + GetSerde()
    └── TypedStream[T]              — + GetConsumer/SetConsumer
        ├── TypedConsumedStream[T]  — + Consume(ctx, T)   (filter, merge, link, delay)
        ├── TypedTransformConsumedStream[T,R]   — input T, output R  (map, flatmap, keyby)
        │   ├── TypedProcessConsumedStream[T,R,E]   — + GetErrorStream()
        │   ├── TypedJoinConsumedStream[K,T1,T2,R]  — + ConsumeRight()
        │   └── TypedMultiJoinConsumedStream[K,T,R] — + ConsumeRight()
        ├── TypedInputStream[T,R,E] — source stream: GetEndpointId, SetResultConsumer, SetSource
        ├── TypedSinkStream[T,E]    — sink (no result back): SetSinkConsumer, GetErrorStream
        ├── TypedSinkStreamWithResult[T,R,E]    — sink with result fed back: ConsumeResult
        ├── TypedCaseStream[T]           — + AddStream(WhenStream)
        └── TypedWhenStream[T]      — + ConsumeCase(ctx, any), SetIndex, GetWhenConsumer, GetType
```

`WhenStream` (non-generic) is embedded in `TypedWhenStream[T]` and used by `TypedCaseStream.AddStream`.

Key helpers:
- `Collect[T]` — `Out(ctx, T)` interface; `CollectFunc[T]` is `func(ctx, T)` adapter
- `Consumer[T]` — `Consume(ctx, T)` interface
- `SinkCallback[T]` — `Done(ctx, T, error)` optional interface on sink consumers

---

### DataSource / DataSink Infrastructure Pattern

Every connector (source or sink) follows the same three-layer structure:

```
DataConnector (config, id, name)
└── Endpoint (id, config, DataConnector ref)
    └── EndpointConsumer (typed, holds stream ref, implements Consume[T])
```

**Source side** (`runtime/datasource.go`):
- `InputDataSource` embeds `DataSourceEndpoint` list
- `DataSourceEndpointConsumer[T,R,E]` — base for all typed source consumers
- Endpoint exposes `OnMissingStreamID`, `OnLateResult`, `OnUnknownMessageID`, `OnDuplicateMessageID`, `OnInvalidHTTPMethod`

**Sink side** (`runtime/datasink.go`):
- `OutputDataSink` embeds `DataSinkEndpoint` list
- `DataSinkEndpointConsumer[T,R]` — base for simple sinks (localsink, kafka)
- For result-bearing sinks (grpc, http) — consumer is built manually without base struct

**Lifecycle** (called by `ServiceApp`):
```
DataConnector.Start(ctx) → foreach Endpoint.Start(ctx)
DataConnector.Stop(ctx)  → foreach Endpoint.Stop(ctx) [concurrent, with WaitGroup]
```

---

## EndpointHandler Lifecycle by Connector Type

### HTTP Source (`datasource/http`) — incoming requests
```
BeginRequest(ctx, sc, HandlerData) → (ctx, HandlerState, error)
ConsumeMessage(ctx, sc, state, HandlerData, ResultContext) → error
  └── sc.Collect.Out(ctx, value)          push T into pipeline
  └── resultCtx.SetResultCallback(id, cb) register result callback
  └── resultCtx.Done()                    signal response complete
GetMessageID(ctx, sc, state, R) → string  correlate result R to request
EndRequest(ctx, sc, err, state, HandlerData)
```
`HandlerData` = `{Writer http.ResponseWriter, Request *http.Request}`
Result R arrives via `consumeResult` → looked up in `RotatingMap` by streamID → callback invoked.

### HTTP Sink (`datasink/http`) — outgoing requests
```
MakeClient() → *http.Client                      called once at construction
BeginRequest(ctx, sc) → (ctx, HandlerState, error)
ConsumeMessage(ctx, sc, state, T, *Requester) → error
  └── req.NewRequest(method, url, body) → (*http.Request, error)
  └── req.Header().Set(...)
HandleResponse(ctx, sc, state, Response) → error
  └── sc.Collect.Out(ctx, result)       push R into pipeline
EndRequest(ctx, sc, err, state)
```
`Response` wraps `*http.Response` — access `StatusCode`, `Body`, etc. without importing `net/http`.

### gRPC Sink (`datasink/grpc`) — 4 variants
```
BeginRequest(ctx, sc) → (ctx, HandlerState, error)
ConsumeMessage(ctx, sc, state, T, Sender[ReqT], ResultContext) → error
  └── sender.Send(req)                   build and send gRPC request
  └── resultCtx.Done()                   bidi/client-streaming: signal end
HandleResponse(ctx, sc, state, ResR) → error
  └── sc.Collect.Out(ctx, result)
EndRequest(ctx, sc, err, state)
```

### Kafka / LocalSink (`datasink/kafka`, `datasink/localsink`)
```
GetStreamID(ctx, T) → string
BeginRequest(ctx, stream) → (ctx, HandlerState)   ← no error return
ConsumeMessage(ctx, stream, state, T, Collect[R]) → error
EndRequest(ctx, stream, err, state)
```

### gRPC Source (`datasource/grpc`) — same 4 variants as sink but reversed
### Kafka Source / LocalSource — same pattern as Kafka/LocalSink handlers

---

## Config System

Config is loaded by `runtime/config/config.go` into `RuntimeConfig`.
Key maps: `GetDataConnectorByID(id)`, `GetEndpointConfigByID(id)`, `GetStreamConfigByID(id)`.

Each transformation type has a dedicated `*StreamConfig` struct in `stream_types.go`.
`transformationNameMap` maps `api.TransformationType` → config struct factory.

Endpoint configs:
- `HttpEndpointConfig` — Path, HttpMethodType, RequestFormat, ResponseFormat
- `GrpcEndpointConfig` — GrpcMethodType, IdDataConnector
- `KafkaEndpointConfig` — Topic, Partitions, ConsumerGroup, CreateTopic
- `CustomEndpointConfig` — minimal, for localsource/localsink

Data connector configs:
- `HttpDataConnectorConfig` — Host, Port, UseDedicatedListener, Implementation
- `GrpcDataConnectorConfig` — Implementation, ProgrammingLanguage
- `KafkaDataConnectorConfig` — Brokers, Version, DialTimeout, Async, UsePartitioner

---

## Key Conventions

- **Three type params** `[T, R, E any]`: T = pipeline value, R = result fed back, E = error type
- **`operators` is top-level**, not inside `runtime` — avoids sub-package importing parent
- **`transformation` is the only import users need** — re-exports operators + runtime types
- **`datasink/datasink.go`** — facade, one function per sink variant
- **`RotatingMap`** — two-bucket map for pending requests; rotation lets GC reclaim over-allocated buckets after burst traffic (entries are NOT TTL-expired, they are preserved)
- **Stream IDs** — propagated via `context` using `runtime.WithStreamId` / `runtime.StreamIdFromContext`; used to correlate async results back to originating requests
- **Pool `Stop()` fix** — both `TaskPoolImpl` and `PriorityTaskPoolImpl`: `done=true` + `Broadcast()` are always called unconditionally (previous bug: guarded by queue length check)
