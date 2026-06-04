<div align="center">

# Service Topology Runtime

**Executable architecture for Go services**

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-7c3aed?style=flat-square)](https://opensource.org/licenses/MIT)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-enabled-f5a623?style=flat-square)](https://opentelemetry.io/)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-e6522c?style=flat-square)](https://prometheus.io/)

Design your service as a typed topology graph.  
Generate the infrastructure. Implement only the business logic.

**[gorundebug.com](https://www.gorundebug.com)**

</div>

---

## The Idea

Modern backend services contain two very different kinds of code:

| Layer | What it is | Who should write it |
|---|---|---|
| **Infrastructure** | routing, transports, serialization, lifecycle, metrics, tracing | The framework |
| **Business Logic** | validation, transformations, integrations, decisions | You |

This project eliminates the first category entirely.

You describe the service as a **typed dataflow graph**. The framework generates connectors, handlers, lifecycle glue, and runtime wiring. You implement only the nodes.

---

## How It Works

```
Visual Designer  →  Topology Definition  →  Code Generator
                                                    ↓
                              Your Business Logic  ←  Generated Typed Stubs
                                      ↓
                               Runtime Execution
```

### Example topology

```text
HTTP Input  →  ValidateUser  →  EnrichProfile  →  PublishKafkaEvent  →  HTTP Response
```

### What you implement

```go
type ValidateUser struct{}

func (v *ValidateUser) Process(
    ctx    context.Context,
    input  CreateUserRequest,
    collect runtime.Collect[ValidatedUser],
) error {
    if input.Email == "" {
        return errors.New("email required")
    }
    collect.Out(ctx, ValidatedUser{Email: input.Email})
    return nil
}
```

No HTTP routing. No transport handling. No serialization code. Only business logic.

---

## Operators

| Operator | Purpose |
|---|---|
| `Input` | Receive data from external systems |
| `Sink` | Send data to external systems |
| `Map` | Transform one value into another |
| `Filter` | Conditionally pass values |
| `FlatMap` | Expand one value into many |
| `Process` | Transform with side-output error stream |
| `KeyBy` | Partition stream by key |
| `Join` | Combine two keyed streams |
| `MultiJoin` | Combine multiple keyed streams |
| `Merge` | Merge streams |
| `Split` | Duplicate streams |
| `Case` | Branch by condition |
| `Delay` | Deferred delivery |
| `CycleLink` | Feedback loops |

---

## Connectors

**Data Sources** — HTTP · gRPC (unary, client-streaming, server-streaming, bidi) · Kafka · Local in-process

**Data Sinks** — HTTP · gRPC · Kafka · Local in-process

Every connector follows the same lifecycle:

```
BeginRequest  →  ConsumeMessage  →  HandleResponse  →  EndRequest
```

---

## Observability — Built In

No opt-in required. Every service gets full observability from day one.

### Distributed Tracing (OpenTelemetry)

Automatic spans at every layer of the request path:

```
grpc.server              ← transport-level, via otelgrpc
  └─ grpc.input          ← endpoint span  {stream, endpoint, stream_id}
       └─ stream.call    ← per-operator span  {stream}
            └─ stream.call
                 └─ grpc.output    ← sink span  {stream, endpoint}
                      └─ grpc.client   ← transport-level, via otelgrpc
```

Per-request sampling via `X-Trace: 1` header (HTTP) or `x-trace` metadata (gRPC) — no global flag needed.

### Metrics (Prometheus)

**Datasource endpoint metrics** (`datasource_endpoint_*`):

| Metric | Type | Description |
|---|---|---|
| `messages_total` | counter | Successfully processed messages |
| `request_duration_seconds` | histogram | Request duration |
| `active_requests` | gauge | Requests currently in flight |
| `pending_requests` | gauge | Requests awaiting a pipeline result |
| `pending_oldest_age_seconds` | gauge (observable) | Age of the oldest stuck pending request |
| `events_total{event=...}` | counter | `request_error` · `late_result` · `missing_stream_id` · `unknown_message_id` · `duplicate_message_id` · `begin_request_failed` |

`pending_oldest_age_seconds` is computed at scrape time — it never goes stale even under zero traffic.

### Grafana Dashboards

Pre-built Jsonnet dashboards for every subsystem:

```
01_service.jsonnet       Service health overview
02_streams.jsonnet       Stream processing metrics
03_datasource.jsonnet    Datasource endpoints + pending visibility
04_datasink.jsonnet      Datasink endpoints
05_pools.jsonnet         Worker pool utilization
06_storage.jsonnet       Storage layer
07_http_server.jsonnet   HTTP server transport
08_http_client.jsonnet   HTTP client transport
09_grpc_server.jsonnet   gRPC server transport
10_grpc_client.jsonnet   gRPC client transport
11_runtime.jsonnet       Go runtime (GC, goroutines, memory)
```

---

## Live Topology Visualization

The running service can expose its own topology graph showing nodes, edges, stream types, service boundaries, and live call counts.

**Architecture diagrams cannot drift from reality** — the runtime graph is the same graph used for generation and execution.

---

## AI-Agent Friendly by Design

Generated stubs are intentionally designed for AI-assisted implementation:

- Explicit named types with clear lifecycle documentation
- Compile-time interface validation
- `TODO` implementation points with full context

An AI coding agent can implement a node with minimal context because the architecture is explicit, the contracts are typed, and responsibilities are isolated. The topology constrains the problem space.

---

## Why This Is Different

| | Traditional | Service Topology Runtime |
|---|---|---|
| Architecture docs | Drift from reality | Graph *is* the source of truth |
| Service wiring | Written by hand | Generated automatically |
| Observability | Added later — or never | Built in from day one |
| AI code generation | Generates entire uncontrolled apps | AI implements focused functions |
| Debugging | Reading logs | Execution graph + full traces |

---

## Mental Model

```
┌─────────────┬───────────────────────────────────────┐
│ Graph       │ Architecture and topology              │
├─────────────┼───────────────────────────────────────┤
│ Nodes       │ Business logic  ← you work here        │
├─────────────┼───────────────────────────────────────┤
│ Runtime     │ Execution, lifecycle, concurrency      │
├─────────────┼───────────────────────────────────────┤
│ Connectors  │ External integration                   │
└─────────────┴───────────────────────────────────────┘
```

Business logic is isolated from infrastructure concerns. Developers work almost entirely at the **Node** layer.

---

## Non-Goals

This project intentionally does **not** try to become:

- a BPM engine or workflow DSL
- a drag-and-drop automation platform
- a replacement for Kubernetes
- a visual programming language

The focus is intentionally narrow: **typed executable architecture for Go services**.

---

## Current Status

- ✅ Runtime engine
- ✅ Visual topology designer
- ✅ Typed operators
- ✅ HTTP / gRPC / Kafka connectors
- ✅ Full OpenTelemetry tracing + Prometheus metrics
- ✅ Live topology visualization
- 🧪 Code generation system (beta)

---

## Documentation

| Document | Description |
|---|---|
| [Architecture Reference](docs/architecture-reference.md) | Runtime internals, execution model, lifecycle |
| [DSL Reference](docs/dsl-reference.md) | Topology YAML format — nodes, edges, connectors, types |
| [Architecture Reference (LLM)](docs/architecture_llm.md) | Condensed reference for AI-assisted development |

---

## License

MIT

---

## Contacts

| | |
|---|---|
| 🌐 Site | [gorundebug.com](https://www.gorundebug.com) |
| ✉️ Email | [serlex777@gmail.com](mailto:serlex777@gmail.com) |
| ✈️ Telegram | [t.me/+31qMliw-DeI3M2M6](https://t.me/+31qMliw-DeI3M2M6) |
