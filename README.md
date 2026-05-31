# Service Topology Runtime

> **Executable architecture for Go services**
>
> Design your service as a typed topology graph.
> Generate the infrastructure.
> Implement only the business logic.

---

# What Is This?

Service Topology Runtime is an **executable architecture runtime** for Go.

Instead of manually wiring:

* HTTP handlers
* gRPC services
* Kafka consumers/producers
* serialization
* concurrency
* lifecycle management
* retries
* error propagation
* stream routing

you describe the service as a **typed dataflow graph**.

The graph becomes:

* the runtime topology
* the execution model
* the architecture documentation
* the code generation source

The architecture diagram is the running program.

---

# Why?

Modern backend services contain two very different kinds of code:

## 1. Infrastructure Plumbing

The repetitive code:

* routing
* connectors
* transports
* concurrency
* serialization
* orchestration
* lifecycle handling

This code is:

* large
* repetitive
* error-prone
* architecturally inconsistent

## 2. Business Logic

The actual decisions your service makes:

* validation
* transformations
* integrations
* business rules

This is the code that matters.

---

This project eliminates category #1.

You design the topology visually.

The framework generates:

* connectors
* handlers
* lifecycle glue
* runtime integration
* typed stubs

You implement only the nodes.

---

# Core Idea

A service is modeled as a **typed execution graph**.

Example:

```text
KafkaInput
    → Filter
    → KeyBy
    → Join
    → Map
    → HttpSink
```

This is not just a diagram.

It is:

* executable
* typed
* validated
* inspectable
* generatable

Every edge has a type.

Every node has a contract.

Every connector has a lifecycle.

---

# Philosophy

This is:

* NOT low-code
* NOT no-code
* NOT drag-and-drop automation
* NOT a workflow engine
* NOT an enterprise BPM system

Developers still write real Go code.

The difference is:

* architecture becomes declarative
* infrastructure becomes generated
* runtime wiring becomes automatic

The framework disappears.
The architecture remains.

---

# Mental Model

| Layer          | Responsibility                    |
| -------------- | --------------------------------- |
| **Graph**      | Architecture and topology         |
| **Nodes**      | Business logic                    |
| **Runtime**    | Execution, lifecycle, concurrency |
| **Connectors** | External integration              |

Business logic is isolated from infrastructure concerns.

Developers work almost entirely at the **Node** layer.

---

# Development Flow

```text
Visual Designer
        ↓
Topology Definition
        ↓
Code Generator
        ↓
Generated Typed Stubs
        ↓
Your Business Logic
        ↓
Runtime Execution
```

---

# Example

## Visual Topology

```text
HTTP Input
    → ValidateUser
    → EnrichProfile
    → PublishKafkaEvent
    → HTTP Response
```

---

## Generated Stub

```go
type ValidateUser struct{}

func (v *ValidateUser) Process(
    ctx context.Context,
    input CreateUserRequest,
    collect runtime.Collect[ValidatedUser],
) error {

    if input.Email == "" {
        return errors.New("email required")
    }

    collect.Out(ctx, ValidatedUser{
        Email: input.Email,
    })

    return nil
}
```

No framework boilerplate.

No HTTP routing.

No transport handling.

No serialization code.

Only business logic.

---

# Typed Operator Model

The runtime provides a minimal set of strongly-typed operators.

| Operator    | Purpose                                 |
| ----------- | --------------------------------------- |
| `Input`     | Receive data from external systems      |
| `Sink`      | Send data to external systems           |
| `Map`       | Transform one value into another        |
| `Filter`    | Conditionally pass values               |
| `FlatMap`   | Expand one value into many              |
| `Process`   | Transform with side-output error stream |
| `KeyBy`     | Partition stream by key                 |
| `Join`      | Combine two keyed streams               |
| `MultiJoin` | Combine multiple keyed streams          |
| `Merge`     | Merge streams                           |
| `Split`     | Duplicate streams                       |
| `Case`      | Branch by condition                     |
| `Delay`     | Deferred delivery                       |
| `CycleLink` | Feedback loops                          |

This operator algebra is sufficient to model:

* CRUD services
* event-driven systems
* stream processing
* orchestration flows
* distributed service interactions

using a single consistent abstraction.

---

# Connectors

## Supported Data Sources

* HTTP
* gRPC
* Kafka
* Local in-process sources

## Supported Data Sinks

* HTTP
* gRPC
* Kafka
* Local in-process sinks

Every connector follows the same lifecycle model:

```text
BeginRequest
    ↓
ConsumeMessage
    ↓
HandleResponse
    ↓
EndRequest
```

---

# Runtime Features

* Typed execution graph
* Automatic connector wiring
* Stream routing
* Async result correlation
* Error propagation
* Service lifecycle management
* Structured concurrency
* Metrics hooks
* Logging hooks
* Real-time topology visualization

---

# Live Runtime Topology

The running service can expose its own topology.

The runtime visualization shows:

* nodes
* edges
* stream types
* service boundaries
* live call counts

The runtime graph is the same graph used for generation and execution.

Architecture diagrams cannot drift from reality.

---

# AI-Agent Friendly by Design

Generated code is intentionally designed for AI-assisted implementation.

Every generated stub includes:

* explicit named types
* clear lifecycle documentation
* compile-time interface validation
* TODO implementation points

An AI coding agent can implement a node with minimal context because:

* the architecture is explicit
* the contracts are typed
* responsibilities are isolated

The topology constrains the problem space.

---

# Why This Is Different

| System                   | Primary Goal                    |
| ------------------------ | ------------------------------- |
| Kafka / Flink            | Stream processing               |
| Temporal                 | Durable workflows               |
| Node-RED                 | Visual automation               |
| Traditional frameworks   | Request handling                |
| Service Topology Runtime | Executable service architecture |

Streaming is an implementation detail.

The primary abstraction is:

> **service topology as executable architecture**

---

# Architectural Properties

## Architecture Cannot Drift

The topology definition drives:

* generation
* runtime behavior
* visualization

The graph is the source of truth.

---

## Onboarding Is Visual

New engineers can inspect the topology and immediately understand:

* data flow
* integrations
* branching
* dependencies
* error routing

without tracing call chains through the codebase.

---

## Changes Are Localized

Adding functionality means:

1. Add a node
2. Regenerate stubs
3. Implement business logic

The impact boundary is explicit.

---

## Consistency Is Structural

All generated integrations follow the same patterns.

Consistency is enforced by the topology and runtime — not by team discipline.

---

# Service Boundary Awareness

A topology may span multiple services.

Cross-service interactions become explicit edges in the graph.

This makes distributed coupling:

* visible
* reviewable
* analyzable

before deployment.

---

# Project Structure

```text
Designer
    → topology definition

Generator
    → typed Go stubs

Runtime
    → execution engine

Business Logic
    → your implementation
```

---

# Current Status

The project currently includes:

* runtime engine
* visual topology designer
* typed operators
* HTTP/gRPC/Kafka connectors
* live topology visualization
* code generation system (in progress)

---

# Goals

* Make backend architecture explicit
* Eliminate infrastructure boilerplate
* Keep business logic isolated
* Enable topology-driven development
* Make distributed systems understandable
* Improve AI-assisted implementation workflows

---

# Non-Goals

This project intentionally does NOT try to become:

* a BPM engine
* a drag-and-drop automation platform
* a general workflow DSL
* a replacement for Kubernetes
* a visual programming language

The focus is intentionally narrow:

> typed executable architecture for Go services

---

# License

MIT

# Contacts
- Email: serlex777@gmail.com
- Telegram: https://t.me/+31qMliw-DeI3M2M6
- Site:  https://www.gorundebug.com