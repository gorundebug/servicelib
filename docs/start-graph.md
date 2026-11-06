# Explicit graph-only startup

The ServiceApp lifecycle has two alternative entry points:

| SDK | Graph-only | Full service |
| --- | --- | --- |
| Go | `StartGraph(ctx)` | `Start(ctx)` |
| Python | `await start_graph(ctx)` | `await start(ctx)` |
| Rust | `start_graph(context).await` | `start(context).await` |
| TypeScript | `await startGraph(context)` | `await start(context)` |
| C++ userver | `startGraph(context)` | `start(context)` |
| C++ Coro | `co_await startGraph(context)` | `co_await start(context)` |

Graph-only startup is for explicit operator/substream execution. It builds the runtime graph and starts its
registered storage, executors, timers, components and output connectors in the
ordinary dependency order. Managed connectors acquire outbound resources, but
their input admission remains closed. No ordinary data source or SDK-owned
HTTP/gRPC listener is started. An HTTP server independently owned by a host
remains that host's responsibility; this API cannot stop an external listener.

Full startup uses the same preparation, then opens managed input admission,
data sources and SDK-owned listeners. A source may produce immediately at
startup, so this order must not be reversed.

`Stop` / `stop` is the cleanup operation for either mode, including partial
startup failure. Existing cancellation, rollback, graph drain and dependency
ownership rules still apply. Use the received owner context. This is not a new
background root, a replacement for modeled sources, or an implicit maker hook.

StartGraph is an alternative mode for explicit CLI/operator graphs, not a
preparation phase before Start on the same instance. Choose either entry point
once per lifecycle; stop releases resources in either mode. Do not register
new components after startup. CatalogBuilder uses ordinary full Start and source
coordination through narrow makers, not this operator-only mode.

Explicit graph calls still use the usual result/error/cancellation contracts;
graph-only mode does not fabricate success or bypass downstream sinks.

The same semantics apply to each SDK implementation. Runtime-specific mechanics
remain independent: userver and Coro do not share their lifecycle implementation.

Custom Source now supports an ordinary `enabled` setting (see
[custom-source-enabled.md](custom-source-enabled.md)). The remaining bootstrap
constraint is the shared Kafka endpoint: disabling it disables both input and
output, whereas the operator needs output without admitting recovery input.
This is separate from S3/Kafka readiness coordination during full service startup.
