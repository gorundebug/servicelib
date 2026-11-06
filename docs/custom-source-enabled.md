# Custom Source admission

`CustomEndpointConfig.enabled` controls the automatic producer of a Custom
Source. The contract is the same in Go, Python, Rust, TypeScript, C++ userver
and C++ Coro:

- `enabled: true` starts the producer through the existing source lifecycle.
- `enabled: false` (or an omitted runtime field) leaves the producer stopped.
  No producer start/stop callbacks or source-owned background task are created.
- The endpoint and graph remain registered. Disabling the source does not
  disable a Custom Sink using the same endpoint; explicit graph calls remain
  available. This flag is a startup setting, not a live pause/resume protocol.

The Python DSL `custom_connector(...).endpoint(..., enabled=True)` and Designer
create enabled endpoints by default. They serialize the value explicitly.
Templates preserve it in each language's runtime configuration, where values
and environment overrides can disable the source. No new API schema field or
new property-control implementation is needed: `Endpoint.enabled` already exists.

Existing handwritten YAML/runtime constructors without the flag must specify
`enabled: true` for automatic Custom Source startup. Regenerate Python-authored
projects so their generated defaults and YAML include the flag.

Custom Source enablement alone does not replace graph-only CLI startup: Kafka
Input and Sink can share an endpoint and its existing enablement setting. The
normal CatalogBuilder service uses ordinary full startup with shared recovery
coordination through narrow makers. It does not use StartGraph as a phase.
