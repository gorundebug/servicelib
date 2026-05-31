# servicelib — Architecture Reference
Module: `github.com/gorundebug/servicelib`

## TL;DR (for LLM)

- Import only: transformation
- Never use operators or runtime directly
- Pipelines are built via transformation.* functions
- All connectors follow: DataConnector → Endpoint → Consumer
- Streams propagate via context (streamID)
- Prefer minimal local context over full project scan

Common tasks:
- Add transformation → use transformation.Map / Filter / ...
- Add sink → use datasink facade
- Handle async result → use ResultContext

Rules:
- Do not break stream type hierarchy
- Do not bypass Collect[T]


## Execution Mode (STRICT)

- Follow TL;DR first, ignore Details unless explicitly needed
- Prefer minimal solution over complete exploration
- Do not scan unrelated packages
- If task scope is local — use only provided context
- Ask for missing info instead of guessing


## Mental Model

Pipeline:
Source → Transformations → Sink

Execution:
Data flows via Collect[T]
Results flow back via ResultContext

Async correlation:
streamID in context → RotatingMap → callback

## Common Tasks

### Add new transformation
1. Define function interface in operators
2. Add stream builder (MakeXStream)
3. Re-export in transformation/
4. Add config in stream_types.go

### Add new datasource
1. Implement EndpointHandler
2. Wire into DataSourceEndpoint
3. Add config struct
4. Register in RuntimeConfig

### Add new datasink
1. Implement EndpointHandler
2. Use DataSinkEndpointConsumer OR custom (if result)
3. Add facade function in datasink/datasink.go


## Do / Don't

Do:
- use transformation facade
- follow connector 3-layer pattern
- propagate context

Don't:
- import operators directly
- bypass runtime abstractions
- mix source/sink responsibilities

## Entry Points

- Build pipeline → transformation.Input → ... → transformation.Sink
- Add business logic → operators (via transformation facade)
- Integrate external system → datasource/ or datasink/
- Run service → ServiceApp


## Anti-Patterns

- Passing full project context to implement small function
- Mixing datasource and datasink logic
- Writing transformations outside operators
- Directly accessing runtime internals

## Output Rules

- Return only relevant code
- No explanations unless requested
- Do not restate architecture
- Keep output minimal and focused
