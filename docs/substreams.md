# SubStream (Go)

A SubStream is a callable graph inside one service. Ordinary outgoing links
carry its input. The existing source property binds its result producer, just
like an Input result. There is no separate ResultStream or SubStream error port.

## Calling from business code

The generated service exposes a typed accessor named after the SubStream.
A custom maker can inject the service through a narrow interface declared
alongside the business maker:

```go
type OfferStreams interface {
    SearchOffers() runtime.SubStream[SearchRequest, SearchResult]
}
```

Business code supplies a SubStreamCollector[R] implementation or uses
SubStreamCollectorFunc[R] to adapt a function:

```go
results := make([]SearchResult, 0, 2)
err := streams.SearchOffers().Consume(ctx, request,
    runtime.SubStreamCollectorFunc[SearchResult](func(_ context.Context, result SearchResult) bool {
        results = append(results, result)
        return len(results) == 2
    }))
```

False keeps receiving; true completes the call successfully. The application
chooses the actual completion condition (count, an outcome, or an end marker).
Cancellation or the context deadline ends waiting with ctx.Err(). Configuration
errors, including a nil collector, are reported before starting the graph.
Business failures remain ordinary graph values or explicit graph error paths.

## Execution

- Graph instances are shared; only a small callback/completion state is created
  per call, attached to a derived context. No message ID parameter is required.
- Ordinary operators carry that context unchanged. Business code must preserve
  it when sending values downstream, as with endpoint correlation.
- The result callback receives the original caller context, so nested calls
  restore the enclosing binding when returning values to the outer graph.
- Callbacks within a call are serialized; independent calls can overlap.
- Completion means the caller has enough results, not that all graph work ended.
  Later results are discarded. Running branches are not forcibly cancelled.
- Cancellation waits for an already-entered callback to return; callbacks must
  cooperate with cancellation and must not block indefinitely.
- A fully synchronous chain runs inline before Consume enters its wait, just as
  endpoint admission does. Consume does not create a goroutine or choose a pool.
- Existing Join keys, storage, transport sessions and scheduling do not change.
  Callers must choose appropriate keys if concurrent requests use a shared Join.
- A caller occupying all workers needed by its substream can deadlock. Use
  suitable existing link/pool configuration and a context deadline.
- With no completing result, Consume waits until the context is cancelled.

This implementation is for ordinary Go execution. It does not add a native
blocking wait to Temporal's deterministic workflow scheduler through hidden
transport hooks; invoking it from workflow code requires a separately agreed
cooperative-wait contract.

## Modeling

Use pipeline.substream(name, value_type=...) to declare the input. Build the body
with existing operators, then connect its result producer using result >> entry.
The result type is inferred from source. No function-to-substream dependency
declaration or runtime string registry is required.

Other generation languages are outside this initial Go implementation.
Build, runtime concurrency and Designer integration checks have not yet run.
