# RabbitMQ AMQP 1.0 sink

Uses official `rabbitmq-amqp-go-client` v1.4.0. Constructors preserve existing typed sink interfaces:

- `MakeRabbitMQEndpointConsumer[H,T,R]` uses the existing plain sink continuation contract.
- `MakeRabbitMQEndpointConsumerWithResult[H,T,R,E]` sends R to the ordinary result continuation. E remains application-defined.

Handlers receive `SinkMessage[R]` containing a native `*amqp.Message`. `Send(ctx, func(Delivery,error) R)` invokes the official native PublishAsync. It does not wait for a broker outcome, but native credit acquisition and socket write can block until context/operation timeout. Admission errors may invoke the callback inline. `SendSync(ctx)` explicitly waits for the outcome. `Out` and `Skip` emit an explicit application result without publishing.

EndRequest and the runtime sink completion callback reflect ConsumeMessage completion, not broker delivery. Async callbacks can outlive EndRequest; captured handler resources/state must remain valid and synchronized until callbacks finish. A native message snapshot protects an admitted publication from later caller mutation. Do not mutate the message concurrently with Send itself.

Delivery retains the native PublishResult, including RabbitMQ 4.3 rejection details. Outcomes are:

| Outcome | Meaning |
|---|---|
| not-sent | Adapter validation/admission failed before native send |
| accepted | Broker accepted the publication |
| unroutable | Native publisher received released |
| rejected | Broker explicitly rejected; inspect Native for details |
| modified | Native modified outcome, retained distinctly |
| unknown | Transmission/outcome failed or timed out after native invocation; delivery may have occurred |

Unknown is never automatically replayed. Accepted does not mean downstream consumption. Rejected/unroutable are native outcomes, not fabricated transport errors; the handler maps them to its typed business result/error.

Async admission is fail-fast and bounded by PublisherMaxInFlight (default256, maximum4096) and 8MiB of pending encoded messages. Each message is also capped at 8MiB. No extra publication queue or wrapper goroutine per publication is introduced. Native SDK receipt completion invokes the callback. A slot remains occupied until that callback returns.

Stop closes admission and drains admitted handlers/publications/callbacks to its deadline. Then owner cancellation closes transport and unresolved receipts become unknown. A user callback ignoring cancellation may outlive Stop. The SDK Publisher.Close waits unconditionally for callbacks; the adapter instead closes the owning connection after bounded drain. Native connection or publisher-link failure rebuilds a generation without replaying unknown publications. See the source lifecycle document for topology, reconnect, authentication, and propagation rules.

Real RabbitMQ acceptance covers async completion, explicit sync, accepted/unroutable/rejected outcomes, native rejection details, broker pause with bounded admission and unknown cancellation, connection restart, and Stop deadlines with a blocked callback. Focused tests cover byte limits, snapshot ownership, modified outcomes, and transport-unknown mapping.
