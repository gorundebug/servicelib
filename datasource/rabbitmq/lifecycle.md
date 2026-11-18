# RabbitMQ AMQP 1.0 source

Uses the official `rabbitmq-amqp-go-client` v1.4.0 and its native AMQP message and delivery context. RabbitMQ 4.x with AMQP 1.0 management is required. No AMQP 0.9.1 transport, CustomSource delegation, or changes to the shared runtime interfaces are involved.

`MakeRabbitMQEndpointConsumer[H,T,R,E]` accepts the existing typed input stream and `EndpointHandler[H,T,R,E]`. One handler request runs at a time per endpoint. Native receiver credits default to 256 (configurable 1..4096); there is no adapter message queue. Credit limits message count, not total bytes. Broker message-size limits remain relevant to receiver memory.

`ConsumerMessage.Message` retains native body sections, properties, annotations, and application properties. `Accept`, `Release`, and `Reject` settle the individual delivery using its original receiver generation. `Done` only completes the request's result wait. It never acknowledges. Register result callbacks before emitting: results can arrive synchronously. BeginRequest failure skips ConsumeMessage and EndRequest. ConsumeMessage errors reach EndRequest. EndRequest waits for admitted result callbacks; no E value is fabricated from a transport error.

Stop disables admission and cancels Receive, then lets admitted handlers/results settle while the connection remains available. The SDK does not expose receiver pause; bounded native credit-prefetch may continue during drain. After drain or the Stop deadline, request and connection contexts are cancelled. Application code that ignores cancellation cannot be forcibly terminated; Stop still returns by its deadline. OnError runs on a bounded, coalescing transport-error dispatcher and must respect context. Old delivery handles are rejected after connection replacement or shutdown.

Each endpoint owns a native connection. Reconnect rotates configured servers, with at most 60 attempts separated by 250ms and a configured timeout per dial. Topology and links are rebuilt under the owning lifecycle context. The SDK's detached automatic recovery is disabled to prevent resurrection after shutdown. Reconnection never replays publications. Queue redelivery remains broker-controlled.

Queue lookup validates an existing classic/quorum queue and any explicitly supplied QueueType. CreateQueue permits creation only if missing, defaulting to a durable quorum queue. CreateExchange permits native declaration (default direct); incompatible declarations fail. ExchangeType is creation-only because the native management API has no read-only exchange-type inspection. Existing exchanges are checked by native sender attach. CreateBinding independently opts into the named exchange-to-queue binding. No flags default to true. No resources are deleted on Stop.

Application properties carry case-insensitive `x-stream-id`, `x-trace`, `traceparent`, `tracestate`, and `baggage`. Extraction precedes BeginRequest. Unrelated caller properties and native correlation/message IDs are retained.

Acceptance uses official RabbitMQ 4.3.0, Go race detection, and the real SDK: accepted/released/rejected delivery, Done redelivery, synchronous result registration, metadata, explicit topology, missing queues, reconnect/generation fencing, handler failures, settlement during drain, and deadline cancellation.

Run from servicelib with a dedicated broker (the controlled tests pause/restart it):

```sh
RABBITMQ_TEST_URL=amqp://127.0.0.1:15672 \
RABBITMQ_TEST_CONTAINER=servicelib-rabbitmq-amqp-acceptance \
go test -race ./datasource/rabbitmq ./datasink/rabbitmq ./internal/rabbitmqclient
```

The test broker uses username `adapter` and password `adapter-test`. Without a URL, native tests skip. Tests requiring pause/restart additionally require the dedicated container name.
