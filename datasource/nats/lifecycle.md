# Native NATS source contract

`MakeNATSEndpointConsumer` registers a public runtime DataSource, selected as Core
or JetStream by `NATSDataConnectorConfig.Type`. Start performs all connection,
subscription and resource checks before returning. Disabled endpoints do not
connect. A failed startup stops previously started endpoints. A lifecycle is
single use; calling Start again fails, while Stop is safe before Start and can
be repeated.

Each endpoint owns one native connection and processes requests sequentially.
There is at most one pending pipeline request per endpoint. Core subscriptions
have a 256-message, 8 MiB pending limit. A slow consumer is reported through
OnError and the endpoint stops; Core messages can be lost. JetStream uses the
official pull iterator with one-message prefetch and an 8 MiB pull byte budget.
Oversized messages cannot be consumed under this fixed budget. Horizontal
parallelism uses Core queue groups or multiple instances of a JetStream durable.
This conservative profile prioritizes bounded memory over per-endpoint throughput.

BeginRequest failure skips both ConsumeMessage and EndRequest. Otherwise,
ConsumeMessage runs and EndRequest receives its error, or context cancellation
while awaiting a result. Without a result stream there is no Done wait. With a
result stream, callback correlation state is present before ConsumeMessage.
Handlers must register each callback **before** emitting a value, because a
pipeline can return a result synchronously. Callback return true deregisters
that callback; Done completes only the response lifecycle.

Core `ConsumerMessage.JetStream` is nil. It has no ACK operation. For JetStream,
the native `jetstream.Msg` exposes Ack, DoubleAck(ctx), Nak, NakWithDelay,
InProgress and Term. Settlement is entirely explicit and independent of Done,
callback return values, handler success, handler failure and Stop. Unsettled
messages can be redelivered. DoubleAck is the context-aware confirmed ACK path.

Existing streams and durable consumers are required by default. CreateStream
and CreateConsumer separately opt in to creation. Creation uses file storage,
one replica, all-available delivery, instant replay and explicit per-message
ACK. Existing resources are never updated or deleted. The existing consumer
must be durable pull, use explicit ACK, and match the endpoint subject filter.

The initial caller context bounds startup and owns the running endpoint.
Stop stops input admission and the native subscription/pull iterator, then
drains admitted requests with their context and ACK connection still usable.
Only after drain or the stop deadline does it cancel request contexts and close
transport. No new result callbacks are admitted after request retirement.
EndRequest waits for already admitted callbacks to finish. Application code that
ignores cancellation can outlive Stop; it cannot be forcibly killed by Go. No
additional per-request or drain-waiter goroutine is created to hide that fact.

Native reconnection permits 60 attempts per disconnect, using a 250 ms reconnect
wait plus SDK jitter, and retains the running lifecycle context. Core native
subscriptions and the JetStream iterator resume after reconnect. Disconnect and
async transport errors reach OnError through a one-element error channel; repeated
errors coalesce while application error handling is busy. Exhausted reconnection
or a terminal iterator error ends reception. Core messages during disconnection
can be lost; JetStream redelivery remains governed by explicit settlement.

Before BeginRequest, native headers restore x-stream-id, x-trace, traceparent,
tracestate and baggage through case-insensitive lookup and the configured tracing
engine. An inherited stream ID is retained for pipeline callback correlation.

Focused real-server tests use `NATS_SERVER_BINARY=/path/to/nats-server go test
-race ./datasource/nats ./datasink/nats ./internal/natsclient`. Without the binary
(or nats-server on PATH), integration cases skip explicitly.
