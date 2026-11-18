# Native NATS sink contract

The two constructors preserve explicit runtime routing: ordinary Sink routes
handler-produced R through its declared legacy result/error stream;
SinkWithResult routes R through ConsumeResult. No runtime handler probing or
invented conversion from transport errors to E is used.

SinkMessage.Data and Header become a native message on the configured subject.
Send returns after bounded admission and native publication. JetStream uses the
SDK's PublishMsgAsync future; a completion callback converts the real PubAck or
error into R. Core Send reports native client acceptance, including acceptance
into the bounded reconnect buffer: Delivery.Accepted is true, Flushed is false,
and Ack is nil. It does not report server acceptance or subscriber delivery.
Admission errors can invoke the callback inline. SendSync explicitly waits for
Core protocol flush (Flushed=true) or the JetStream PubAck. Out and Skip emit R
without publishing.

Async capacity is 256 publications and 8 MiB of retained message data/headers per
endpoint. Admission fails immediately with ErrBackpressure when either limit is
reached. One completion goroutine per admitted publication is bounded by that
capacity; there is no additional adapter queue. The native async publisher also
has a 256-publication limit and OperationTimeout. Cancellation reports an error
but retains the adapter's byte/slot reservation until the native future settles,
so repeated cancellation cannot bypass the retained-memory bound.

EndRequest follows ConsumeMessage return, exactly as in Kafka. It is independent
of delivery and asynchronous callbacks can outlive it. Handlers must not release
state that pending callbacks capture. The adapter keeps callback contexts alive
until completion without detaching them from caller/owner cancellation. Handler
ConsumeMessage errors reach EndRequest and the runtime SinkCallback. Transport
errors which cannot be assigned to a Core message (for example asynchronous
permission rejection) reach the mandatory OnError method. OnError runs on a
separate bounded error dispatcher and may overlap request methods; repeated
transport errors coalesce while it is busy. Handlers construct their own typed
error/result values explicitly.

JetStream publication asserts the configured stream name. Timeouts can leave
publication outcome uncertain; no adapter publication retry or exactly-once
promise is made. Native reconnect may resume buffered writes. Deduplication
headers, when wanted, must be supplied explicitly by the handler.

Start verifies connections and opt-in resources before admitting messages. Stop
closes admission, lets admitted handlers and asynchronous delivery/result callbacks
drain until the stop context expires, then cancels operations and closes native
connections. Application callbacks that ignore cancellation can outlive Stop.
Caller contexts and the Start owner context propagate into publication; each
publication is additionally bounded by OperationTimeout (default 5 seconds).
SinkMessage must not escape ConsumeMessage or be used after it returns.

Connections permit 60 reconnect attempts per disconnect, spaced by the native
250 ms reconnect wait (plus SDK jitter), with an 8 MiB native reconnect buffer.
Each dial retains the lifecycle context and its connection timeout; it never
reuses an expired startup context. Core subscriptions,
queue groups, durable names and CreateConsumer are rejected on sinks. Existing
JetStream streams are never mutated or deleted; CreateStream alone opts in to
file-storage, one-replica creation. Subjects on sinks cannot contain wildcards.

Correlation and W3C trace headers are copied from the request context without
mutating caller Header: x-stream-id, x-trace, traceparent, tracestate and baggage.
Propagation keys are matched case-insensitively; unrelated caller headers remain.
