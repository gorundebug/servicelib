# Kafka recovery from external durable state

Implementation in progress. This is not yet verified cross-SDK support.

An endpoint may restore its processing state from a durable checkpoint whose
next offset differs from the consumer group's committed position. Kafka group
positions alone cannot reconstruct that state. Reading the entire retained log
is not a substitute for starting at the saved position, and resetting an expired
position to earliest/latest must not silently skip lost input.

`RestoreCheckpoint` / `restore_checkpoint` / `restoreCheckpoint` is an explicit
endpoint handler method. The generated default returns nil/None/null, retaining
ordinary broker-managed positions. It is not discovered by optional interfaces,
reflection, application property names or a special maker registration.

When enabled, it returns next offsets by partition for the endpoint's one topic.
The application has already restored the state represented by those positions.
The prefix below each position is durable and may be committed before delivery
of new messages. This is checkpoint restoration, **not a read-only seek**. In
particular, Sarama ResetOffset/MarkOffset update the offset manager as well as
its next read position. Ordinary message/result ACK behavior is unchanged.

The method is called before consumer startup to select the mode and again before
each assignment begins delivering records. The initial call may await state
installation by another modeled source, without blocking service startup.
Downloading and reconstructing the snapshot belong to that source, not the Kafka
callback. Reassignment reads the installed state. Recovery mode cannot
change during a service lifecycle. The map must cover every assigned partition;
additional entries for currently unassigned partitions are allowed. The SDK
copies the selected positions and does not retain a mutable application map.

Checkpoint mode uses read_committed consumption and disables automatic offset
reset. Before admission, validate every assigned position against retained log
bounds; next offset equal to log end is valid, negative/special offsets are not.
Missing state, lost retained history, changed mode, failed restoration or a client
reset is an error with no message admission. Normal EOF/high watermark is not a
business completion signal. Rebalance cancellation invalidates in-flight work
under the ordinary endpoint lifecycle; a new assignment obtains the checkpoint
again. Topic/cluster identity, snapshot format and catalog revision checks remain
application responsibilities. Never silently accept a different topic incarnation.

For OpenAdFlow, archive completion uses the selected published DATA offset + 1
and the delivered last record. It does not need an SDK-specific EOF extension.
This fact does not remove source identity, retained-history or semantic replay
checks before archive coverage publication.

Implementation/acceptance matrix (not a completed release):

| SDK | Runtime | Generated handler | Checks |
| --- | --- | --- | --- |
| Go | Implemented | Default implemented | Kafka package tests passed, including startup cancellation and checkpoint validation; real recovery acceptance pending |
| Python | Implemented | Default implemented | 41 checkpoint/startup/scheduling/lifecycle tests passed; real recovery acceptance pending |
| Rust | Implemented | Default implemented | 10 assignment/ACK/lifecycle unit tests passed; real recovery acceptance pending |
| TypeScript | Native consumer implemented | Default implemented | 35 checks passed, including real Kafka restore, two-member rebalance and transaction visibility |
| C++ userver | External checkpoints unsupported; ordinary ConsumerScope retained | Default returns nullopt; explicit checkpoints rejected | 19 selected adapter/lifecycle tests passed; no broker in this check |
| C++ Coro | Independent librdkafka implementation | Default implemented | Real Kafka restore, two-member rebalance and transaction visibility passed (3); 27 ordinary/protocol checks passed separately |


Finish the connected implementations and generated contracts before the combined
checks. Then wire the actual checkpoint in the Python-model-driven application,
remove the old readers, and perform the P02 generation/build/Kafka/S3/PostgreSQL
acceptance. A Go unit test alone cannot close that work.


## Current client integration findings

Rust now assigns the validated offsets through the public `ConsumerContext`
rebalance API. Each assignment owns cancellation and an ACK generation guard.
OffsetOutOfRange/AutoOffsetReset in checkpoint mode stops admission instead of
retrying the same failed recovery indefinitely. Assignment/ACK and owned-startup unit checks pass; real-cluster recovery remains unverified.

Coro uses its own librdkafka assignment callback, not userver code. The callback
validates all assigned positions before native assignment, uses strict reset
policy, and cancels revoked partition work. Retained messages own an ACK gate,
not a raw pointer to the consumer; they cannot commit into a later assignment or
a destroyed transport. Cancellation callbacks run outside the ACK mutex.
The existing statistics callback still receives its native snapshots through
an explicit forwarding method because the consumer now owns the native opaque
pointer. No new application scheduling/queue/recovery registry is introduced.
Prepared broker-protocol tests cover rewind/advance against a previously
committed group, invalid/missing positions, changed mode, two replicas and stale
ACK after transfer. The invalid/missing/mode-change cases pass against the protocol mock. Repeated
membership tests stalled in that mock before a new assignment; both passed
against Apache Kafka 4.1.2. Those two group tests now explicitly require
SA_TEST_KAFKA_BROKERS rather than using the mock. Transaction visibility also
passed on that broker. These results do not prove OpenAdFlow integration.

TypeScript now uses the public native consumer in the same pinned Confluent
package for Kafka inputs, instead of the KafkaJS consumer facade. Admin and sink
implementations are unchanged. This is SDK transport implementation, not a
second application endpoint or maker-selected capability. The native callback
has no implicit assignment fallback: admission remains closed until every
selected offset is validated. Fetch/rebalance errors are surfaced to the
existing endpoint supervisor/OnError path; checkpoint errors stop that supervisor
rather than reconnecting with a fallback position. Ordinary nil mode still uses
broker-managed offsets and earliest reset, with read_committed isolation.

The native driver keeps the facade's 32-record fetch size. Assigned partitions
are paused while their accepted batch is processed, retaining order within a
partition and concurrency between partitions. Handler concurrency remains in the
existing endpoint lifecycle. Revocation cancels the corresponding message contexts
and invalidates retained ACK. Already submitted manual commits settle before a
new assignment is installed. Shutdown keeps polling to dispatch commit callbacks
while accepted work drains. Offsets beyond Number.MAX_SAFE_INTEGER fail explicitly;
this client exposes native offsets as numbers, so rounding would corrupt recovery.
Tests for rewind/advance, missing/invalid/unrepresentable positions, two
consumers and stale ACK passed against Apache Kafka 4.1.2, alongside ordinary
endpoint tests and native transaction visibility (35 checks, no skips). The
native addon was run in the existing Linux toolchain image; the host installation
has no native Kafka addon. The broker and test containers each used one CPU.

Userver uses its unmodified public ConsumerScope. It exposes Seek, but not the
error/reset controls and buffered-batch boundaries required by this strict
recovery contract. External checkpoint restoration is therefore unsupported in
cppUserver. The default nullopt preserves ordinary consumption; an explicit
checkpoint, including an empty map, fails before ConsumerScope.Start. Endpoint
startup reports this failure through the existing onError contract.

Do not patch upstream userver or ship a private fork to provide this feature.
The previously prepared patch, export integration and patched recipe locks have
been removed. Coro is independent and uses librdkafka's own public APIs.

The CatalogBuilder journal input now obtains positions from the installed
snapshot through its narrow maker. Archive/serving integration and the joint
application acceptance remain open; this does not close P02.


## Startup coordination

Initial restoration runs on the endpoint-owned background execution, before
creating/subscribing its Kafka consumer. Endpoint Start must return without
waiting for application restoration; otherwise a sequential service startup
cannot reach another source that supplies that state. This does not introduce
phases in ServiceApp. Restoring offsets is still explicit in the Kafka handler.

The provider receives the endpoint lifetime/cancellation context. It may wait
for shared application state, and must complete on failure or cancellation.
Stop cancels this wait and joins its owned work. Restoration failures go through
the ordinary endpoint error contract with no record admission. Reassignment
still obtains and validates current positions before assignment is opened.
Python and TypeScript allow awaitable checkpoint results; synchronous providers
must return promptly on their event loop. Rust initial synchronous restoration
uses an owned blocking task, joined by its endpoint worker. C++ Coro already launches its producer off the service startup path; the
restoration context also carries endpoint-stop cancellation. Userver uses the
same endpoint background startup, but accepts only the default nullopt mode.

Go/Python startup checks and TypeScript lifecycle checks now pass. Python
checkpoint methods belong to the endpoint consumer, not the datasource; normal
partition revocation drains its lane without recording a false shutdown failure.
Tests observe actual readiness/errors rather than treating Start return as a
Kafka connection. The combined OpenAdFlow acceptance remains pending.
