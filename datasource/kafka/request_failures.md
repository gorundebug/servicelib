# Kafka request failure and committed offsets

The ordinary Go EndpointHandler now propagates request failure through the
internal EndpointRequest call to ConsumeClaim. BeginRequest/ConsumeMessage errors,
pending registration failures and incomplete cancelled requests end the current
claim before its next message. Sarama ends the assignment and uses its ordinary
group loop to resume at committed offsets. This does not change Consumer.Consume
or add an optional handler hook, queue, custom assignor or transaction coordinator.

For asynchronous processing, the handler can derive a cancellable context in
BeginRequest and cancel it with a cause after a failed result. Do not mark the
message or call Done on that failure. Done means successful completion of the
request lifecycle and wins a simultaneous cancellation, as before. Explicit
MarkMessage/Commit remains the application's responsibility. EndRequest must
release its request context/deadline resources.

Errors reported only through an error collector do not implicitly change offset
handling. A deliberate skip must be represented as completed handling, with the
chosen explicit acknowledgement. There is no automatic dead-letter or poison
message skip. Persistent failure can keep retrying the failed record; operational
recovery must not simply commit a later offset to make the error disappear.

## Evidence, 2026-10-28

Before the fix, TestOrdinaryConsumerClaimStopsOnRequestFailure dispatched both
offset 7 (failed) and offset 9 and returned nil. Afterwards it stops at 7. Real
typed endpoint tests also verify error propagation, cancellation cause, pending
cleanup, completed-result precedence and fresh-assignment replay. Full Go Kafka
suite passed with race; see OpenAdFlow's financial-kafka-2026-10-28 measurement.

OpenAdFlow live acceptance used ordinary generated handlers, actual Kafka,
Registry and PostgreSQL. SQL failure on the first of two consecutive admissions
did not advance the group offset to the second. Removing the failure allowed
automatic replay; a fresh service instance did not duplicate financial effects.

## Cross-runtime audit: still open

The same invariant was inspected in the other implementations, not assumed from
Go tests. Source inspection found swallowed processing failures that need native
reproduction before a cross-runtime fix can be accepted:

- Python `datasource/kafka/aiokafkads.py`: `_endpoint_request_inner` catches the
  handler exception and returns after end_request; `_run_partition` resumes its
  lane. Simply rethrowing is insufficient because its finally resumes the
  partition; recovery/seek and cancellation ownership need a native test.
- Rust `datasource/kafka/rdkafka.rs`: `endpoint_request` returns unit after
  begin/processing failure; the per-partition lane then receives another record.
- TypeScript `datasource/kafka/confluent.ts`: `handleTraced` records the failure
  and calls endRequest but does not reject the handle promise; the consumer's
  reconnect path only receives rejected operations.
- C++ userver and Coro, separately: their Kafka `detail/endpoint.hpp` processing
  catches failures, completes endRequest/metrics and returns normally to submit.
  Transport exception behaviour must be checked in each backend independently.

These are code-audit findings, not completed live tests or cross-language fixes.
Do not claim cross-language delivery parity until each ordinary path has native
failure/replay tests. The Kafka handler exposes only its documented request,
result and error contract. Optional consumer-control interfaces and automatic
interface assertions have been removed; application methods do not silently
configure the consumer or alter assignment, startup or shutdown behavior.
