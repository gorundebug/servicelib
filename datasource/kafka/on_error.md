# Kafka source transport errors

`OnError` is a required handler method, not an optional capability discovered
through a maker or type assertion. Other runtimes use their native spelling:
`on_error` in Python/Rust and `onError` in C++/TypeScript.

The callback receives the owning context (implicit request context in Python),
the source stream context, and the original transport error. No Kafka message,
handler state, BeginRequest, EndRequest, result correlation or acknowledgement
is fabricated for an error that happened before a record was read.

The callback may overlap message handlers. It must not wait for endpoint stop.
It reports or maps an error to the existing error stream; implementing it does
not select a retry policy, commit an offset, or enable a new consumer mode.

Scaffolds forward native error values to the error stream when its type matches.
For a custom error type the user implements the explicit conversion. Rust's
standard String error stream receives the error text. Raw transport diagnostics
remain the responsibility of the SDK even when no custom mapping is supplied.

Existing user-owned handler implementations need the new method added explicitly.
Regeneration must not overwrite user code merely to insert it.

The canonical examples implement the required method directly in all six
languages. Native transport errors go to the existing error collector (String
text in Rust); no handler state, acknowledgement or delivery result is created.
There is no compatibility adapter or optional-interface discovery. Generator
regressions exercise both native and domain error types in every language;
domain errors require explicit user mapping instead of a cast of a native error.

## Rollout checkpoint

- Go: required contract, generator and OpenAdFlow implementations added; SDK and
  affected domain race tests plus AdServer/Bidder/CatalogBuilder build passed in
  `/private/tmp/openadflow-kafka-explicit-onerror.1vQYCO`.
- Python: required protocol, scaffold, consumer-loop and periodic commit errors.
- TypeScript: required interface, scaffold and awaited reconnect error reporting.
- Rust: required trait, scaffold and receive-error reporting; `cargo check --tests`
  passed offline with one CPU (`/private/tmp/sa-onerror-rust-cached.log`).
  The public datasource transport-error/recovery test also passed (4.32 seconds,
  one CPU, no skips; `/private/tmp/sa-onerror-rust-runtime-fixed.log`). A librdkafka
  protocol mock starts unavailable: the handler receives the owner context with
  no message lifecycle calls. After recovery, one message is consumed and
  committed, and stopping the datasource preserves the parent context. This
  covers that error path, not every driver error or a live Kafka-cluster outage.
  Rust scaffolds select String conversion from the explicit error type in the
  template view, never by searching the rendered method signature. Regression
  cases cover nested String in input, result and error types; these and canonical
  Rust generation passed (`/private/tmp/sa-onerror-rust-template-fixed.log`).
- C++ userver and Coro: separate scaffolds and producer-error entry points added;
  generation passed for native and custom error types in both runtimes
  (`/private/tmp/sa-onerror-cpp-matrix.log`).
- C++ userver native Kafka/custom endpoint target compiled and passed all 21
  existing tests with one CPU (`/private/tmp/sa-onerror-userver-native-online.log`).
  This proves compilation and existing lifecycle regression coverage, not delivery
  of broker errors hidden inside ConsumerScope.
- C++ Coro: the native Kafka target compiled and passed all 26 tests, no skips,
  in a one-CPU container (10.712 seconds of test execution). Evidence:
  `/private/tmp/sa-onerror-coro-fetch.YJw8oH/resume.log`.
  The new `ReportsTransportErrorWithoutCreatingMessageState` test injects both
  a recoverable callback error and a terminal consumer exception. It checks the
  owner context, one notification, no message lifecycle calls, and continued
  execution after the recoverable callback. Existing librdkafka mock-broker
  tests also passed. This is not proof that every internal driver error reaches
  the callback, nor a live Kafka-cluster outage acceptance test.
- Remaining acceptance: generated custom-error types, compilation and transport
  tests for each runtime; C++ driver-level errors that are handled internally
  must be audited separately. Producer failure tests alone do not prove that path.
- OpenAdFlow retention-before-claim passed for both registrations and receipts
  after the explicit callback: no input commits or output writes, terminal state,
  race, real Kafka and Schema Registry, one CPU per container, no skipped cases
  (`/private/tmp/openadflow-onerror-retention.BuyQ8Q/live.log`).

Userver limitation: the installed public `kafka::ConsumerScope` exposes message
and rebalance callbacks, not broker-error callbacks. It handles fatal broker
errors by recreating the consumer internally. Catching `Start` failures does not
cover these hidden driver events. Do not claim full broker-error parity until
this path has an explicit supported notification mechanism; do not approximate
it by polling metrics or treating ordinary rebalances as transport failures.
