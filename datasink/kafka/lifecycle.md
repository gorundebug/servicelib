# Kafka producer lifecycle

The Go sink uses the normal handler/result contract; no optional application
lifecycle capability or configuration is needed.

`Stop` closes admission and wakes calls blocked on the Sarama input channel.
An internal enqueue barrier ensures those calls have finished their channel
operations before `AsyncClose` can close the input. The barrier does not wait
for broker acknowledgement or run delivery callbacks under the admission lock.
Concurrent/repeated stops initiate producer close once. Already admitted records
finish through the existing Successes/Errors readers; stop waits for those
readers subject to its context deadline. Deadline expiry remains a stop-timeout,
not a successful delivery or a guarantee that all background transport has ended.

The former delivery-count wait group was never waited on. It has been replaced
by an enqueue barrier: ACK draining is already owned by the result-reader wait
group and Sarama AsyncClose. There is no per-message registry or extra worker.

## Evidence (2026-10-28)

`producer_lifecycle_test.go` reproduces the former blocked-send/close ordering
and repeated-close defects, and checks a 5ms blocked enqueue, concurrent send/stop
and exactly one callback for every rejected message with the race detector.
Red/green logs are under `/private/tmp/openadflow-budget-accounting-generation/`
(`producer-stop-red.log`, `producer-stop-green.log`).

OpenAdFlow's `scripts/check-budget-accounting.sh` exercised the actual producer,
consumer and PostgreSQL. A real broker size rejection generates a failed result;
the same debit can be retried without a second financial effect. A paused broker
prevents ACK and graceful completion; resuming it allows delivery and native stop
completion. The handler used its default 5ms enqueue limit. This is not a proof
of permanent-broker-outage shutdown completion after the caller's deadline.

## Other implementations: audit boundary

These runtimes do not share Sarama's channel-close implementation. Do not copy
Go's synchronization mechanically into them.

- TypeScript `datasink/kafka/confluent.ts`: stop admission to the runtime delivery
  registry, drain, flush, then disconnect. Six existing Kafka sink tests passed,
  including pending delivery during stop; these use the client fixture.
- Python `datasink/kafka/aiokafkads.py`: stopped flag and `AIOKafkaProducer.stop`;
  sends await the producer's future and report exceptions. Four context/result
  task-registry tests passed; they do not prove native backpressure/shutdown.
- Rust `datasink/kafka/rdkafka.rs`: terminal state, task tracker close/wait with
  caller cancellation, then bounded flush iterations. Native concurrent
  send/stop proof is still required; no Rust implementation was changed here.
- C++ userver `datasink/kafka/userver.hpp`: disables admission and waits for
  owned delivery tasks so borrowed producer buffers remain alive. Its stop
  deadline and concurrent admission semantics still need native qualification.
- C++ Coro `datasink/kafka/librdkafka.hpp`: requestStop, request/delivery admission
  tracking, then blocking producer stop/flush. Native failure/deadline checks
  remain separate from this Go acceptance.

Cross-language parity is not complete merely because the Go gate passed.
