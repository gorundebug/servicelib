# Kafka input isolation

Ordinary ServiceLib Kafka inputs use `read_committed`: aborted transactional
records are not business messages, and an open transaction holds later records
in that partition until its outcome is known. Ordinary nontransactional records
remain supported. This is the native input transport contract, independent of
application batching, SQL storage, handlers, makers and error/result routing.
It does not promise exactly-once application effects or automatic acknowledgement.

Go uses Sarama `Consumer.IsolationLevel = ReadCommitted`. The SDK's native Kafka
baseline is 2.6; explicitly configured brokers older than 0.11 cannot provide
this isolation and fail native configuration validation rather than downgrade.
Python selects aiokafka `isolation_level='read_committed'`. Rust and C++ Coro
select librdkafka `isolation.level=read_committed`. TypeScript's native factory
selects `readUncommitted: false`. Generated C++ userver static configuration
sets `rd_kafka_custom_options.isolation.level: read_committed`; applications
providing their own external ConsumerScope/configuration must preserve this
contract. Existing userver deployments must regenerate/update their static config.

This removes reliance on differing native defaults (Sarama and aiokafka default
to uncommitted reads; librdkafka defaults to committed reads). No new API/DSL
field, financial option, optional handler capability, consumer loop or implicit
SQL behaviour is introduced. Producers and their transaction policy are unchanged.

Offsets are broker positions, not counts of delivered business records. A
consumer may skip control markers and aborted records. Consumers must not infer
that an offset gap is a lost business event, or that the last delivered message
plus one necessarily equals the broker's high watermark. SQL-backed progress and
recovery checks remain application responsibilities.

Userver's native configuration surface is documented at
https://userver.tech/db/ddc/classkafka_1_1ConsumerComponent.html .
