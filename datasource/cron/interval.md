# Local interval scheduling

Work in progress: the local Python model, validation and Go/Python/TypeScript/
Rust providers support the contract below. C++ Coro and C++ userver now have
interval adapters with passing native interval/result/restart checks. Userver
recreates its BackgroundTaskStorage after a fully drained stop; timed-out stops
retain the current storage and accepted work until a later stop succeeds.
Do not treat this as a released cross-language
capability.

`IntervalSchedule(milliseconds=100)` uses the existing local Cron connector and
serializes as `schedule: "@every 100ms"`. No new API field, property control or
application timer is required. The wire grammar is exactly `@every Nms`, where
N is a canonical decimal integer from 10 through 86400000. Signs, leading zeroes,
fractional values, alternative units and additional whitespace are invalid.
Timezone is UTC. This extension does not apply to Temporal schedules.

The first logical occurrence is one interval after the process-local scheduler
anchor, truncated to milliseconds. Later occurrences retain that fixed cadence;
they are not measured from completion of the preceding business operation.
Existing overlap and missed-run policies apply. There is no durable replay after
a process restart, and two replicas schedule independently. Domain ownership
must restrict maintenance to the partitions owned by the current replica.

The Go implementation uses gocron's supported Cron calculation interface.
gocron still owns timers, dispatch, overlap and shutdown; the adapter calculates
logical timestamps and retains only the latest due timestamp and a count, not
an expanding list of missed occurrences. Existing five-field calendar schedules
retain their meaning. Request/result handling uses the same scheduled endpoint.

The Python provider uses APScheduler's native `IntervalTrigger`, with the same
millisecond anchor and its existing request/result/owner-context path. Interval
backlog is coalesced by APScheduler; submission metadata applies `Skip` versus
`FireOnce` without relying on its seconds-based misfire grace window. Max-instance
rejections advance the occurrence cursor without inventing a callback or replay.
The interval trigger implements APScheduler's native next-fire-time interface
and jumps directly to the latest due slot during catch-up. APScheduler therefore
enumerates at most two timestamps before coalescing, even after a long process
pause. Coalescing alone is insufficient: its standard interval trigger first
materializes every missed slot. The scheduler and third-party sources are not
patched. Calendar Cron retains its existing trigger, coalescing and grace
configuration.

Rust extends the existing Tokio scheduler loop with an interval occurrence
calculator. Catch-up computes the latest slot and whether multiple occurrences
were missed in constant time and space, then uses existing Skip/FireOnce,
overlap, owner cancellation and task draining. Calendar schedules stay with
Croner.

TypeScript keeps calendar scheduling in Croner and owns the local interval timer
inside the same SDK endpoint. Fixed slots do not drift with business completion.
Catch-up selects at most one occurrence; the existing endpoint dispatch applies
overlap and keeps accepted work owned through shutdown. Owner cancellation
removes the timer, while stopping the endpoint drains its existing active work.
No application-owned polling or dispatch loop is required.

C++ Coro and userver have separate local interval implementations. Both calculate
the latest due slot in constant time, then use the existing endpoint dispatch,
overlap, result and drain paths. Coro adjusts the existing Asio timer to the next
interval deadline (at most the existing 500 ms calendar polling delay). Userver
retains its PeriodicTask, reducing its polling period to half the shortest enabled
interval when necessary. Neither creates a scheduler per endpoint. UTC and the
existing five-field calendar path are retained. Standalone contract checks for
both implementations passed with UBSan; this does not establish successful
compilation or execution of the complete native providers. Subsequent checks
compiled both native Cron sources. Coro passed 16 tests and userver passed 15,
including a real 100 ms endpoint test covering correlated results, overlap
prevention, shutdown timeout/retry, restart, cadence and owner propagation.
The userver test first exposed reuse of a closed task storage; the lifecycle
fix preserves the same test rather than weakening restart assertions. These
native provider tests are not acceptance of OpenAdFlow's generated graph.

The typed example is `sa-python-dsl/examples/local-interval/project.py`. Its
worker uses a modeled TaskPool and returns completion to the scheduled Input.
The `acceptance/run.sh` fixture runner takes a freshly generated maintenance
service directory, a local Go SDK directory and a new output directory. It
copies the generated service, installs user-owned test implementations through
narrow maker dependencies and runs the real graph with the race detector.
The source callback returns while downstream work is held; a second trigger is
not admitted before the graph result arrives. Compilation, result ownership,
fixed cadence and owner propagation passed with GOMAXPROCS=1. No generated wiring
is patched. These fixtures are not a production maintenance implementation or
proof of OpenAdFlow correlation ownership/transaction integration.
