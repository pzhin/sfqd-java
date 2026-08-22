# SFQ(D) for Java

`sfqd-core` is a generic, thread-safe scheduler for weighted, cost-aware
work. It decides which jobs may start; it does not execute jobs and does not
own a thread pool, connection pool, or other resource pool.

Use it when several tenants, queues, users, or workloads share a bounded
number of execution slots and should receive service in proportion to their
weights.

Canonical repository: <https://github.com/pzhin/sfqd-java>

## Status

The implementation, tests, JavaDoc, concurrency harness, and benchmark
harness are complete. The source branch uses the pre-release coordinates
`io.github.pzhin:sfqd-core:0.1.0-SNAPSHOT` and Java package
`io.github.pzhin.sfqd`. No artifact has been published to a public Maven
repository yet.

The benchmark harness is an executable measurement protocol, not a benchmark
result. This repository intentionally contains no raw machine-specific runs,
so production throughput, latency, allocation rate, retained heap, and a
practical maximum batch size have not been established.

The project is licensed under the
[Apache License, Version 2.0](LICENSE).

## Requirements

- JDK 25
- the checked-in Maven Wrapper

Build and verify the library:

```shell
./mvnw --batch-mode --no-transfer-progress clean verify
```

Install the snapshot into your local Maven repository:

```shell
./mvnw --batch-mode --no-transfer-progress install
```

Then add it to a local consumer:

```xml
<dependency>
  <groupId>io.github.pzhin</groupId>
  <artifactId>sfqd-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The runtime JAR has no third-party dependencies. Test and verification
dependencies do not leak into the published artifact.

## Mental model

The scheduler manages:

- a **flow** for each tenant or independent source of work;
- a fixed positive **weight** for each registered flow;
- queued jobs with a positive estimated **cost**;
- at most `D` dispatched but not yet completed jobs.

Higher weight means a larger long-run share when flows compete. Higher cost
means more charged work. Costs and weights are application units: bytes,
records, predicted CPU time, or another stable estimate. They are not measured
by the library.

For each admitted job the scheduler assigns exact rational tags:

```text
start  = max(virtual time, previous finish tag of the flow)
finish = start + cost / weight
```

Jobs are selected by increasing start tag. An admission sequence provides a
deterministic FIFO tie-break. No floating-point arithmetic is used.

## Minimal example

```java
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SfqdScheduler;

var scheduler = new SfqdScheduler<String, String, Runnable>(
        new SchedulerConfig(4, 1_000, 100_000));

var result = scheduler.registerFlow("tenant-a", 2);
if (!(result instanceof RegisterFlowResult.Registered registered)) {
    throw new IllegalStateException("registration rejected: " + result);
}

var admitted = scheduler.enqueue(
        registered.flowHandle(),
        "job-42",
        () -> System.out.println("job-42"),
        5);
if (!(admitted instanceof EnqueueResult.Accepted)) {
    throw new IllegalStateException("enqueue rejected: " + admitted);
}

for (Dispatch<String, String, Runnable> dispatch
        : scheduler.dispatchUpTo(4)) {
    try {
        dispatch.payload().run();
    } finally {
        if (scheduler.complete(dispatch.jobHandle())
                != CompletionResult.COMPLETED) {
            throw new IllegalStateException("completion rejected");
        }
    }
}
```

`dispatchUpTo(k)` does not reserve future permits. Every returned job is
already dispatched and must eventually be completed, even when submission to
your executor fails.

For a real executor or connection pool, use a pump that couples external
capacity to dispatch and runs after every accepted enqueue, every completion,
and every external capacity signal. See the
[production executor pump example](docs/EXECUTOR_INTEGRATION.md).

## Public operations

### Register a flow

`registerFlow(flowId, weight)` creates an opaque flow handle. A flow identifier
cannot be registered twice at the same time, and the configured flow limit is
enforced. A closed identifier may later be registered again, producing a new
handle.

### Enqueue a job

`enqueue(flowHandle, jobId, payload, cost)` admits work to a registered flow.
The job identifier must be unique among currently live jobs. Rejected
admissions are atomic no-ops: they do not consume a sequence number or mutate
tags, queues, counters, or indexes.

### Admission policy boundary

SFQ(D) fairness starts after successful admission: the scheduler distributes
issue slots among jobs that `enqueue` has already accepted. `maxFlows` and
`maxLiveJobs` are global safety bounds, not a fair admission policy.

The core does not provide per-flow queue limits, per-flow live-cost limits,
capacity reserved for other flows, or fair selection among admission attempts.
One flow can therefore fill the entire `maxLiveJobs` allowance. Until capacity
is released, another flow's `enqueue` call returns `LIVE_LIMIT` before SFQ(D)
dispatch fairness can apply.

Applications that need admission isolation should enforce it before calling
the scheduler. For example, a PostgreSQL integration can keep per-class limits
in its application layer:

```text
application admission policy
  global/per-class queue and cost limits, optional reserves
                         |
                         v
                    SFQ(D) enqueue
                         |
                         v
              fair dispatch of accepted jobs
```

### Dispatch jobs

`dispatchUpTo(k)` returns an immutable batch of up to `k` jobs, bounded
by configured issue depth `D`, available issue slots, and queue size. The name
describes an irreversible state transition, not a capacity notification. A
non-empty batch is one atomic scheduling decision.

### Complete or cancel

`complete(jobHandle)` releases a slot held by a dispatched job. It does not
automatically dispatch a replacement; call `dispatchUpTo` again when
capacity is available.

`cancel(jobHandle)` succeeds only while the job is still queued. Once a job is
dispatched, it must be completed.

#### Cancellation accounting

The only supported policy is
`CancellationAccounting.CHARGE_RESERVED_COST`. Cancelling a queued job removes
it from the queue and live-job indexes and releases its payload, but it does
not roll back the job's reserved virtual cost:

- the flow's `lastFinish` tag is not reduced;
- tags already assigned to later jobs of the flow are not recomputed;
- the charge disappears only when the scheduler becomes globally idle and
  ends the current busy period.

Consequently, completed-work fairness guarantees do not apply to any trace
containing cancellation. Treat frequent deadline or timeout cancellations as
a release blocker for an integration unless the resulting virtual charge and
dispatch delay are acceptable for that workload.

For example, consider two equal-weight flows in a new busy period. Keeping a
job from B live prevents an idle reset:

```text
A: enqueue cost=1_000_000
B: enqueue cost=1
A: cancel the cost=1_000_000 job
A: enqueue cost=1
B: after each dispatch, enqueue another cost=1 job
```

A's new job has start tag `1_000_000`. B's jobs have start tags `0` through
`999_999`, so one million B jobs can be dispatched first even though A's
cancelled job received no service. If cancellation instead removes the last
live job globally, the immediate idle reset clears this charge.

### Close a flow

`closeFlow(flowHandle)` succeeds for an inactive flow once its finish tag is
no greater than current virtual time. An unused flow can therefore be closed
immediately, and an old tenant can be deregistered during continuous traffic
as soon as its fairness debt has been repaid. If `lastFinish > V`, the method
returns `FAIRNESS_DEBT_ACTIVE`; retry after scheduling progress. This condition
also makes close followed by registration with a different weight safe: the
old and new identities would both assign the next job start tag `V`.

### Inspect state

`snapshot()` returns constant-size counts and cumulative counters. It does not
expose internal queues or mutable scheduler state.

`snapshot(flowHandle)` returns an `Optional<FlowSnapshot>` for the exact active
registration. It reports current queued/running counts and exact cumulative
accepted, dispatched, and cancelled cost units. Current queued cost is also
available as `acceptedCost - dispatchedCost - cancelledCost` through
`queuedCost()`. Cost totals use `BigInteger`, so valid `long` costs do not
overflow observability counters.

The per-flow snapshot is captured atomically with lifecycle transitions. A
foreign, stale, or closed handle returns an empty optional. The library does
not own a clock, retain enqueue timestamps, expose internal virtual tags, or
invoke metrics callbacks; applications can keep timestamps in payloads or an
external observer when wall-clock queue age is needed.

## Thread safety

All public operations are linearizable and may be called concurrently. The
implementation serializes state transitions internally; callers do not need
an external lock.

Important race outcomes:

- cancel before dispatch removes the job; dispatch before cancel makes cancel
  return `TOO_LATE_ALREADY_DISPATCHED` or `NOT_LIVE` after completion;
- concurrent dispatch calls cannot duplicate a job or an issue slot;
- at most one cancel or completion for a handle succeeds;
- concurrent enqueues of the same live job identifier admit at most one job;
- a completion that wins before the next capacity call makes its slot
  available to that call.

Handles are scheduler-specific capabilities. A foreign, stale, completed, or
cancelled job handle is reported as `NOT_LIVE` where applicable. Flow and job
identifier objects must keep stable, deterministic, non-throwing `equals` and
`hashCode` behavior while retained by the scheduler.

## Configuration

`SchedulerConfig(issueDepth, maxFlows, maxLiveJobs)` selects
`CHARGE_RESERVED_COST` and enforces:

- `issueDepth`: `1..1_000_000`;
- `maxFlows`: `1..Integer.MAX_VALUE`;
- `maxLiveJobs`: `issueDepth..Integer.MAX_VALUE`.

The four-argument form makes the policy explicit:

```java
new SchedulerConfig(
        issueDepth,
        maxFlows,
        maxLiveJobs,
        CancellationAccounting.CHARGE_RESERVED_COST);
```

No free-cancellation accounting policy is currently implemented.

Weights and costs are positive `long` values. Choose `D` as the number of jobs
your execution layer can have issued but not completed. For `N` identical
non-preemptive resources, the usual direct mapping is `D = N`.

The scheduler rejects work rather than allocating without bound:

- registered flows are limited by `maxFlows`;
- queued plus running jobs are limited by `maxLiveJobs`;
- exact tag components have explicit bit budgets;
- flow and job admission sequences do not wrap or reuse values.

An enqueue may perform one transactional normalization when exact tags approach
their numeric budget. If the result still cannot fit, it returns
`NUMERIC_LIMIT` without changing scheduler state.

`1_000_000` is a representation and validation limit for `issueDepth`, not a
practically tested scale. One `capacityAvailable(k)` call is atomic, holds the
scheduler's internal serialization boundary for the whole selection, and may
create up to `k` `Dispatch` objects. The repository's measurement matrix stops
at depth `1_024`; even within that matrix, no performance claim exists until a
recorded run is reviewed for the target hardware and workload.

## Complexity

Let `R` be registered flows, `Q` queued jobs, `B` backlogged flows, and `m` the
number of jobs returned by one capacity call.

| Operation | Expected or worst-case time |
| --- | ---: |
| register or close flow | expected `O(1)` |
| enqueue to a backlogged flow | expected `O(1)` |
| enqueue that makes a flow backlogged | `O(log B)` |
| cancel a non-head queued job | expected `O(1)` |
| cancel a flow head | `O(log B)` |
| dispatch `m` jobs | `O(m log B + m)` |
| ordinary completion | expected `O(1)` |
| aggregate or per-flow snapshot | expected `O(1)` |
| transition to global idle | `O(R)` |
| rare exact-tag normalization | `O(Q + R)` time and temporary space |

Retained state is `O(Q + running jobs + R)`. Terminal jobs and payloads are
not retained as tombstones.

## Verification and benchmarks

```shell
# Unit, property, differential, coverage, static analysis, JavaDoc, artifacts
./mvnw --batch-mode --no-transfer-progress clean verify

# Bounded JVM concurrency suite
./mvnw --batch-mode --no-transfer-progress -Pjcstress clean verify

# Build JMH and run bounded fixture smoke checks
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify

# List available JMH workloads without running a long benchmark
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l
```

The JMH and jcstress modules are verification tools, not runtime dependencies.
CI packages and discovers the harness, validates representative fixture states,
and runs one 100 ms JMH wiring check. None of those steps is a performance
measurement. See the workload guide for the decision-bearing scale matrix and
measurement protocol.

## Further documentation

- [Practical and normative operation contract](docs/FORMAL_SPEC.md)
- [Production executor pump integration](docs/EXECUTOR_INTEGRATION.md)
- [Theory in plain language and full papers](docs/THEORY.md)
- [Build, CI, artifact, and publication checks](docs/TOOLING.md)
- [Benchmark workload guide](sfqd-benchmarks/README.md)
