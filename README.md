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
        : scheduler.capacityAvailable(4)) {
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

`capacityAvailable(k)` does not reserve future permits. Every returned job is
already dispatched and must eventually be completed, even when submission to
your executor fails.

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

### Offer capacity

`capacityAvailable(k)` returns an immutable batch of up to `k` jobs, bounded
by configured issue depth `D`, available issue slots, and queue size. A
non-empty batch is one atomic scheduling decision.

### Complete or cancel

`complete(jobHandle)` releases a slot held by a dispatched job. It does not
automatically dispatch a replacement; call `capacityAvailable` again when
capacity is available.

`cancel(jobHandle)` succeeds only while the job is still queued. Once a job is
dispatched, it must be completed.

### Close a flow

`closeFlow(flowHandle)` succeeds only for an inactive flow and only while the
scheduler is globally idle. This keeps registration removal separate from an
active scheduling period.

### Inspect state

`snapshot()` returns constant-size counts and cumulative counters. It does not
expose internal queues or mutable scheduler state.

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

`SchedulerConfig(issueDepth, maxFlows, maxLiveJobs)` enforces:

- `issueDepth`: `1..1_000_000`;
- `maxFlows`: `1..Integer.MAX_VALUE`;
- `maxLiveJobs`: `issueDepth..Integer.MAX_VALUE`.

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
| snapshot | `O(1)` |
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

# Build JMH and run the fixture smoke matrix
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify

# List available JMH workloads without running a long benchmark
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l
```

The JMH and jcstress modules are verification tools, not runtime dependencies.
This release branch intentionally does not contain raw machine-specific
performance runs.

## Further documentation

- [Practical and normative operation contract](docs/FORMAL_SPEC.md)
- [Theory in plain language and full papers](docs/THEORY.md)
- [Build, CI, artifact, and publication checks](docs/TOOLING.md)
- [Benchmark workload guide](sfqd-benchmarks/README.md)
