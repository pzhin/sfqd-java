# SFQ(D) for Java

`sfqd-core` is a generic, thread-safe scheduler for weighted, cost-aware
work. It decides which jobs may start; it does not execute jobs and does not
own a thread pool, connection pool, or other resource pool.

Use it when several tenants, queues, users, or workloads share a bounded
number of execution slots and should receive service in proportion to their
weights.

## Status

Version 1.0.0 is the first stable release. The library is published to Maven
Central as `io.github.pzhin:sfqd-core:1.0.0` and uses the Java package
`io.github.pzhin.sfqd`.

The benchmark harness is an executable measurement protocol, not a benchmark
result. This repository intentionally contains no raw machine-specific runs,
so production throughput, latency, allocation rate, retained heap, and a
practical maximum batch size have not been established.

The project is licensed under the
[Apache License, Version 2.0](LICENSE).

## Requirements

- JDK 17 or newer
- the checked-in Maven Wrapper

Published classes target Java 17 (`--release 17`), so applications do not need
a newer JDK to consume the library. The ordinary Maven lifecycle is implemented
without POSIX shell commands and is supported by both wrappers.

Build and verify the library:

```shell
./mvnw --batch-mode --no-transfer-progress clean verify
```

On Windows Command Prompt or PowerShell:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress clean verify
```

Install the release into your local Maven repository:

```shell
./mvnw --batch-mode --no-transfer-progress install
```

On Windows, use the same arguments with `.\mvnw.cmd`.

Then add it to a local consumer:

```xml
<dependency>
  <groupId>io.github.pzhin</groupId>
  <artifactId>sfqd-core</artifactId>
  <version>1.0.0</version>
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
import io.github.pzhin.sfqd.CancellationAccounting;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SfqdScheduler;
import io.github.pzhin.sfqd.WeightDomain;

var scheduler = new SfqdScheduler<String, String, Runnable>(
        new SchedulerConfig(
                4,
                1_000,
                100_000,
                CancellationAccounting.CHARGE_RESERVED_COST,
                WeightDomain.divisorsOf(8)));

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

One integration option is a pump that couples external capacity to dispatch
and runs after every accepted enqueue, every completion, and every external
capacity signal. See the
[minimal executor lifecycle example](docs/EXECUTOR_INTEGRATION.md).

## Bounded resource-pool lifecycle example

For a compiled and tested lifecycle example, see the
[`sfqd-examples` bounded resource-pool example](sfqd-examples/README.md). It is
not a reusable production adapter. It keeps cost computation and resource
ownership in the application while making these integration rules explicit:

- configure `D` to equal the pool's maximum concurrently issued resources;
- report only capacity that is actually free;
- treat every returned `Dispatch` as irrevocably running;
- call `complete()` after every accepted task terminates and after synchronous
  submission failure;
- offer each released resource to the scheduler again;
- leave any unused reported resources free in the pool.

The example pulls one job at a time. This avoids pre-dispatching a whole batch
whose later jobs could be stranded if an earlier pool submission throws.

## Public operations

### Register a flow

`registerFlow(flowId, weight)` creates an opaque flow handle. A flow identifier
cannot be registered twice at the same time, and the configured flow and weight
domain limits are enforced. A closed identifier may later be registered again,
producing a new handle.

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

`CancellationAccounting.CHARGE_RESERVED_COST` remains the default for every
constructor that does not receive an explicit policy. Cancelling a queued job
removes it from the queue and live-job indexes and releases its payload, but it
does not roll back the job's reserved virtual cost:

- the flow's `lastFinish` tag is not reduced;
- tags already assigned to later jobs of the flow are not recomputed;
- the charge disappears only when the scheduler becomes globally idle and
  ends the current busy period.

The opt-in alternative is:

```java
new SchedulerConfig(
        issueDepth,
        maxFlows,
        maxLiveJobs,
        CancellationAccounting.REFUND_CANCELLED_COST);
```

With `REFUND_CANCELLED_COST`, a successful queued cancellation removes that
job's cost from future virtual debt. Every later queued job of the same flow is
recomputed in enqueue order from the cancelled job's start tag, and the flow's
finish history moves to the end of that recomputed suffix. Other flows do not
change. The recomputation is one atomic transition under the scheduler's common
lock; callers cannot observe a partial suffix.

Refund admission is intentionally narrower. Before accepting a job, enqueue
checks that the prospective flow queue has one common exact denominator and a
maximum accumulated numerator within the 4096-bit persistent budget. This
reserves enough numeric space for every later subset of queued cancellations.
An otherwise representable enqueue can therefore return `NUMERIC_LIMIT` under
the refund policy while the same enqueue remains accepted under the default
charge-reserved policy. If enqueue attempts a canonical rebase, every affected
queued flow must remain refund-closed or the complete enqueue is an atomic
no-op. An already accepted queued job can always be cancelled without a numeric
failure.

Refund is prospective: dispatch decisions that linearized before cancellation
are never revised. Consequently, the published completed-work fairness bound is
not claimed for traces containing cancellation under either policy without a
separate proof.

For example, under the default charge-reserved policy, consider two
equal-weight flows in a new busy period. Keeping a job from B live prevents an
idle reset:

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
accepted, dispatched, and cancelled supplied-cost units. Current queued supplied
cost is available as `acceptedSuppliedCost - dispatchedSuppliedCost -
cancelledSuppliedCost` through `queuedSuppliedCost()`.
`runningSuppliedCost()` reports the committed in-flight supplied cost directly,
while `completedSuppliedCost()` derives the cumulative completed supplied cost
as dispatched supplied cost minus running supplied cost. All six cost accessors
describe caller-supplied service estimates, not actual execution time. Cost
values use `BigInteger`, so valid `long` costs do not overflow observability
counters.

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

`REFUND_CANCELLED_COST` must always be selected explicitly. Existing
constructors and configurations using `CHARGE_RESERVED_COST` retain their
previous behavior and admission domain.

The three- and four-argument forms preserve the unrestricted positive `long`
weight domain. For production configurations with a known common scale, the
five-argument form can reject weights outside a denominator-safe profile at
registration:

```java
new SchedulerConfig(
        issueDepth,
        maxFlows,
        maxLiveJobs,
        CancellationAccounting.CHARGE_RESERVED_COST,
        WeightDomain.divisorsOf(8));
```

This profile accepts `8, 4, 2, 1, 1`: every weight divides `8`. Consequently,
every reduced `cost / weight` denominator divides `8`, and exact addition,
maximum, and rebase subtraction cannot introduce new denominator factors.
`WeightDomain.unrestricted()` remains available for workloads that need the
full `long` range.

The divisor profile prevents denominator growth caused by mutually coprime
weights; it is not an unconditional promise that `NUMERIC_LIMIT` can never
occur. Numerators and accumulated cancellation debt still use the documented
finite exact-arithmetic budget. In unrestricted mode, a natural trace with an
anchor job and 69 consecutive prime weights above `2^60` reaches
`NUMERIC_LIMIT` after 273 successful admissions; this behavior is covered by a
regression test.

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
`NUMERIC_LIMIT` without changing scheduler state. Refund accounting additionally
reserves numeric space for every later queued cancellation, so it may reject a
larger portion of the otherwise valid admission domain.

`1_000_000` is a representation and validation limit for `issueDepth`, not a
practically tested scale. One `dispatchUpTo(k)` call is atomic, holds the
scheduler's internal serialization boundary for the whole selection, and may
create up to `k` `Dispatch` objects. The repository's measurement matrix stops
at depth `1_024`; even within that matrix, no performance claim exists until a
recorded run is reviewed for the target hardware and workload.

## Complexity

Let `R` be registered flows, `Q` queued jobs, `B` backlogged flows, `K` jobs in
the affected per-flow queue or suffix, and `m` the number of jobs returned by
one capacity call.

| Operation | Expected or worst-case time |
| --- | ---: |
| register or close flow | expected `O(1)` |
| charge-reserved enqueue to a backlogged flow | expected `O(1)` |
| charge-reserved enqueue that makes a flow backlogged | `O(log B)` |
| refund enqueue to a backlogged flow | `O(K)` |
| refund enqueue that makes a flow backlogged | `O(K + log B)` |
| charge-reserved cancel of a non-head queued job | expected `O(1)` |
| charge-reserved cancel of a flow head | `O(log B)` |
| refund cancel without changing the indexed head | `O(K)` |
| refund cancel that changes the indexed head | `O(K + log B)` |
| dispatch `m` jobs | `O(m log B + m)` |
| ordinary completion | expected `O(1)` |
| aggregate or per-flow snapshot | expected `O(1)` |
| transition to global idle | `O(R)` |
| rare exact-tag normalization | `O(Q + R)` time and temporary space |

Retained state is `O(Q + running jobs + R)`. Terminal jobs and payloads are
not retained as tombstones. These are algorithmic complexity bounds, not
measured performance claims; no throughput or latency claim is made for refund
accounting without a preserved benchmark run.

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

The examples, JMH, and jcstress modules are verification tools, not runtime
dependencies.
CI packages and discovers the harness, validates representative fixture states,
and runs one 100 ms JMH wiring check. None of those steps is a performance
measurement. See the workload guide for the decision-bearing scale matrix and
measurement protocol.

## Further documentation

- [Practical and normative operation contract](docs/FORMAL_SPEC.md)
- [Minimal executor lifecycle example](docs/EXECUTOR_INTEGRATION.md)
- [Bounded resource-pool lifecycle example](sfqd-examples/README.md)
- [Theory in plain language and full papers](docs/THEORY.md)
- [Build, CI, artifact, and publication checks](docs/TOOLING.md)
- [Release history](CHANGELOG.md)
- [Benchmark workload guide](sfqd-benchmarks/README.md)
