# SFQ(D) JMH workloads

This module contains JMH 1.37 workloads for the scheduler. It is enabled only
by the Maven `benchmarks` profile and is never a runtime dependency of
`sfqd-core`. The workloads define how to measure performance; they do not
establish production performance by existing in the repository.

## Build and discover workloads

```shell
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l
```

The Maven `verify` phase also runs a bounded 60-case idle-reset fixture smoke
and three scale-fixture cases. The largest scale fixture uses `B=10_000`,
`Q=100_000`, and `depth=1_024`. These checks validate scheduler state and
workload restoration; their execution time is not a benchmark result.

## Workload families

### Operation latency

`OperationLatencyBenchmark` measures individual public calls such as enqueue,
dispatch, cancellation, and completion. Invocation setup selects a prebuilt
target; teardown validates the result and restores bounded scheduler state.

### Steady-state cycles

`SteadyStateCycleBenchmark` measures a complete
`dispatchUpTo -> complete -> enqueue replacement` transaction. A batch
score is batches per second. Convert it to jobs per second only after the
full-batch invariant has passed.

### Performance-scale matrix

`PerformanceScaleBenchmark` makes the decision-bearing dimensions explicit:

- backlogged flows `B = 1, 10, 100, 1_000, 10_000`;
- queued jobs `Q = 1_000, 10_000, 100_000`;
- issue depths `1, 16, 64, 256, 1_024`;
- equal weights and a distinct-prime, pairwise-coprime weight population;
- batch limits `1, 16, 64, 256`, and full depth.

All 14 possible `(B,Q)` pairs with `B <= Q` are named `Population` values.
`B=10_000, Q=1_000` is omitted because 10,000 flows cannot all be backlogged
by 1,000 queued jobs. The effective batch size is
`min(batch limit, depth, Q)`; this matters for `FULL_DEPTH` when `depth > Q`.

`dispatchBatch` measures the atomic `capacityAvailable` call and restores its
state outside the JMH timer. `continuousBusyPeriodCycle` measures dispatch,
completion, and replacement admission. Its scheduler starts busy and never
reaches global idle during the trial, so long runs retain flow history and
exercise exact-tag growth instead of repeatedly measuring idle reset.

For example, a single scale point can be recorded as JSON with allocation
counters:

```shell
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar \
  '^io\.github\.pzhin\.sfqd\.benchmarks\.PerformanceScaleBenchmark\.continuousBusyPeriodCycle$' \
  -p population=B10000_Q100000 -p depth=1024 \
  -p weightModel=PAIRWISE_COPRIME -p batchLimit=FULL_DEPTH \
  -prof gc -wi 5 -i 10 -f 3 -w 2s -r 5s \
  -rf json -rff /absolute/output/path/continuous-busy-period.json
```

The primary JMH score is cycles/s. The `servicedJobs` auxiliary counter reports
jobs/s, while `cycles` independently checks the invocation rate. For GC profiler
output, allocation bytes per serviced job are
`gc.alloc.rate.norm / effective batch size`; record both the original cycle
metric and the normalization.

### Cancellation cycles

`CancellationCycleBenchmark` measures `cancel -> enqueue replacement`. The
score belongs to the combined cycle, not cancellation alone.

### First busy-period cycles

`FirstBusyPeriodCycleBenchmark` measures
`enqueue first job -> cancel last queued job`, returning the scheduler to
global idle.

### Contention

`ContentionBenchmark` runs 1:1, 3:1, 1:3, and 4:4 producer/consumer groups
against one shared scheduler. It covers the requested flow and depth scales
with equal and pairwise-coprime weights. Consumer invocation throughput counts
attempts, including empty attempts. Auxiliary counters expose successful jobs,
successful batches, and empty attempts so useful work is not confused with
actor invocation rate.

### Retained-heap fixture

`RetainedHeapFixture` constructs a scheduler with a requested live-state
snapshot, prints its PID and configuration, and waits. It is a diagnostic aid
for an external profiler; it does not calculate retained bytes by itself.

```shell
java -cp sfqd-benchmarks/target/sfqd-benchmarks.jar \
  io.github.pzhin.sfqd.benchmarks.RetainedHeapFixture \
  --flowCount=100 --queuedJobs=10000 --runningJobs=0 \
  --depth=64 --weightModel=PAIRWISE_COPRIME --holdSeconds=60
```

`flowCount` and `depth` must be positive. Queued, running, and hold counts must
be nonnegative; `runningJobs` must not exceed `depth`, and queued plus running
jobs must fit in a Java `int`. `holdSeconds=0` waits until the process is
terminated. `PAIRWISE_COPRIME` supports at most 10,000 registered flows. Run
each measured point in a fresh JVM and verify the printed `CONFIG`, `SNAPSHOT`,
and `READY` records before attaching a profiler. Measure a matching empty-JVM
or empty-scheduler baseline with the same JVM options before attributing the
retained difference to scheduler state.

## Parameter scenarios

The reusable state supports these scenarios:

- `UNIFORM`: active flows have equal weights and costs;
- `ONE_HOT`: one active flow with a larger registered population;
- `ALL_BACKLOGGED`: every flow has queued work;
- `SKEWED_WEIGHTS`: deterministic weights `1, 2, 4, 8, 16, 32`;
- `PAIRWISE_COPRIME_WEIGHTS`: one distinct prime weight per active flow;
- `SKEWED_COSTS`: deterministic costs `1, 2, 4, 8, 16, 32, 64`.

Flow counts and issue depths intentionally include small and large boundary
points. Teardown checks cardinalities, cumulative counters, and restoration of
the caller model. A failed invariant fails the fork.

## Short local smoke

This command demonstrates wiring only:

```shell
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar \
  '^io\.github\.pzhin\.sfqd\.benchmarks\.OperationLatencyBenchmark\.enqueueBackloggedTail$' \
  -p flowCount=1 -p depth=1 -p scenario=UNIFORM \
  -wi 0 -i 1 -f 1 -r 100ms
```

Do not treat a short run, a laptop result, or `-prof gc` output as a universal
service-level objective. Before using a measurement for an engineering
decision, retain:

- the exact source commit and a clean-worktree check;
- JDK vendor/version, JVM flags, CPU, memory, operating system, and power mode;
- the complete command, forks, warmups, iterations, durations, and raw output;
- throughput plus tail-latency percentiles, allocation rate, and retained-heap
  evidence for the same `(B,Q,depth,weights,batch)` point;
- an explanation of outliers, failed forks, thermal throttling, and any profiler
  overhead.

The public maximum `depth=1_000_000` is a representation and validation limit.
It is outside this measurement matrix. A call at that limit can hold the single
internal serialization boundary for a long time and allocate up to one million
`Dispatch` objects; no practical-scale claim should extend to it without a
separately reviewed measurement protocol and raw result.
