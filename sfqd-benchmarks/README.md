# SFQ(D) JMH workloads

This module contains JMH 1.37 workloads for the production scheduler. It is
enabled only by the Maven `benchmarks` profile and is never a runtime
dependency of `sfqd-core`.

## Build and discover workloads

```shell
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l
```

The Maven `verify` phase also runs a bounded 60-case fixture smoke. The smoke
checks scheduler state and workload restoration; it is not a performance
measurement.

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

### Cancellation cycles

`CancellationCycleBenchmark` measures `cancel -> enqueue replacement`. The
score belongs to the combined cycle, not cancellation alone.

### First busy-period cycles

`FirstBusyPeriodCycleBenchmark` measures
`enqueue first job -> cancel last queued job`, returning the scheduler to
global idle.

### Contention

`ContentionBenchmark` runs producer and consumer groups against one shared
scheduler. Consumer invocation throughput counts attempts, including empty
attempts. Auxiliary counters expose successful jobs, successful batches, and
empty attempts so useful work is not confused with actor invocation rate.

### Retained-heap fixture

`RetainedHeapFixture` constructs a scheduler with a requested live-state
snapshot, prints its PID and configuration, and waits. It is a diagnostic aid
for an external profiler; it does not calculate retained bytes by itself.

```shell
java -cp sfqd-benchmarks/target/sfqd-benchmarks.jar \
  io.github.pzhin.sfqd.benchmarks.RetainedHeapFixture \
  --flowCount=100 --queuedJobs=10000 --runningJobs=0 \
  --depth=64 --holdSeconds=60
```

`flowCount` and `depth` must be positive. Queued, running, and hold counts must
be nonnegative; `runningJobs` must not exceed `depth`, and queued plus running
jobs must fit in a Java `int`. `holdSeconds=0` waits until the process is
terminated. Run each measured point in a fresh JVM and verify the printed
`CONFIG`, `SNAPSHOT`, and `READY` records before attaching a profiler.

## Parameter scenarios

The reusable state supports these scenarios:

- `UNIFORM`: active flows have equal weights and costs;
- `ONE_HOT`: one active flow with a larger registered population;
- `ALL_BACKLOGGED`: every flow has queued work;
- `SKEWED_WEIGHTS`: deterministic weights `1, 2, 4, 8, 16, 32`;
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
service-level objective. Record the exact source commit, JDK, JVM flags,
machine, command line, forks, warmups, iterations, and raw JMH output before
using a measurement for an engineering decision.
