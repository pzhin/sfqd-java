package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.PAYLOAD;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCancelled;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCompleted;

import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Fixture;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.FlowKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Payload;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Scenario;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Bounded producer/consumer groups that expose scheduler-lock throughput and wait latency. */
public class ContentionBenchmark {

    /** Scheduler state shared by every actor in one JMH group. */
    @State(Scope.Group)
    public static class GroupState {
        /** Number of genuinely active consumer flows. */
        @Param({"1", "100", "10000"})
        private int flowCount;

        /** Representative scheduler issue depth. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        /** Representative uniform or weight-skewed distribution. */
        @Param({"UNIFORM", "SKEWED_WEIGHTS"})
        private Scenario scenario;

        private Fixture fixture;
        private final JobKey[] producerJobIds = new JobKey[16];
        private JobKey[][] consumerFreeJobIds;

        /**
         * Creates the bounded consumer queue and gives the producer flow a far-future exact start tag.
         * The debt keeps transient producer jobs away from dispatch without an external lock.
         */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount, depth, scenario, 1);
            consumerFreeJobIds = new JobKey[16][depth];
            for (int index = 0; index < producerJobIds.length; index++) {
                producerJobIds[index] = new JobKey(Long.MIN_VALUE + index);
                for (int slot = 0; slot < depth; slot++) {
                    long offset = producerJobIds.length + (long) index * depth + slot;
                    consumerFreeJobIds[index][slot] = new JobKey(Long.MIN_VALUE + offset);
                }
            }
            JobKey debtId = new JobKey(Long.MAX_VALUE);
            EnqueueResult debt = fixture.scheduler().enqueue(
                    fixture.flowHandle(flowCount), debtId, PAYLOAD, Long.MAX_VALUE);
            JobHandle debtHandle = requireAccepted(debt);
            requireCancelled(fixture.scheduler().cancel(debtHandle));
        }

        /** Verifies every actor transaction returned the scheduler to bounded quiescent shape. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(fixture().initialQueued(), flowCount);
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.dispatchedTotal() != snapshot.completedTotal()
                    || snapshot.acceptedTotal()
                    != fixture().initialQueued() + snapshot.cancelledTotal() + snapshot.completedTotal()) {
                throw new IllegalStateException("contention transaction counters diverged: " + snapshot);
            }
        }

        int producer(ThreadParams threadParams) {
            JobKey jobId = producerJobIds[threadParams.getGroupThreadIndex()];
            EnqueueResult result = fixture().scheduler().enqueue(
                    fixture().flowHandle(flowCount), jobId, PAYLOAD, 1L);
            JobHandle handle = requireAccepted(result);
            requireCancelled(fixture().scheduler().cancel(handle));
            return 2;
        }

        int consumer(ThreadParams threadParams, WorkCounters counters) {
            return consumer(threadParams, counters, true);
        }

        int consumerLatency(ThreadParams threadParams) {
            return consumer(threadParams, null, false);
        }

        private int consumer(ThreadParams threadParams, WorkCounters counters, boolean recordCounters) {
            List<Dispatch<FlowKey, JobKey, Payload>> dispatches = fixture().scheduler().capacityAvailable(depth);
            if (dispatches.isEmpty()) {
                if (recordCounters) {
                    counters.recordEmptyAttempt();
                }
                return 0;
            }
            int actor = threadParams.getGroupThreadIndex();
            for (int slot = 0; slot < dispatches.size(); slot++) {
                Dispatch<FlowKey, JobKey, Payload> dispatch = dispatches.get(slot);
                requireAccepted(fixture().scheduler().enqueue(
                        fixture().flowHandle(dispatch.flowId().index()),
                        consumerFreeJobIds()[actor][slot], PAYLOAD, dispatch.cost()));
                requireCompleted(fixture().scheduler().complete(dispatch.jobHandle()));
                consumerFreeJobIds()[actor][slot] = dispatch.jobId();
            }
            if (recordCounters) {
                counters.recordSuccessfulBatch(dispatches.size());
            }
            return dispatches.size();
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private JobKey[][] consumerFreeJobIds() {
            return Objects.requireNonNull(consumerFreeJobIds, "trial setup did not run");
        }
    }

    /** Observable successful work for throughput groups, normalized by JMH to events per second. */
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class WorkCounters {
        private long successfulJobs;
        private long successfulBatches;
        private long emptyAttempts;

        /** Resets method-backed counters because JMH only resets public counter fields automatically. */
        @Setup(Level.Iteration)
        public void reset() {
            successfulJobs = 0L;
            successfulBatches = 0L;
            emptyAttempts = 0L;
        }

        /**
         * Returns successfully dispatched and completed jobs.
         *
         * @return jobs normalized to jobs/s in throughput mode
         */
        public long successfulJobs() {
            return successfulJobs;
        }

        /**
         * Returns non-empty consumer batch attempts.
         *
         * @return successful batches normalized to batches/s in throughput mode
         */
        public long successfulBatches() {
            return successfulBatches;
        }

        /**
         * Returns consumer attempts that observed no free issue slot.
         *
         * @return empty attempts normalized to attempts/s in throughput mode
         */
        public long emptyAttempts() {
            return emptyAttempts;
        }

        void recordSuccessfulBatch(int jobs) {
            successfulJobs += jobs;
            successfulBatches++;
        }

        void recordEmptyAttempt() {
            emptyAttempts++;
        }
    }

    /**
     * One-producer side of the 1:1 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p1c1Throughput")
    @GroupThreads(1)
    public int p1c1ThroughputProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * One-consumer side of the 1:1 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @param counters observable successful-work counters
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p1c1Throughput")
    @GroupThreads(1)
    public int p1c1ThroughputConsumer(GroupState state, ThreadParams actor, WorkCounters counters) {
        return state.consumer(actor, counters);
    }

    /**
     * Three-producer side of the 3:1 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p3c1Throughput")
    @GroupThreads(3)
    public int p3c1ThroughputProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * One-consumer side of the 3:1 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @param counters observable successful-work counters
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p3c1Throughput")
    @GroupThreads(1)
    public int p3c1ThroughputConsumer(GroupState state, ThreadParams actor, WorkCounters counters) {
        return state.consumer(actor, counters);
    }

    /**
     * One-producer side of the 1:3 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p1c3Throughput")
    @GroupThreads(1)
    public int p1c3ThroughputProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * Three-consumer side of the 1:3 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @param counters observable successful-work counters
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p1c3Throughput")
    @GroupThreads(3)
    public int p1c3ThroughputConsumer(GroupState state, ThreadParams actor, WorkCounters counters) {
        return state.consumer(actor, counters);
    }

    /**
     * Four-producer side of the 4:4 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p4c4Throughput")
    @GroupThreads(4)
    public int p4c4ThroughputProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * Four-consumer side of the 4:4 throughput group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @param counters observable successful-work counters
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Group("p4c4Throughput")
    @GroupThreads(4)
    public int p4c4ThroughputConsumer(GroupState state, ThreadParams actor, WorkCounters counters) {
        return state.consumer(actor, counters);
    }

    /**
     * One-producer side of the 1:1 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p1c1Latency")
    @GroupThreads(1)
    public int p1c1LatencyProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * One-consumer side of the 1:1 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p1c1Latency")
    @GroupThreads(1)
    public int p1c1LatencyConsumer(GroupState state, ThreadParams actor) {
        return state.consumerLatency(actor);
    }

    /**
     * Three-producer side of the 3:1 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p3c1Latency")
    @GroupThreads(3)
    public int p3c1LatencyProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * One-consumer side of the 3:1 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p3c1Latency")
    @GroupThreads(1)
    public int p3c1LatencyConsumer(GroupState state, ThreadParams actor) {
        return state.consumerLatency(actor);
    }

    /**
     * One-producer side of the 1:3 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p1c3Latency")
    @GroupThreads(1)
    public int p1c3LatencyProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * Three-consumer side of the 1:3 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p1c3Latency")
    @GroupThreads(3)
    public int p1c3LatencyConsumer(GroupState state, ThreadParams actor) {
        return state.consumerLatency(actor);
    }

    /**
     * Four-producer side of the 4:4 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated identity
     * @return public operations completed by the transaction
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p4c4Latency")
    @GroupThreads(4)
    public int p4c4LatencyProducer(GroupState state, ThreadParams actor) {
        return state.producer(actor);
    }

    /**
     * Four-consumer side of the 4:4 sampled-latency group.
     *
     * @param state shared group state
     * @param actor JMH actor metadata selecting a preallocated spare identity
     * @return public operations completed, or zero when all slots are concurrently occupied
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Group("p4c4Latency")
    @GroupThreads(4)
    public int p4c4LatencyConsumer(GroupState state, ThreadParams actor) {
        return state.consumerLatency(actor);
    }
}
