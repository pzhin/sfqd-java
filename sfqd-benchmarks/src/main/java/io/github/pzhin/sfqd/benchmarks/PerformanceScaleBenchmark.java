package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.PAYLOAD;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCompleted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireRegistered;

import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.FlowKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Payload;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.WeightModel;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Explicit B/Q/depth/weight/batch scale matrix for decision-bearing external performance runs. */
public class PerformanceScaleBenchmark {

    /** Every mathematically valid requested pair of backlogged flows and queued jobs. */
    public enum Population {
        /** B=1, Q=1,000. */
        B00001_Q001000(1, 1_000),
        /** B=10, Q=1,000. */
        B00010_Q001000(10, 1_000),
        /** B=100, Q=1,000. */
        B00100_Q001000(100, 1_000),
        /** B=1,000, Q=1,000. */
        B01000_Q001000(1_000, 1_000),
        /** B=1, Q=10,000. */
        B00001_Q010000(1, 10_000),
        /** B=10, Q=10,000. */
        B00010_Q010000(10, 10_000),
        /** B=100, Q=10,000. */
        B00100_Q010000(100, 10_000),
        /** B=1,000, Q=10,000. */
        B01000_Q010000(1_000, 10_000),
        /** B=10,000, Q=10,000. */
        B10000_Q010000(10_000, 10_000),
        /** B=1, Q=100,000. */
        B00001_Q100000(1, 100_000),
        /** B=10, Q=100,000. */
        B00010_Q100000(10, 100_000),
        /** B=100, Q=100,000. */
        B00100_Q100000(100, 100_000),
        /** B=1,000, Q=100,000. */
        B01000_Q100000(1_000, 100_000),
        /** B=10,000, Q=100,000. */
        B10000_Q100000(10_000, 100_000);

        private final int backloggedFlows;
        private final int queuedJobs;

        Population(int backloggedFlows, int queuedJobs) {
            this.backloggedFlows = backloggedFlows;
            this.queuedJobs = queuedJobs;
        }

        int backloggedFlows() {
            return backloggedFlows;
        }

        int queuedJobs() {
            return queuedJobs;
        }
    }

    /** Requested batch limits; the effective size is bounded by depth and queued population. */
    public enum BatchLimit {
        /** At most one job. */
        ONE(1),
        /** At most 16 jobs. */
        SIXTEEN(16),
        /** At most 64 jobs. */
        SIXTY_FOUR(64),
        /** At most 256 jobs. */
        TWO_FIFTY_SIX(256),
        /** Up to the configured issue depth. */
        FULL_DEPTH(Integer.MAX_VALUE);

        private final int limit;

        BatchLimit(int limit) {
            this.limit = limit;
        }

        int effectiveSize(int depth, int queuedJobs) {
            return Math.min(Math.min(limit, depth), queuedJobs);
        }
    }

    /** Shared matrix parameters and a continuously busy scheduler population. */
    @State(Scope.Thread)
    public abstract static class ScaleState {
        /** Compatible B/Q point; combinations with B greater than Q do not exist. */
        @Param
        private Population population;

        /** Requested issue-depth scale. */
        @Param({"1", "16", "64", "256", "1024"})
        private int depth;

        /** Equal or pairwise-coprime flow weights. */
        @Param
        private WeightModel weightModel;

        /** Requested dispatch batch limit. */
        @Param
        private BatchLimit batchLimit;

        private ScaleFixture fixture;

        /** Builds exactly B backlogged flows and Q queued jobs before measurement. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new ScaleFixture(population, depth, weightModel);
        }

        final ScaleFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        final int batchSize() {
            return batchLimit.effectiveSize(depth, population.queuedJobs());
        }

        final void verifySteadyState() {
            fixture().verifySteadyState();
        }
    }

    /** State for atomic dispatch-call latency with restoration outside the JMH timer. */
    @State(Scope.Thread)
    public static class DispatchState extends ScaleState {
        private List<Dispatch<FlowKey, JobKey, Payload>> dispatches = List.of();

        /** Restores every dispatched job without allowing the scheduler to become globally idle. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restore(dispatches, batchSize());
            dispatches = List.of();
        }

        /** Checks exact B/Q restoration and cumulative operation conservation. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            verifySteadyState();
        }

        List<Dispatch<FlowKey, JobKey, Payload>> dispatch() {
            dispatches = fixture().dispatch(batchSize());
            return dispatches;
        }
    }

    /** State for sustainable cycles that remain inside one busy period for the entire trial. */
    @State(Scope.Thread)
    public static class BusyPeriodState extends ScaleState {
        /** Checks exact B/Q restoration and proves that each dispatch was completed and replaced. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            verifySteadyState();
        }

        int cycle() {
            return fixture().cycle(batchSize());
        }
    }

    /** Successful-work counters for interpreting cycle throughput and allocation per serviced job. */
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class WorkCounters {
        private long servicedJobs;
        private long cycles;

        /**
         * Returns jobs dispatched, completed, and replaced during the iteration.
         *
         * @return jobs normalized by JMH as jobs/s
         */
        public long servicedJobs() {
            return servicedJobs;
        }

        /**
         * Returns complete busy-period cycles during the iteration.
         *
         * @return cycles normalized by JMH as cycles/s
         */
        public long cycles() {
            return cycles;
        }

        /** Resets counters at the measurement boundary. */
        @Setup(Level.Iteration)
        public void reset() {
            servicedJobs = 0L;
            cycles = 0L;
        }

        void record(int jobs) {
            servicedJobs += jobs;
            cycles++;
        }
    }

    /**
     * Measures one atomic {@code capacityAvailable} call at the selected scale and batch limit.
     *
     * @param state exact scale fixture
     * @return immutable dispatch batch, whose cardinality is validated during teardown
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public List<Dispatch<FlowKey, JobKey, Payload>> dispatchBatch(DispatchState state) {
        return state.dispatch();
    }

    /**
     * Measures dispatch, completion, and replacement admission without crossing global idle.
     *
     * @param state exact scale fixture
     * @param counters successful work normalized by JMH as jobs/s and cycles/s
     * @return serviced jobs in this cycle
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int continuousBusyPeriodCycle(BusyPeriodState state, WorkCounters counters) {
        int jobs = state.cycle();
        counters.record(jobs);
        return jobs;
    }

    /** Public-API-only exact scale fixture. */
    static final class ScaleFixture {
        private final Population population;
        private final int depth;
        private final SfqdScheduler<FlowKey, JobKey, Payload> scheduler;
        private final FlowHandle[] flows;

        ScaleFixture(Population population, int depth, WeightModel weightModel) {
            this.population = population;
            this.depth = depth;
            scheduler = new SfqdScheduler<>(new SchedulerConfig(
                    depth, population.backloggedFlows(), Math.max(depth, population.queuedJobs())));
            flows = new FlowHandle[population.backloggedFlows()];
            for (int index = 0; index < flows.length; index++) {
                flows[index] = requireRegistered(scheduler.registerFlow(
                        new FlowKey(index), SchedulerBenchmarkSupport.weight(index, weightModel)));
            }
            for (int index = 0; index < population.queuedJobs(); index++) {
                int flow = index % flows.length;
                requireAccepted(scheduler.enqueue(flows[flow], new JobKey(index + 1L), PAYLOAD, 1L));
            }
            verifySteadyState();
        }

        List<Dispatch<FlowKey, JobKey, Payload>> dispatch(int batchSize) {
            List<Dispatch<FlowKey, JobKey, Payload>> result = scheduler.capacityAvailable(batchSize);
            if (result.size() != batchSize) {
                throw new IllegalStateException("expected a full dispatch batch of " + batchSize);
            }
            return result;
        }

        void restore(List<Dispatch<FlowKey, JobKey, Payload>> dispatches, int expectedBatchSize) {
            if (dispatches.size() != expectedBatchSize) {
                throw new IllegalStateException("dispatch batch cardinality changed before restoration");
            }
            restore(dispatches);
        }

        int cycle(int batchSize) {
            List<Dispatch<FlowKey, JobKey, Payload>> dispatches = dispatch(batchSize);
            restore(dispatches);
            return dispatches.size();
        }

        void verifySteadyState() {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            if (snapshot.registeredFlows() != population.backloggedFlows()
                    || snapshot.queuedJobs() != population.queuedJobs()
                    || snapshot.runningJobs() != 0
                    || snapshot.freeSlots() != depth
                    || snapshot.activeFlows() != population.backloggedFlows()
                    || snapshot.backloggedFlows() != population.backloggedFlows()
                    || snapshot.dispatchedTotal() != snapshot.completedTotal()
                    || snapshot.acceptedTotal() != population.queuedJobs() + snapshot.completedTotal()) {
                throw new IllegalStateException("performance-scale fixture diverged: " + snapshot);
            }
        }

        private void restore(List<Dispatch<FlowKey, JobKey, Payload>> dispatches) {
            for (Dispatch<FlowKey, JobKey, Payload> dispatch : dispatches) {
                requireCompleted(scheduler.complete(dispatch.jobHandle()));
                requireAccepted(scheduler.enqueue(
                        flows[dispatch.flowId().index()], dispatch.jobId(), PAYLOAD, dispatch.cost()));
            }
        }
    }
}
