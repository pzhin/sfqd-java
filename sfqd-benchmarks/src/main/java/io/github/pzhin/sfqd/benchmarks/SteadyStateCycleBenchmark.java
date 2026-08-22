package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.PAYLOAD;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCompleted;

import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Fixture;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.FlowKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Payload;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Scenario;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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

/** Bounded combined dispatch-complete-enqueue cycles for sustainable throughput and allocation profiling. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class SteadyStateCycleBenchmark {

    /** Per-thread steady scheduler population. */
    @State(Scope.Thread)
    public static class CycleState {
        /** Configured flow count; ONE_HOT keeps only flow zero active. */
        @Param({"1", "10", "100", "1000", "10000"})
        private int flowCount;

        /** Scheduler issue depth. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        /** Deterministic workload distribution. */
        @Param
        private Scenario scenario;

        private Fixture fixture;

        /** Creates the bounded queue outside measured iterations. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount, depth, scenario, 0);
        }

        /** Verifies every cycle returned to the exact initial cardinalities and conserved counters. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(
                    fixture().initialQueued(), SchedulerBenchmarkSupport.expectedActiveFlows(flowCount, scenario));
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.dispatchedTotal() != snapshot.completedTotal()
                    || snapshot.acceptedTotal() != fixture().initialQueued() + snapshot.completedTotal()) {
                throw new IllegalStateException("steady cycle counters diverged: " + snapshot);
            }
        }

        int cycle(int capacity) {
            List<Dispatch<FlowKey, JobKey, Payload>> dispatches = fixture().scheduler().dispatchUpTo(capacity);
            if (dispatches.size() != capacity) {
                throw new IllegalStateException("expected full cycle batch of " + capacity);
            }
            for (Dispatch<FlowKey, JobKey, Payload> dispatch : dispatches) {
                requireCompleted(fixture().scheduler().complete(dispatch.jobHandle()));
                EnqueueResult result = fixture().scheduler().enqueue(
                        fixture().flowHandle(dispatch.flowId().index()),
                        dispatch.jobId(), PAYLOAD, dispatch.cost());
                requireAccepted(result);
            }
            return dispatches.size();
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }
    }

    /**
     * Measures a bounded one-job dispatch-complete-enqueue cycle; one cycle equals one serviced job.
     *
     * @param state bounded cycle state
     * @return serviced-job count, always one
     */
    @Benchmark
    public int singleJobCycle(CycleState state) {
        return state.cycle(1);
    }

    /**
     * Measures full-depth batches; the score unit is batches/s, while the return value proves batch size.
     *
     * @param state bounded cycle state
     * @return serviced-job count, equal to depth
     */
    @Benchmark
    public int batchCycle(CycleState state) {
        return state.cycle(state.depth);
    }
}
