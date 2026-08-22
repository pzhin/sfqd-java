package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCancelled;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Fixture;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobRecord;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Scenario;
import java.util.ArrayDeque;
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

/** Bounded frequent-cancellation workload measured as cancel-plus-enqueue cycles. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CancellationCycleBenchmark {

    /** Cancellation target distribution. */
    public enum Target {
        HEAD,
        NON_HEAD,
        HALF_HEAD_HALF_NON_HEAD
    }

    /** Per-thread cancellation fixture. */
    @State(Scope.Thread)
    public static class CancellationState {
        /** Configured flow count; ONE_HOT keeps only flow zero active. */
        @Param({"1", "10", "100", "1000", "10000"})
        private int flowCount;

        /** Scheduler issue depth. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        /** Deterministic workload distribution. */
        @Param
        private Scenario scenario;

        /** Head/non-head selection policy. */
        @Param
        private Target target;

        private Fixture fixture;
        private boolean nextHead;
        private int nextFlow;
        private boolean observedNonzeroFlow;
        private boolean selectedHead;
        private ArrayDeque<JobRecord> selectedQueue;
        private JobRecord selectedRecord;
        private CancelResult cancelResult;
        private EnqueueResult enqueueResult;

        /** Creates the bounded caller and scheduler queues outside measurements. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount, depth, scenario, 0, 2);
        }

        /** Selects and removes caller-side target bookkeeping outside the timed cycle. */
        @Setup(Level.Invocation)
        public void selectTarget() {
            int flow = scenario == Scenario.ONE_HOT ? 0 : nextFlow++ % flowCount;
            observedNonzeroFlow |= flow != 0;
            selectedHead = target == Target.HEAD
                    || target == Target.HALF_HEAD_HALF_NON_HEAD && (nextHead = !nextHead);
            selectedQueue = fixture().queue(flow);
            selectedRecord = selectedHead ? selectedQueue().removeFirst() : selectedQueue().removeLast();
        }

        /** Validates both scheduler outcomes and restores caller-side queue bookkeeping. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            requireCancelled(cancelResult());
            selectedRecord().replaceHandle(requireAccepted(enqueueResult()));
            selectedQueue().addLast(selectedRecord());
            cancelResult = null;
            enqueueResult = null;
        }

        /** Verifies the cancellation cycle never grows live state. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(
                    fixture().initialQueued(), SchedulerBenchmarkSupport.expectedActiveFlows(flowCount, scenario));
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.acceptedTotal() != fixture().initialQueued() + snapshot.cancelledTotal()) {
                throw new IllegalStateException("cancellation cycle counters diverged: " + snapshot);
            }
            if (flowCount > 1 && scenario != Scenario.ONE_HOT && !observedNonzeroFlow) {
                throw new IllegalStateException("cancellation cycle never exercised a nonzero flow index");
            }
        }

        int cycle() {
            JobRecord record = selectedRecord();
            cancelResult = fixture().scheduler().cancel(record.handle());
            enqueueResult = fixture().scheduler().enqueue(
                    fixture().flowHandle(record.flowIndex()), record.jobId(),
                    SchedulerBenchmarkSupport.PAYLOAD, record.cost());
            return selectedHead ? 1 : 0;
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private ArrayDeque<JobRecord> selectedQueue() {
            return Objects.requireNonNull(selectedQueue, "invocation setup did not run");
        }

        private JobRecord selectedRecord() {
            return Objects.requireNonNull(selectedRecord, "invocation setup did not run");
        }

        private CancelResult cancelResult() {
            return Objects.requireNonNull(cancelResult, "benchmark invocation did not run");
        }

        private EnqueueResult enqueueResult() {
            return Objects.requireNonNull(enqueueResult, "benchmark invocation did not run");
        }
    }

    /**
     * Measures one bounded cancellation and replacement admission; the score unit is cycles/s.
     *
     * @param state bounded cancellation state
     * @return one for a head target and zero for a non-head target
     */
    @Benchmark
    public int cancelAndEnqueue(CancellationState state) {
        return state.cycle();
    }
}
