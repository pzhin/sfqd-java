package io.github.pzhin.sfqd.benchmarks;

import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.FirstBusyPeriodFixture;
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

/** Bounded first-admission plus terminal-cancellation cycles for throughput and allocation profiling. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FirstBusyPeriodCycleBenchmark {

    /** Per-thread scheduler that returns to global idle after every measured transaction. */
    @State(Scope.Thread)
    public static class CycleState {
        /** Registered-flow population. */
        @Param({"1", "10000"})
        private int flowCount;

        /** Representative issue depths. */
        @Param({"1", "256"})
        private int depth;

        private FirstBusyPeriodFixture fixture;

        /** Registers all flows and establishes the initial globally idle boundary. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new FirstBusyPeriodFixture(flowCount, depth);
        }

        /** Verifies that every measured transaction conserved bounded idle state. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyIdle();
        }

        int cycle() {
            return fixture().enqueueCancelCycle();
        }

        private FirstBusyPeriodFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }
    }

    /**
     * Measures one enqueue-first-busy-period plus cancel-last-queued transaction.
     *
     * @param state bounded idle-reset cycle state
     * @return completed cycle count, always one
     */
    @Benchmark
    public int enqueueCancelCycle(CycleState state) {
        return state.cycle();
    }
}
