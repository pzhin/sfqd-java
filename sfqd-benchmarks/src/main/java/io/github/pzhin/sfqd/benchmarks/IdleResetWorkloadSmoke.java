package io.github.pzhin.sfqd.benchmarks;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.FirstBusyPeriodFixture;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.TerminalFixture;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.TerminalOperation;

/** Executable exact-matrix state/teardown validation for the global-idle-reset workloads. */
public final class IdleResetWorkloadSmoke {
    private static final int[] FULL_FLOW_COUNTS = {1, 10, 100, 1_000, 10_000};
    private static final int[] FULL_DEPTHS = {1, 8, 64, 256};
    private static final int[] ALL_TAGGED_FLOW_COUNTS = {1, 100, 10_000};
    private static final int[] TARGET_DEPTHS = {1, 256};
    private static final int[] FIRST_BUSY_FLOW_COUNTS = {1, 10_000};

    private IdleResetWorkloadSmoke() {
    }

    /**
     * Exercises each frozen parameter combination once and fails on any public invariant violation.
     *
     * @param arguments no arguments are accepted
     */
    public static void main(String[] arguments) {
        if (arguments.length != 0) {
            throw new IllegalArgumentException("idle-reset workload smoke accepts no arguments");
        }
        int cases = 0;
        for (int flowCount : FULL_FLOW_COUNTS) {
            for (int depth : FULL_DEPTHS) {
                exerciseCompletion(flowCount, depth, false);
                exerciseCancellation(flowCount, depth, false);
                cases += 2;
            }
        }
        for (int flowCount : ALL_TAGGED_FLOW_COUNTS) {
            for (int depth : TARGET_DEPTHS) {
                exerciseCompletion(flowCount, depth, true);
                exerciseCancellation(flowCount, depth, true);
                cases += 2;
            }
        }
        for (int flowCount : FIRST_BUSY_FLOW_COUNTS) {
            for (int depth : TARGET_DEPTHS) {
                exerciseFirstAdmission(flowCount, depth);
                exerciseCycle(flowCount, depth);
                cases += 2;
            }
        }
        if (cases != 60) {
            throw new IllegalStateException("unexpected idle-reset smoke matrix size: " + cases);
        }
        System.out.println("IDLE_RESET_WORKLOAD_SMOKE PASS cases=" + cases);
    }

    private static void exerciseCompletion(int flowCount, int depth, boolean allTagged) {
        TerminalFixture fixture = new TerminalFixture(
                flowCount, depth, allTagged, TerminalOperation.COMPLETE);
        CompletionResult result = fixture.completeLastRunning();
        fixture.restoreAfterCompletion(result);
        fixture.verifyPrepared();
    }

    private static void exerciseCancellation(int flowCount, int depth, boolean allTagged) {
        TerminalFixture fixture = new TerminalFixture(
                flowCount, depth, allTagged, TerminalOperation.CANCEL);
        CancelResult result = fixture.cancelLastQueued();
        fixture.restoreAfterCancellation(result);
        fixture.verifyPrepared();
    }

    private static void exerciseFirstAdmission(int flowCount, int depth) {
        FirstBusyPeriodFixture fixture = new FirstBusyPeriodFixture(flowCount, depth);
        EnqueueResult result = fixture.enqueueFirstBusyPeriod();
        fixture.restoreAfterEnqueue(result);
        fixture.verifyIdle();
    }

    private static void exerciseCycle(int flowCount, int depth) {
        FirstBusyPeriodFixture fixture = new FirstBusyPeriodFixture(flowCount, depth);
        if (fixture.enqueueCancelCycle() != 1) {
            throw new IllegalStateException("first-busy-period cycle count diverged");
        }
        fixture.verifyIdle();
    }
}
