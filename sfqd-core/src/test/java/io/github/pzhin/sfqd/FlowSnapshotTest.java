package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

final class FlowSnapshotTest {
    @Test
    void reportsExactRegistrationLifetimeCostsAndCurrentCounts() {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 2));
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));

        assertEquals(snapshot(0, 0, 0L, 0L, 0L), scheduler.snapshot(flow).orElseThrow());

        JobHandle running = accepted(scheduler.enqueue(flow, "running", "payload", Long.MAX_VALUE));
        JobHandle cancelled = accepted(scheduler.enqueue(flow, "cancelled", "payload", Long.MAX_VALUE));
        BigInteger twiceMaximum = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO);
        FlowSnapshot queued = scheduler.snapshot(flow).orElseThrow();
        assertEquals(2, queued.queuedJobs());
        assertEquals(0, queued.runningJobs());
        assertEquals(twiceMaximum, queued.acceptedCost());
        assertEquals(BigInteger.ZERO, queued.dispatchedCost());
        assertEquals(BigInteger.ZERO, queued.cancelledCost());
        assertEquals(twiceMaximum, queued.queuedCost());
        assertEquals(BigInteger.ZERO, queued.runningSuppliedCost());
        assertEquals(BigInteger.ZERO, queued.completedSuppliedCost());

        scheduler.dispatchUpTo(1);
        assertEquals(
                new FlowSnapshot(
                        1, 1, twiceMaximum, BigInteger.valueOf(Long.MAX_VALUE), BigInteger.ZERO,
                        BigInteger.valueOf(Long.MAX_VALUE)),
                scheduler.snapshot(flow).orElseThrow());

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(cancelled));
        FlowSnapshot afterCancellation = scheduler.snapshot(flow).orElseThrow();
        assertEquals(0, afterCancellation.queuedJobs());
        assertEquals(1, afterCancellation.runningJobs());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), afterCancellation.dispatchedCost());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), afterCancellation.cancelledCost());
        assertEquals(BigInteger.ZERO, afterCancellation.queuedCost());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), afterCancellation.runningSuppliedCost());
        assertEquals(BigInteger.ZERO, afterCancellation.completedSuppliedCost());

        assertEquals(CompletionResult.COMPLETED, scheduler.complete(running));
        FlowSnapshot completed = scheduler.snapshot(flow).orElseThrow();
        assertEquals(0, completed.runningJobs());
        assertEquals(BigInteger.ZERO, completed.runningSuppliedCost());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), completed.completedSuppliedCost());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(flow));
        assertFalse(scheduler.snapshot(flow).isPresent());
    }

    @Test
    void distinguishesRunningJobCountsFromTheirSuppliedCosts() {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 1, 2));
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle cheap = accepted(scheduler.enqueue(flow, "cheap", "payload", 1L));
        JobHandle expensive = accepted(scheduler.enqueue(flow, "expensive", "payload", 1_000_000L));

        scheduler.dispatchUpTo(2);
        FlowSnapshot bothRunning = scheduler.snapshot(flow).orElseThrow();
        assertEquals(2, bothRunning.runningJobs());
        assertEquals(BigInteger.valueOf(1_000_001L), bothRunning.runningSuppliedCost());
        assertEquals(BigInteger.ZERO, bothRunning.completedSuppliedCost());

        assertEquals(CompletionResult.COMPLETED, scheduler.complete(cheap));
        FlowSnapshot expensiveRunning = scheduler.snapshot(flow).orElseThrow();
        assertEquals(1, expensiveRunning.runningJobs());
        assertEquals(BigInteger.valueOf(1_000_000L), expensiveRunning.runningSuppliedCost());
        assertEquals(BigInteger.ONE, expensiveRunning.completedSuppliedCost());

        assertEquals(CompletionResult.COMPLETED, scheduler.complete(expensive));
        assertEquals(BigInteger.ZERO, scheduler.snapshot(flow).orElseThrow().runningSuppliedCost());
    }

    @Test
    void rejectsNullAndDoesNotExposeForeignOrReplacedRegistrations() {
        SchedulerConfig config = new SchedulerConfig(1, 1, 1);
        SfqdScheduler<String, String, String> scheduler = new SfqdScheduler<>(config);
        SfqdScheduler<String, String, String> other = new SfqdScheduler<>(config);
        FlowHandle stale = registered(scheduler.registerFlow("flow", 1L));
        FlowHandle foreign = registered(other.registerFlow("flow", 1L));

        assertFalse(scheduler.snapshot(foreign).isPresent());
        assertThrows(NullPointerException.class, () -> scheduler.snapshot(nullReference()));
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(stale));
        FlowHandle replacement = registered(scheduler.registerFlow("flow", 1L));

        assertFalse(scheduler.snapshot(stale).isPresent());
        assertEquals(snapshot(0, 0, 0L, 0L, 0L), scheduler.snapshot(replacement).orElseThrow());
    }

    private static FlowSnapshot snapshot(
            int queuedJobs, int runningJobs, long acceptedCost, long dispatchedCost, long cancelledCost) {
        return new FlowSnapshot(
                queuedJobs, runningJobs,
                BigInteger.valueOf(acceptedCost),
                BigInteger.valueOf(dispatchedCost),
                BigInteger.valueOf(cancelledCost),
                BigInteger.ZERO);
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private static <T> T nullReference() {
        return null;
    }
}
