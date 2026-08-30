package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SfqdRefundCancellationTest {
    @Test
    void refundOfHeadChangesGlobalOrderWhileDefaultRemainsChargeReserved() {
        SfqdScheduler<String, String, String> refund = scheduler(3, 2, 5,
                CancellationAccounting.REFUND_CANCELLED_COST);
        FlowHandle refundA = registered(refund.registerFlow("a", 2L));
        FlowHandle refundB = registered(refund.registerFlow("b", 1L));
        JobHandle cancelled = accepted(refund.enqueue(refundA, "cancelled", "p", 4L));
        refund.enqueue(refundA, "successor", "p", 2L);
        refund.enqueue(refundA, "second-successor", "p", 2L);
        refund.enqueue(refundB, "competitor", "p", 1L);

        assertEquals(CancelResult.CANCELLED, refund.cancel(cancelled));
        assertEquals(List.of("successor", "competitor", "second-successor"), ids(refund.dispatchUpTo(3)));

        SfqdScheduler<String, String, String> charged = new SfqdScheduler<>(new SchedulerConfig(3, 2, 4));
        FlowHandle chargedA = registered(charged.registerFlow("a", 2L));
        FlowHandle chargedB = registered(charged.registerFlow("b", 1L));
        JobHandle chargedCancellation = accepted(charged.enqueue(chargedA, "cancelled", "p", 4L));
        charged.enqueue(chargedA, "successor", "p", 2L);
        charged.enqueue(chargedB, "competitor", "p", 1L);

        assertEquals(CancelResult.CANCELLED, charged.cancel(chargedCancellation));
        assertEquals(List.of("competitor", "successor"), ids(charged.dispatchUpTo(3)));
        assertEquals(CancellationAccounting.CHARGE_RESERVED_COST,
                new SchedulerConfig(1, 1, 1).cancellationAccounting());
    }

    @Test
    void refundOfMiddleAndTailPreservesOtherFlowAndCostCounters() {
        SfqdScheduler<String, String, String> scheduler = scheduler(3, 2, 6,
                CancellationAccounting.REFUND_CANCELLED_COST);
        FlowHandle target = registered(scheduler.registerFlow("target", 6L));
        FlowHandle other = registered(scheduler.registerFlow("other", 5L));
        scheduler.enqueue(target, "first", "p", 1L);
        JobHandle middle = accepted(scheduler.enqueue(target, "middle", "p", 2L));
        JobHandle tail = accepted(scheduler.enqueue(target, "tail", "p", 3L));
        scheduler.enqueue(other, "other", "p", 2L);

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(middle));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(tail));
        scheduler.enqueue(target, "next", "p", 1L);

        assertEquals(List.of("first", "other", "next"), ids(scheduler.dispatchUpTo(3)));
        assertEquals(new FlowSnapshot(
                0,
                2,
                BigInteger.valueOf(7L),
                BigInteger.valueOf(2L),
                BigInteger.valueOf(5L),
                BigInteger.valueOf(2L)), scheduler.snapshot(target).orElseThrow());
        assertEquals(new FlowSnapshot(
                0,
                1,
                BigInteger.valueOf(2L),
                BigInteger.valueOf(2L),
                BigInteger.ZERO,
                BigInteger.valueOf(2L)), scheduler.snapshot(other).orElseThrow());
        assertEquals(5L, scheduler.snapshot().acceptedTotal());
        assertEquals(2L, scheduler.snapshot().cancelledTotal());
    }

    @Test
    void refundPreservesRunningDebtAndIdleReset() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 2, 5,
                CancellationAccounting.REFUND_CANCELLED_COST);
        FlowHandle target = registered(scheduler.registerFlow("target", 2L));
        FlowHandle progress = registered(scheduler.registerFlow("progress", 1L));
        JobHandle running = accepted(scheduler.enqueue(target, "running", "p", 2L));
        JobHandle cancelled = accepted(scheduler.enqueue(target, "cancelled", "p", 4L));
        assertSame(running, scheduler.dispatchUpTo(1).get(0).jobHandle());

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(cancelled));
        JobHandle afterRefund = accepted(scheduler.enqueue(target, "after-refund", "p", 2L));
        JobHandle progressJob = accepted(scheduler.enqueue(progress, "progress", "p", 1L));
        assertEquals(List.of("progress"), ids(scheduler.dispatchUpTo(1)));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(afterRefund));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(running));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(progressJob));

        scheduler.enqueue(target, "after-idle", "p", 1L);
        scheduler.enqueue(progress, "tie", "p", 1L);
        assertEquals(List.of("after-idle", "tie"), ids(scheduler.dispatchUpTo(2)));
    }

    @Test
    void reachableRefundClosureTraceRejectsSecondAdmissionAsAtomicNoOp()
            throws ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(RefundNumericRegressionTrace.config());
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        JobHandle anchorJob = accepted(scheduler.enqueue(anchor, "anchor", "p", 1L));
        assertSame(anchorJob, scheduler.dispatchUpTo(1).get(0).jobHandle());
        int enqueueAttempts = 1;

        int builderId = 0;
        for (long weight : RefundNumericRegressionTrace.builderWeights()) {
            FlowHandle builder = registered(scheduler.registerFlow("builder-" + builderId, weight));
            JobHandle firstBuilder = accepted(scheduler.enqueue(
                    builder, "builder-" + builderId + "-first", "p", 1L));
            JobHandle secondBuilder = accepted(scheduler.enqueue(
                    builder, "builder-" + builderId + "-second", "p", 1L));
            enqueueAttempts += 2;
            assertSame(firstBuilder, scheduler.dispatchUpTo(1).get(0).jobHandle());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(firstBuilder));
            assertSame(secondBuilder, scheduler.dispatchUpTo(1).get(0).jobHandle());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(secondBuilder));
            builderId++;
        }

        FlowHandle target = registered(scheduler.registerFlow("target", 6L));
        JobHandle first = accepted(scheduler.enqueue(target, "first", "p", 3L));
        enqueueAttempts++;
        SchedulerSnapshot publicBefore = scheduler.snapshot();
        Object before = SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler);
        assertEquals(1, publicBefore.runningJobs());
        assertEquals(1, publicBefore.queuedJobs());
        assertEquals(132L, publicBefore.acceptedTotal());

        assertEquals(EnqueueResult.Rejected.NUMERIC_LIMIT,
                scheduler.enqueue(target, "rejected", "p", 1L));
        enqueueAttempts++;
        assertEquals(RefundNumericRegressionTrace.ENQUEUE_ATTEMPTS, enqueueAttempts);
        assertEquals(publicBefore, scheduler.snapshot());
        assertEquals(before, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(first));
    }

    @Test
    void failedRefundCancellationsAreAtomicNoOps() throws ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 2,
                CancellationAccounting.REFUND_CANCELLED_COST);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle running = accepted(scheduler.enqueue(flow, "running", "p", 1L));
        scheduler.dispatchUpTo(1);
        JobHandle queued = accepted(scheduler.enqueue(flow, "queued", "p", 1L));
        SfqdScheduler<String, String, String> foreign = scheduler(1, 1, 1,
                CancellationAccounting.REFUND_CANCELLED_COST);
        FlowHandle foreignFlow = registered(foreign.registerFlow("foreign", 1L));
        JobHandle foreignJob = accepted(foreign.enqueue(foreignFlow, "foreign", "p", 1L));

        Object beforeRunning = SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler);
        assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, scheduler.cancel(running));
        assertEquals(beforeRunning, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));
        assertEquals(CancelResult.NOT_LIVE, scheduler.cancel(foreignJob));
        assertEquals(beforeRunning, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(queued));
        Object afterSuccess = SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler);
        assertEquals(CancelResult.NOT_LIVE, scheduler.cancel(queued));
        assertEquals(afterSuccess, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));
    }

    private static SfqdScheduler<String, String, String> scheduler(
            int depth, int flows, int jobs, CancellationAccounting policy) {
        return new SfqdScheduler<>(new SchedulerConfig(depth, flows, jobs, policy));
    }

    private static List<String> ids(List<Dispatch<String, String, String>> dispatches) {
        return dispatches.stream().map(Dispatch::jobId).toList();
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }
}
