package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.junit.jupiter.api.Test;

final class ReferenceCancellationTest {
    @Test
    void cancellationIsQueuedOnlyBoundedAndAllowsJobIdReuseWithoutAba() {
        ReferenceScheduler<String, String, Object> model = model(1, 1, 2);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle oldHandle = accepted(model.enqueue(flow, "job", new Object(), 1L));

        assertEquals(CancelResult.CANCELLED, model.cancel(oldHandle));
        assertEquals(CancelResult.NOT_LIVE, model.cancel(oldHandle));
        JobHandle newHandle = accepted(model.enqueue(flow, "job", new Object(), 1L));
        assertEquals(CancelResult.NOT_LIVE, model.cancel(oldHandle));
        assertEquals(CancelResult.CANCELLED, model.cancel(newHandle));
        assertEquals(2L, model.snapshot().acceptedTotal());
        assertEquals(2L, model.snapshot().cancelledTotal());
        assertEquals(0, model.snapshot().queuedJobs());
    }

    @Test
    void dispatchMakesCancellationTooLateUntilTerminalMetadataIsRemoved() {
        ReferenceScheduler<String, String, Object> model = model(1, 1, 1);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle job = accepted(model.enqueue(flow, "job", new Object(), 1L));
        assertSame(job, model.dispatchUpTo(1).get(0).jobHandle());

        assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, model.cancel(job));
        assertEquals(CompletionResult.COMPLETED, model.complete(job));
        assertEquals(CancelResult.NOT_LIVE, model.cancel(job));
    }

    @Test
    void cancellationChargePersistsWithinBusyPeriodButGlobalIdleResetsAllHistories() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 3);
        FlowHandle chargedFlow = registered(model.registerFlow("charged", 1L));
        FlowHandle anchorFlow = registered(model.registerFlow("anchor", 1L));
        JobHandle anchor = accepted(model.enqueue(anchorFlow, "anchor", new Object(), 1L));
        JobHandle charged = accepted(model.enqueue(chargedFlow, "charged", new Object(), 5L));

        assertEquals(CancelResult.CANCELLED, model.cancel(charged));
        JobHandle debt = accepted(model.enqueue(chargedFlow, "debt", new Object(), 1L));
        assertEquals(ExactRational.of(5L, 1L), model.startTag(debt));
        assertEquals(CancelResult.CANCELLED, model.cancel(debt));
        assertEquals(CancelResult.CANCELLED, model.cancel(anchor));

        JobHandle reset = accepted(model.enqueue(chargedFlow, "reset", new Object(), 1L));
        assertEquals(ExactRational.ZERO, model.startTag(reset));
        assertEquals(ExactRational.ZERO, model.virtualTime());
    }

    @Test
    void dormantCompetitorCannotResetFinishHistoryAndStarveFixedVictim() {
        ReferenceScheduler<String, String, Object> model = model(2, 3, 12);
        FlowHandle anchorFlow = registered(model.registerFlow("anchor-flow", 1L));
        FlowHandle victimFlow = registered(model.registerFlow("victim-flow", 1L));
        FlowHandle competitorFlow = registered(model.registerFlow("competitor-flow", 1L));
        JobHandle anchor = accepted(model.enqueue(anchorFlow, "anchor", new Object(), 100L));
        JobHandle warmup = accepted(model.enqueue(victimFlow, "warmup", new Object(), 5L));
        assertEquals(java.util.List.of(anchor, warmup),
                model.dispatchUpTo(2).stream().map(Dispatch::jobHandle).toList());
        assertEquals(CompletionResult.COMPLETED, model.complete(warmup));

        JobHandle firstCompetitor = accepted(model.enqueue(competitorFlow, "competitor-0", new Object(), 1L));
        JobHandle victim = accepted(model.enqueue(victimFlow, "victim", new Object(), 1L));
        assertSame(firstCompetitor, model.dispatchUpTo(1).get(0).jobHandle());
        assertEquals(CompletionResult.COMPLETED, model.complete(firstCompetitor));
        for (int index = 1; index < 5; index++) {
            JobHandle competitor = accepted(
                    model.enqueue(competitorFlow, "competitor-" + index, new Object(), 1L));
            assertSame(competitor, model.dispatchUpTo(1).get(0).jobHandle());
            assertEquals(CompletionResult.COMPLETED, model.complete(competitor));
        }
        accepted(model.enqueue(competitorFlow, "competitor-5", new Object(), 1L));
        assertSame(victim, model.dispatchUpTo(1).get(0).jobHandle());
    }

    @Test
    void cancellingFlowsLastJobKeepsItsFairnessDebtUntilVirtualTimeCatchesUp() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 2);
        FlowHandle firstFlow = registered(model.registerFlow("first-flow", 1L));
        FlowHandle secondFlow = registered(model.registerFlow("second-flow", 1L));
        JobHandle firstJob = accepted(model.enqueue(firstFlow, "first-job", new Object(), 1L));
        JobHandle secondJob = accepted(model.enqueue(secondFlow, "second-job", new Object(), 1L));

        assertEquals(CancelResult.CANCELLED, model.cancel(firstJob));
        assertEquals(1, model.snapshot().activeFlows());
        assertEquals(1, model.snapshot().backloggedFlows());
        assertEquals(CloseFlowResult.FAIRNESS_DEBT_ACTIVE, model.closeFlow(firstFlow));
        assertEquals(CancelResult.CANCELLED, model.cancel(secondJob));
        assertEquals(0, model.snapshot().activeFlows());
        assertEquals(CloseFlowResult.CLOSED, model.closeFlow(firstFlow));
    }

    @Test
    void completionCounterPreservesBothConservationEquations() {
        ReferenceScheduler<String, String, Object> model = model(1, 1, 1);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle job = accepted(model.enqueue(flow, "job", new Object(), 1L));
        model.dispatchUpTo(1);
        assertEquals(CompletionResult.COMPLETED, model.complete(job));

        SchedulerSnapshot snapshot = model.snapshot();
        assertEquals(1L, snapshot.acceptedTotal());
        assertEquals(1L, snapshot.dispatchedTotal());
        assertEquals(1L, snapshot.completedTotal());
        assertEquals(0, snapshot.queuedJobs());
        assertEquals(0, snapshot.runningJobs());
    }

    @Test
    void refundRecomputesHeadSuffixAndCanChangeGlobalOrderWithStableTies() {
        ReferenceScheduler<String, String, Object> refund = refundModel(3, 2, 5);
        FlowHandle refundA = registered(refund.registerFlow("a", 2L));
        FlowHandle refundB = registered(refund.registerFlow("b", 1L));
        JobHandle cancelled = accepted(refund.enqueue(refundA, "cancelled", new Object(), 4L));
        JobHandle successor = accepted(refund.enqueue(refundA, "successor", new Object(), 2L));
        JobHandle secondSuccessor = accepted(refund.enqueue(refundA, "second-successor", new Object(), 2L));
        JobHandle competitor = accepted(refund.enqueue(refundB, "competitor", new Object(), 1L));

        assertEquals(CancelResult.CANCELLED, refund.cancel(cancelled));
        assertEquals(ExactRational.ZERO, refund.startTag(successor));
        assertEquals(ExactRational.ONE, refund.finishTag(successor));
        assertEquals(ExactRational.ONE, refund.startTag(secondSuccessor));
        assertEquals(ExactRational.of(2L, 1L), refund.finishTag(secondSuccessor));
        assertEquals(List.of(successor, competitor, secondSuccessor), refund.queuedHandles());

        ReferenceScheduler<String, String, Object> charged = model(3, 2, 4);
        FlowHandle chargedA = registered(charged.registerFlow("a", 2L));
        FlowHandle chargedB = registered(charged.registerFlow("b", 1L));
        JobHandle chargedCancelled = accepted(charged.enqueue(chargedA, "cancelled", new Object(), 4L));
        JobHandle chargedSuccessor = accepted(charged.enqueue(chargedA, "successor", new Object(), 2L));
        JobHandle chargedCompetitor = accepted(charged.enqueue(chargedB, "competitor", new Object(), 1L));

        assertEquals(CancelResult.CANCELLED, charged.cancel(chargedCancelled));
        assertEquals(ExactRational.of(2L, 1L), charged.startTag(chargedSuccessor));
        assertEquals(List.of(chargedCompetitor, chargedSuccessor), charged.queuedHandles());
    }

    @Test
    void refundOfMiddleAndTailPreservesFractionalOrderAndOtherFlows() {
        ReferenceScheduler<String, String, Object> model = refundModel(3, 2, 6);
        FlowHandle target = registered(model.registerFlow("target", 6L));
        FlowHandle other = registered(model.registerFlow("other", 5L));
        JobHandle first = accepted(model.enqueue(target, "first", new Object(), 1L));
        JobHandle middle = accepted(model.enqueue(target, "middle", new Object(), 2L));
        JobHandle tail = accepted(model.enqueue(target, "tail", new Object(), 3L));
        JobHandle untouched = accepted(model.enqueue(other, "untouched", new Object(), 2L));
        ExactRational otherStart = model.startTag(untouched);
        ExactRational otherFinish = model.finishTag(untouched);

        assertEquals(CancelResult.CANCELLED, model.cancel(middle));
        assertEquals(ExactRational.of(1L, 6L), model.startTag(tail));
        assertEquals(ExactRational.of(2L, 3L), model.finishTag(tail));
        assertEquals(CancelResult.CANCELLED, model.cancel(tail));
        JobHandle next = accepted(model.enqueue(target, "next", new Object(), 1L));

        assertEquals(ExactRational.of(1L, 6L), model.startTag(next));
        assertEquals(otherStart, model.startTag(untouched));
        assertEquals(otherFinish, model.finishTag(untouched));
        assertEquals(List.of(first, next), model.queuedHandles().stream()
                .filter(handle -> handle.equals(first) || handle.equals(next)).toList());
        assertEquals(new FlowSnapshot(2, 0, java.math.BigInteger.valueOf(7L),
                java.math.BigInteger.ZERO, java.math.BigInteger.valueOf(5L), java.math.BigInteger.ZERO),
                model.snapshot(target).orElseThrow());
        assertEquals(5L, model.snapshot().acceptedTotal());
        assertEquals(2L, model.snapshot().cancelledTotal());
    }

    @Test
    void refundKeepsDispatchedDebtAndGlobalIdleStillResetsHistory() {
        ReferenceScheduler<String, String, Object> model = refundModel(2, 2, 5);
        FlowHandle target = registered(model.registerFlow("target", 2L));
        FlowHandle progress = registered(model.registerFlow("progress", 1L));
        JobHandle running = accepted(model.enqueue(target, "running", new Object(), 2L));
        JobHandle cancelled = accepted(model.enqueue(target, "cancelled", new Object(), 4L));
        assertSame(running, model.dispatchUpTo(1).get(0).jobHandle());

        assertEquals(CancelResult.CANCELLED, model.cancel(cancelled));
        JobHandle afterRefund = accepted(model.enqueue(target, "after-refund", new Object(), 2L));
        assertEquals(ExactRational.ONE, model.startTag(afterRefund));
        assertEquals(CancelResult.CANCELLED, model.cancel(afterRefund));
        assertEquals(CompletionResult.COMPLETED, model.complete(running));

        JobHandle reset = accepted(model.enqueue(progress, "reset", new Object(), 1L));
        assertEquals(ExactRational.ZERO, model.startTag(reset));
    }

    @Test
    void refundOfOnlyQueuedJobKeepsDispatchedDebtForCloseFlow() {
        ReferenceScheduler<String, String, Object> model = refundModel(2, 2, 4);
        FlowHandle target = registered(model.registerFlow("target", 1L));
        FlowHandle progress = registered(model.registerFlow("progress", 1L));
        JobHandle running = accepted(model.enqueue(target, "running", new Object(), 1L));
        JobHandle cancelled = accepted(model.enqueue(target, "cancelled", new Object(), 5L));
        assertSame(running, model.dispatchUpTo(1).get(0).jobHandle());
        JobHandle progressFirst = accepted(model.enqueue(progress, "progress-1", new Object(), 1L));
        JobHandle progressSecond = accepted(model.enqueue(progress, "progress-2", new Object(), 1L));

        assertEquals(CancelResult.CANCELLED, model.cancel(cancelled));
        assertEquals(CompletionResult.COMPLETED, model.complete(running));
        assertEquals(CloseFlowResult.FAIRNESS_DEBT_ACTIVE, model.closeFlow(target));
        assertSame(progressFirst, model.dispatchUpTo(1).get(0).jobHandle());
        assertEquals(CompletionResult.COMPLETED, model.complete(progressFirst));
        assertSame(progressSecond, model.dispatchUpTo(1).get(0).jobHandle());
        assertEquals(CloseFlowResult.CLOSED, model.closeFlow(target));
    }

    private static ReferenceScheduler<String, String, Object> model(int depth, int maxFlows, int maxJobs) {
        return new ReferenceScheduler<>(new SchedulerConfig(depth, maxFlows, maxJobs));
    }

    private static ReferenceScheduler<String, String, Object> refundModel(int depth, int maxFlows, int maxJobs) {
        return new ReferenceScheduler<>(new SchedulerConfig(
                depth, maxFlows, maxJobs, CancellationAccounting.REFUND_CANCELLED_COST));
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }
}
