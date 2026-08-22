package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

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
        assertSame(job, model.capacityAvailable(1).getFirst().jobHandle());

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
                model.capacityAvailable(2).stream().map(Dispatch::jobHandle).toList());
        assertEquals(CompletionResult.COMPLETED, model.complete(warmup));

        JobHandle firstCompetitor = accepted(model.enqueue(competitorFlow, "competitor-0", new Object(), 1L));
        JobHandle victim = accepted(model.enqueue(victimFlow, "victim", new Object(), 1L));
        assertSame(firstCompetitor, model.capacityAvailable(1).getFirst().jobHandle());
        assertEquals(CompletionResult.COMPLETED, model.complete(firstCompetitor));
        for (int index = 1; index < 5; index++) {
            JobHandle competitor = accepted(
                    model.enqueue(competitorFlow, "competitor-" + index, new Object(), 1L));
            assertSame(competitor, model.capacityAvailable(1).getFirst().jobHandle());
            assertEquals(CompletionResult.COMPLETED, model.complete(competitor));
        }
        accepted(model.enqueue(competitorFlow, "competitor-5", new Object(), 1L));
        assertSame(victim, model.capacityAvailable(1).getFirst().jobHandle());
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
        model.capacityAvailable(1);
        assertEquals(CompletionResult.COMPLETED, model.complete(job));

        SchedulerSnapshot snapshot = model.snapshot();
        assertEquals(1L, snapshot.acceptedTotal());
        assertEquals(1L, snapshot.dispatchedTotal());
        assertEquals(1L, snapshot.completedTotal());
        assertEquals(0, snapshot.queuedJobs());
        assertEquals(0, snapshot.runningJobs());
    }

    private static ReferenceScheduler<String, String, Object> model(int depth, int maxFlows, int maxJobs) {
        return new ReferenceScheduler<>(new SchedulerConfig(depth, maxFlows, maxJobs));
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }
}
