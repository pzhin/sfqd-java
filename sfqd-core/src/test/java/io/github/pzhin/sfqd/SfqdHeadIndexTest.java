package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class SfqdHeadIndexTest {
    @Test
    void cancellingHeadPromotesNextOnlyAfterOtherLowerHead() {
        SfqdScheduler<String, String, String> scheduler = scheduler();
        FlowHandle flowA = registered(scheduler.registerFlow("a", 1L));
        FlowHandle flowB = registered(scheduler.registerFlow("b", 1L));
        JobHandle a1 = accepted(scheduler.enqueue(flowA, "a1", "p", 100L));
        scheduler.enqueue(flowA, "a2", "p", 1L);
        scheduler.enqueue(flowB, "b1", "p", 1L);

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(a1));
        assertEquals(List.of("b1", "a2"), ids(scheduler.capacityAvailable(3)));
    }

    @Test
    void dispatchingHeadPromotesNextOnlyAfterOtherEqualHead() {
        SfqdScheduler<String, String, String> scheduler = scheduler();
        FlowHandle flowA = registered(scheduler.registerFlow("a", 1L));
        FlowHandle flowB = registered(scheduler.registerFlow("b", 1L));
        scheduler.enqueue(flowA, "a1", "p", 100L);
        scheduler.enqueue(flowA, "a2", "p", 1L);
        scheduler.enqueue(flowB, "b1", "p", 1L);

        assertEquals(List.of("a1"), ids(scheduler.capacityAvailable(1)));
        assertEquals(List.of("b1", "a2"), ids(scheduler.capacityAvailable(2)));
    }

    @Test
    void cancellingMiddleAndTailDoesNotDisturbIndexedHead() {
        SfqdScheduler<String, String, String> scheduler = scheduler();
        FlowHandle flowA = registered(scheduler.registerFlow("a", 1L));
        FlowHandle flowB = registered(scheduler.registerFlow("b", 1L));
        scheduler.enqueue(flowA, "a1", "p", 1L);
        JobHandle middle = accepted(scheduler.enqueue(flowA, "a2", "p", 1L));
        JobHandle tail = accepted(scheduler.enqueue(flowA, "a3", "p", 1L));
        scheduler.enqueue(flowB, "b1", "p", 1L);

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(middle));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(tail));
        assertEquals(List.of("a1", "b1"), ids(scheduler.capacityAvailable(3)));
    }

    @Test
    void cancelledTagDebtPersistsUntilGlobalIdleAndThenResets() {
        SfqdScheduler<String, String, String> busy = scheduler();
        FlowHandle anchorFlow = registered(busy.registerFlow("anchor", 1L));
        FlowHandle chargedFlow = registered(busy.registerFlow("charged", 1L));
        FlowHandle competingFlow = registered(busy.registerFlow("competing", 1L));
        busy.enqueue(anchorFlow, "anchor", "p", 1L);
        busy.capacityAvailable(1);
        JobHandle cancelled = accepted(busy.enqueue(chargedFlow, "cancelled", "p", 10L));
        assertEquals(CancelResult.CANCELLED, busy.cancel(cancelled));
        busy.enqueue(chargedFlow, "charged", "p", 1L);
        busy.enqueue(competingFlow, "competing", "p", 1L);
        assertEquals(List.of("competing"), ids(busy.capacityAvailable(1)));

        SfqdScheduler<String, String, String> idle = scheduler();
        FlowHandle resetFlow = registered(idle.registerFlow("reset", 1L));
        FlowHandle otherFlow = registered(idle.registerFlow("other", 1L));
        JobHandle reset = accepted(idle.enqueue(resetFlow, "reset-debt", "p", 10L));
        assertEquals(CancelResult.CANCELLED, idle.cancel(reset));
        idle.enqueue(resetFlow, "after-reset", "p", 1L);
        idle.enqueue(otherFlow, "other", "p", 1L);
        assertEquals(List.of("after-reset", "other"), ids(idle.capacityAvailable(2)));
    }

    private static SfqdScheduler<String, String, String> scheduler() {
        return new SfqdScheduler<>(new SchedulerConfig(3, 4, 12));
    }

    private static List<String> ids(List<Dispatch<String, String, String>> dispatches) {
        return dispatches.stream().map(Dispatch::jobId).toList();
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }
}
