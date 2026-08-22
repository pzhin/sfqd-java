package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class SfqdSchedulerTest {
    @Test
    void schedulesByExactStartTagAndAccountsLifecycle() {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 3, 6));
        FlowHandle light = registered(scheduler.registerFlow("light", 1L));
        FlowHandle heavy = registered(scheduler.registerFlow("heavy", 2L));
        Object lightPayload = new Object();
        Object heavyPayload = new Object();
        JobHandle lightOne = accepted(scheduler.enqueue(light, "l1", lightPayload, 4L));
        JobHandle lightTwo = accepted(scheduler.enqueue(light, "l2", new Object(), 4L));
        JobHandle heavyOne = accepted(scheduler.enqueue(heavy, "h1", heavyPayload, 4L));

        List<Dispatch<String, String, Object>> first = scheduler.capacityAvailable(2);

        assertEquals(List.of("l1", "h1"), first.stream().map(Dispatch::jobId).toList());
        assertSame(lightPayload, first.get(0).payload());
        assertSame(heavyPayload, first.get(1).payload());
        assertThrows(UnsupportedOperationException.class, () -> first.clear());
        assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, scheduler.cancel(lightOne));
        assertEquals(CompletionResult.NOT_DISPATCHED, scheduler.complete(lightTwo));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(lightOne));
        assertEquals(CompletionResult.NOT_LIVE, scheduler.complete(lightOne));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(lightTwo));
        assertEquals(CancelResult.NOT_LIVE, scheduler.cancel(lightTwo));
        assertEquals(CloseFlowResult.FLOW_ACTIVE, scheduler.closeFlow(heavy));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(heavyOne));

        assertEquals(
                new SchedulerSnapshot(2, 3, 6, 2, 0, 0, 2, 0, 0, 3L, 2L, 1L, 2L),
                scheduler.snapshot());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(light));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(light));
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(heavy));
    }

    @Test
    void enforcesIdentityCapacityAndArgumentContractsWithoutMutation() {
        SchedulerConfig config = new SchedulerConfig(1, 1, 1);
        SfqdScheduler<String, String, String> first = new SfqdScheduler<>(config);
        SfqdScheduler<String, String, String> second = new SfqdScheduler<>(config);
        FlowHandle flow = registered(first.registerFlow("flow", 1L));
        FlowHandle foreignFlow = registered(second.registerFlow("flow", 1L));
        JobHandle job = accepted(first.enqueue(flow, "job", "payload", 1L));

        assertEquals(RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID, first.registerFlow("flow", 2L));
        assertEquals(RegisterFlowResult.Rejected.FLOW_LIMIT, first.registerFlow("other", 1L));
        assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                first.enqueue(foreignFlow, "foreign", "payload", 1L));
        assertEquals(EnqueueResult.Rejected.DUPLICATE_LIVE_ID,
                first.enqueue(flow, "job", "replacement", 1L));
        assertEquals(EnqueueResult.Rejected.LIVE_LIMIT,
                first.enqueue(flow, "other", "payload", 1L));
        assertEquals(CloseFlowResult.FLOW_ACTIVE, first.closeFlow(flow));
        assertTrue(first.capacityAvailable(0).isEmpty());
        assertEquals(1, first.capacityAvailable(1).size());
        assertTrue(first.capacityAvailable(1).isEmpty());
        assertEquals(CancelResult.NOT_LIVE, first.cancel(accepted(second.enqueue(foreignFlow,
                "foreign", "payload", 1L))));
        assertEquals(CompletionResult.COMPLETED, first.complete(job));
        JobHandle reincarnation = accepted(first.enqueue(flow, "job", "again", 1L));
        assertNotSame(job, reincarnation);

        assertThrows(IllegalArgumentException.class, () -> first.registerFlow("bad", 0L));
        assertThrows(IllegalArgumentException.class, () -> first.enqueue(flow, "x", "x", 0L));
        assertThrows(IllegalArgumentException.class, () -> first.capacityAvailable(-1));
        assertThrows(IllegalArgumentException.class, () -> first.capacityAvailable(2));
    }

    @Test
    void closesUnusedFlowDuringAnotherFlowsBusyPeriod() {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 2, 4));
        FlowHandle first = registered(scheduler.registerFlow("first", 1L));
        FlowHandle dormant = registered(scheduler.registerFlow("dormant", 1L));
        JobHandle running = accepted(scheduler.enqueue(first, "running", "p", 1L));
        scheduler.capacityAvailable(1);

        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(dormant));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(running));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(dormant));
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }
}
