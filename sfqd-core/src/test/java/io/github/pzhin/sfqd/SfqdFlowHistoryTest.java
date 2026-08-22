package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class SfqdFlowHistoryTest {
    @Test
    void activeNonBackloggedFlowRetainsFinishHistory() {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 2, 4));
        FlowHandle historical = registered(scheduler.registerFlow("historical", 1L));
        FlowHandle fresh = registered(scheduler.registerFlow("fresh", 1L));
        Object runningPayload = new Object();
        Object historicalPayload = new Object();
        Object freshPayload = new Object();
        JobHandle runningHandle = accepted(
                scheduler.enqueue(historical, "historical-running", runningPayload, 10L));
        Dispatch<String, String, Object> running = scheduler.capacityAvailable(1).getFirst();
        assertEquals(runningHandle, running.jobHandle());
        assertSame(runningPayload, running.payload());
        JobHandle historicalNext = accepted(
                scheduler.enqueue(historical, "historical-next", historicalPayload, 1L));
        JobHandle freshJob = accepted(scheduler.enqueue(fresh, "fresh", freshPayload, 1L));

        Dispatch<String, String, Object> selected = scheduler.capacityAvailable(1).getFirst();

        assertEquals(freshJob, selected.jobHandle());
        assertEquals("fresh", selected.jobId());
        assertSame(freshPayload, selected.payload());
        assertEquals(CompletionResult.NOT_DISPATCHED, scheduler.complete(historicalNext));
        assertEquals(1, scheduler.snapshot().queuedJobs());
    }

    @Test
    void inactiveFlowRetainsFinishHistoryInsideNonemptyBusyPeriod() {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 3, 5));
        FlowHandle historical = registered(scheduler.registerFlow("historical", 1L));
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        FlowHandle fresh = registered(scheduler.registerFlow("fresh", 1L));
        JobHandle warmup = accepted(scheduler.enqueue(historical, "warmup", new Object(), 10L));
        JobHandle anchorJob = accepted(scheduler.enqueue(anchor, "anchor", new Object(), 1L));
        assertEquals(
                java.util.List.of(warmup, anchorJob),
                scheduler.capacityAvailable(2).stream().map(Dispatch::jobHandle).toList());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(warmup));
        Object historicalPayload = new Object();
        Object freshPayload = new Object();
        JobHandle historicalReturn = accepted(
                scheduler.enqueue(historical, "historical-return", historicalPayload, 1L));
        JobHandle freshJob = accepted(scheduler.enqueue(fresh, "fresh", freshPayload, 1L));

        Dispatch<String, String, Object> selected = scheduler.capacityAvailable(1).getFirst();

        assertEquals(freshJob, selected.jobHandle());
        assertEquals("fresh", selected.jobId());
        assertSame(freshPayload, selected.payload());
        assertEquals(CompletionResult.NOT_DISPATCHED, scheduler.complete(historicalReturn));
        assertEquals(1, scheduler.snapshot().queuedJobs());
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }
}
