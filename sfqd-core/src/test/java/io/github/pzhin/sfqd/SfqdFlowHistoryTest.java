package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SfqdFlowHistoryTest {
    @Test
    void cancelledCostDelaysLaterWorkUntilGlobalIdle() {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 2, 3));
        FlowHandle charged = registered(scheduler.registerFlow("charged", 1L));
        FlowHandle competing = registered(scheduler.registerFlow("competing", 1L));
        JobHandle expensive = accepted(scheduler.enqueue(charged, "cancelled", new Object(), 5L));
        JobHandle competingNext = accepted(scheduler.enqueue(competing, "competing-0", new Object(), 1L));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(expensive));
        JobHandle chargedNext = accepted(scheduler.enqueue(charged, "charged-next", new Object(), 1L));

        for (int index = 0; index < 5; index++) {
            Dispatch<String, String, Object> selected = scheduler.capacityAvailable(1).getFirst();
            assertEquals(competingNext, selected.jobHandle());
            assertEquals("competing-" + index, selected.jobId());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(selected.jobHandle()));
            if (index < 4) {
                competingNext = accepted(
                        scheduler.enqueue(competing, "competing-" + (index + 1), new Object(), 1L));
            }
        }

        assertEquals(chargedNext, scheduler.capacityAvailable(1).getFirst().jobHandle());
    }

    @Test
    void closesInactiveFlowAfterVirtualTimeRepaysItsFinishTagDebt() {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 2, 12));
        FlowHandle historical = registered(scheduler.registerFlow("historical", 1L));
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        JobHandle historicalJob = accepted(
                scheduler.enqueue(historical, "historical-job", new Object(), 10L));
        List<JobHandle> anchorJobs = new ArrayList<>();
        for (int index = 0; index <= 10; index++) {
            anchorJobs.add(accepted(
                    scheduler.enqueue(anchor, "anchor-" + index, new Object(), 1L)));
        }
        assertEquals(historicalJob, scheduler.capacityAvailable(1).getFirst().jobHandle());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(historicalJob));

        assertEquals(CloseFlowResult.FAIRNESS_DEBT_ACTIVE, scheduler.closeFlow(historical));
        for (int index = 0; index < 10; index++) {
            Dispatch<String, String, Object> anchorJob = scheduler.capacityAvailable(1).getFirst();
            assertEquals(anchorJobs.get(index), anchorJob.jobHandle());
            assertEquals("anchor-" + index, anchorJob.jobId());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(anchorJob.jobHandle()));
        }
        Dispatch<String, String, Object> frontier = scheduler.capacityAvailable(1).getFirst();
        assertEquals(anchorJobs.get(10), frontier.jobHandle());
        assertEquals("anchor-10", frontier.jobId());

        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(historical));
        FlowHandle reweighted = registered(scheduler.registerFlow("historical", 100L));
        assertEquals(2, scheduler.snapshot().registeredFlows());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(reweighted));
    }

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
