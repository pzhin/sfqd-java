package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

final class ReferenceDispatchTest {
    @Test
    void depthOneIsSfqWithStartTagOrderAndAdmissionSequenceTies() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 4);
        FlowHandle firstFlow = registered(model.registerFlow("first-flow", 1L));
        FlowHandle secondFlow = registered(model.registerFlow("second-flow", 2L));
        Object firstPayload = new Object();
        JobHandle first = accepted(model.enqueue(firstFlow, "first", firstPayload, 1L));
        JobHandle second = accepted(model.enqueue(firstFlow, "second", new Object(), 1L));
        JobHandle third = accepted(model.enqueue(secondFlow, "third", new Object(), 2L));

        Dispatch<String, String, Object> firstDispatch = model.capacityAvailable(1).getFirst();
        assertSame(first, firstDispatch.jobHandle());
        assertEquals("first", firstDispatch.jobId());
        assertEquals("first-flow", firstDispatch.flowId());
        assertSame(firstPayload, firstDispatch.payload());
        assertEquals(1L, firstDispatch.cost());
        assertEquals(ExactRational.ZERO, model.virtualTime());
        assertEquals(List.of(), model.capacityAvailable(1));
        assertEquals(CompletionResult.NOT_DISPATCHED, model.complete(second));
        assertEquals(CompletionResult.COMPLETED, model.complete(first));
        assertSame(third, model.capacityAvailable(1).getFirst().jobHandle());
        assertEquals(CompletionResult.COMPLETED, model.complete(third));
        assertSame(second, model.capacityAvailable(1).getFirst().jobHandle());
        assertEquals(ExactRational.ONE, model.virtualTime());
    }

    @Test
    void depthGreaterThanOneOrdersOneBatchAcrossFlowsByStartTagThenSequence() {
        ReferenceScheduler<String, String, Object> model = model(3, 2, 3);
        FlowHandle firstFlow = registered(model.registerFlow("first-flow", 1L));
        FlowHandle secondFlow = registered(model.registerFlow("second-flow", 1L));
        JobHandle first = accepted(model.enqueue(firstFlow, "first", new Object(), 1L));
        JobHandle laterStart = accepted(model.enqueue(firstFlow, "later-start", new Object(), 1L));
        JobHandle tiedStart = accepted(model.enqueue(secondFlow, "tied-start", new Object(), 2L));

        List<Dispatch<String, String, Object>> batch = model.capacityAvailable(3);
        assertEquals(List.of(first, tiedStart, laterStart), batch.stream().map(Dispatch::jobHandle).toList());
        assertEquals(List.of("first", "tied-start", "later-start"),
                batch.stream().map(Dispatch::jobId).toList());
        assertEquals(List.of("first-flow", "second-flow", "first-flow"),
                batch.stream().map(Dispatch::flowId).toList());
        assertEquals(List.of(1L, 2L, 1L), batch.stream().map(Dispatch::cost).toList());
        ReferenceScheduler.RunningState<String> state = model.runningState(tiedStart);
        assertSame(tiedStart, state.handle());
        assertEquals("tied-start", state.jobId());
        assertSame(secondFlow, state.flowHandle());
        assertEquals(2L, state.cost());
    }

    @Test
    void partialBatchLeavesSlotsAndNonFinalOutOfOrderCompletionDoesNotAdvanceVirtualTime() {
        ReferenceScheduler<String, String, Object> model = model(4, 1, 4);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle first = accepted(model.enqueue(flow, "first", new Object(), 1L));
        JobHandle second = accepted(model.enqueue(flow, "second", new Object(), 1L));
        JobHandle third = accepted(model.enqueue(flow, "third", new Object(), 1L));

        assertEquals(List.of(first, second),
                model.capacityAvailable(2).stream().map(Dispatch::jobHandle).toList());
        assertEquals(2, model.snapshot().freeSlots());
        assertEquals(ExactRational.ONE, model.virtualTime());
        assertEquals(CompletionResult.COMPLETED, model.complete(second));
        assertEquals(ExactRational.ONE, model.virtualTime());
        assertEquals(CompletionResult.NOT_LIVE, model.complete(second));
        assertEquals(1, model.snapshot().runningJobs());
        assertSame(third, model.capacityAvailable(1).getFirst().jobHandle());
    }

    @Test
    void depthGreaterThanOneFillsAllSlotsAndRequiresCompletionToReuseOne() {
        ReferenceScheduler<String, String, Object> model = model(3, 1, 4);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle first = accepted(model.enqueue(flow, "first", new Object(), 1L));
        JobHandle second = accepted(model.enqueue(flow, "second", new Object(), 1L));
        JobHandle third = accepted(model.enqueue(flow, "third", new Object(), 1L));
        JobHandle fourth = accepted(model.enqueue(flow, "fourth", new Object(), 1L));

        List<Dispatch<String, String, Object>> batch = model.capacityAvailable(3);
        assertEquals(List.of(first, second, third), batch.stream().map(Dispatch::jobHandle).toList());
        assertThrows(UnsupportedOperationException.class, () -> batch.clear());
        assertEquals(3, model.snapshot().runningJobs());
        assertEquals(0, model.snapshot().freeSlots());
        assertEquals(List.of(), model.capacityAvailable(3));
        assertEquals(CompletionResult.COMPLETED, model.complete(second));
        assertSame(fourth, model.capacityAvailable(3).getFirst().jobHandle());
        assertEquals(3, model.snapshot().runningJobs());
        assertEquals(4L, model.snapshot().dispatchedTotal());
    }

    @Test
    void batchSizeAndCompletionOutcomesAreExplicitNoOpsWhenInapplicable() {
        ReferenceScheduler<String, String, Object> model = model(2, 1, 2);
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        JobHandle queued = accepted(model.enqueue(flow, "job", new Object(), 1L));

        assertThrows(IllegalArgumentException.class, () -> model.capacityAvailable(-1));
        assertThrows(IllegalArgumentException.class, () -> model.capacityAvailable(3));
        assertEquals(List.of(), model.capacityAvailable(0));
        assertEquals(CompletionResult.NOT_DISPATCHED, model.complete(queued));
        JobHandle unknown = new JobHandle(new OwnerToken(), 1L);
        assertEquals(CompletionResult.NOT_LIVE, model.complete(unknown));
        assertEquals(1, model.snapshot().queuedJobs());
        assertEquals(0L, model.snapshot().completedTotal());
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
