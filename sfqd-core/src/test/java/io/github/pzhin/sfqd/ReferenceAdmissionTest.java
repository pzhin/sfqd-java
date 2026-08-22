package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class ReferenceAdmissionTest {
    @Test
    void registerAndCloseFollowIdentityCapacityAndBusyPeriodRules() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 3);
        FlowHandle first = registered(model.registerFlow("first", 1L));
        assertEquals(RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID, model.registerFlow("first", 2L));
        FlowHandle second = registered(model.registerFlow("second", 2L));
        assertEquals(RegisterFlowResult.Rejected.FLOW_LIMIT, model.registerFlow("third", 1L));

        JobHandle job = accepted(model.enqueue(first, "job", new Object(), 1L));
        assertEquals(CloseFlowResult.FLOW_ACTIVE, model.closeFlow(first));
        assertEquals(CloseFlowResult.BUSY_PERIOD_ACTIVE, model.closeFlow(second));
        assertEquals(1, model.snapshot().activeFlows());
        assertEquals(1, model.snapshot().backloggedFlows());
        assertSame(job, model.queuedHandles().getFirst());

        ReferenceScheduler<String, String, Object> foreign = model(1, 1, 1);
        FlowHandle foreignHandle = registered(foreign.registerFlow("foreign", 1L));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, model.closeFlow(foreignHandle));
        assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                model.enqueue(foreignHandle, "foreign-job", new Object(), 1L));
    }

    @Test
    void enqueueCalculatesExactTagsAndAppliesAdmissionChecksInOrder() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 2);
        FlowHandle flow = registered(model.registerFlow("flow", 3L));
        Object payload = new Object();
        JobHandle first = accepted(model.enqueue(flow, "first", payload, 2L));
        JobHandle second = accepted(model.enqueue(flow, "second", new Object(), 1L));

        assertEquals(ExactRational.ZERO, model.startTag(first));
        assertEquals(ExactRational.of(2L, 3L), model.finishTag(first));
        assertEquals(ExactRational.of(2L, 3L), model.startTag(second));
        assertEquals(ExactRational.ONE, model.finishTag(second));
        assertEquals(EnqueueResult.Rejected.DUPLICATE_LIVE_ID,
                model.enqueue(flow, "first", new Object(), 1L));
        assertEquals(EnqueueResult.Rejected.LIVE_LIMIT,
                model.enqueue(flow, "third", new Object(), 1L));
        assertEquals(2L, model.snapshot().acceptedTotal());
        ReferenceScheduler.QueuedState<String, String, Object> state = model.queuedState(first);
        assertSame(first, state.handle());
        assertEquals("first", state.jobId());
        assertSame(flow, state.flowHandle());
        assertSame(payload, state.payload());
        assertEquals(2L, state.cost());
        assertEquals(ExactRational.ZERO, state.start());
        assertEquals(ExactRational.of(2L, 3L), state.finish());
        assertEquals(1L, state.sequence());
    }

    @Test
    void priorityIsExactStartTagThenAdmissionSequenceAcrossFlows() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 3);
        FlowHandle firstFlow = registered(model.registerFlow("first-flow", 1L));
        FlowHandle secondFlow = registered(model.registerFlow("second-flow", 1L));
        JobHandle first = accepted(model.enqueue(firstFlow, "first", new Object(), 1L));
        JobHandle laterTag = accepted(model.enqueue(firstFlow, "later-tag", new Object(), 1L));
        JobHandle tiedTag = accepted(model.enqueue(secondFlow, "tied-tag", new Object(), 1L));

        assertEquals(java.util.List.of(first, tiedTag, laterTag), model.queuedHandles());
    }

    @Test
    void invalidArgumentsAndExhaustedSequencesNeverMutateState() {
        ReferenceScheduler<String, String, Object> model = model(1, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> model.registerFlow("flow", 0L));
        FlowHandle flow = registered(model.registerFlow("flow", 1L));
        SchedulerSnapshot before = model.snapshot();
        assertThrows(IllegalArgumentException.class, () -> model.enqueue(flow, "job", new Object(), 0L));
        assertEquals(before, model.snapshot());

        ReferenceScheduler<String, String, Object> exhaustedFlow =
                ReferenceScheduler.withSequences(new SchedulerConfig(1, 1, 1), 0L, Long.MAX_VALUE);
        assertEquals(RegisterFlowResult.Rejected.FLOW_SEQUENCE_EXHAUSTED,
                exhaustedFlow.registerFlow("flow", 1L));
        ReferenceScheduler<String, String, Object> exhaustedJob =
                ReferenceScheduler.withSequences(new SchedulerConfig(1, 1, 1), Long.MAX_VALUE, 0L);
        FlowHandle exhaustedJobFlow = registered(exhaustedJob.registerFlow("flow", 1L));
        assertEquals(EnqueueResult.Rejected.SEQUENCE_EXHAUSTED,
                exhaustedJob.enqueue(exhaustedJobFlow, "job", new Object(), 1L));
        assertEquals(0L, exhaustedJob.snapshot().acceptedTotal());
    }

    @Test
    void closedAndReregisteredFlowGetsANewCapability() {
        ReferenceScheduler<String, String, Object> model = model(1, 1, 1);
        FlowHandle oldHandle = registered(model.registerFlow("flow", 1L));
        assertEquals(CloseFlowResult.CLOSED, model.closeFlow(oldHandle));
        FlowHandle newHandle = registered(model.registerFlow("flow", 2L));

        assertNotEquals(oldHandle, newHandle);
        assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                model.enqueue(oldHandle, "stale", new Object(), 1L));
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
