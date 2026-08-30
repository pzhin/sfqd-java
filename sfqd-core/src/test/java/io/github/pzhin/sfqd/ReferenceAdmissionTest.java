package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

final class ReferenceAdmissionTest {
    @Test
    void registerAndCloseFollowIdentityCapacityAndFairnessDebtRules() {
        ReferenceScheduler<String, String, Object> model = model(1, 2, 3);
        FlowHandle first = registered(model.registerFlow("first", 1L));
        assertEquals(RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID, model.registerFlow("first", 2L));
        FlowHandle second = registered(model.registerFlow("second", 2L));
        assertEquals(RegisterFlowResult.Rejected.FLOW_LIMIT, model.registerFlow("third", 1L));

        JobHandle job = accepted(model.enqueue(first, "job", new Object(), 1L));
        assertEquals(CloseFlowResult.FLOW_ACTIVE, model.closeFlow(first));
        assertEquals(CloseFlowResult.CLOSED, model.closeFlow(second));
        registered(model.registerFlow("third", 1L));
        assertEquals(1, model.snapshot().activeFlows());
        assertEquals(1, model.snapshot().backloggedFlows());
        assertSame(job, model.queuedHandles().get(0));

        ReferenceScheduler<String, String, Object> foreign = model(1, 1, 1);
        FlowHandle foreignHandle = registered(foreign.registerFlow("foreign", 1L));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, model.closeFlow(foreignHandle));
        assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                model.enqueue(foreignHandle, "foreign-job", new Object(), 1L));
    }

    @Test
    void registrationEnforcesConfiguredWeightDomainBeforeOtherOperationalLimits() {
        SchedulerConfig config = new SchedulerConfig(
                1, 1, 1, CancellationAccounting.CHARGE_RESERVED_COST, WeightDomain.divisorsOf(8L));
        ReferenceScheduler<String, String, Object> model = new ReferenceScheduler<>(config);

        assertEquals(RegisterFlowResult.Rejected.WEIGHT_OUTSIDE_DOMAIN, model.registerFlow("outside", 3L));
        FlowHandle flow = registered(model.registerFlow("inside", 4L));
        assertEquals(RegisterFlowResult.Rejected.WEIGHT_OUTSIDE_DOMAIN, model.registerFlow("inside", 3L));
        assertEquals(RegisterFlowResult.Rejected.FLOW_LIMIT, model.registerFlow("full", 2L));
        assertEquals(1, model.snapshot().registeredFlows());
        assertEquals(CloseFlowResult.CLOSED, model.closeFlow(flow));
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

    @Test
    void reachableRefundClosureTraceRejectsProspectiveCancellationOverflowWithoutMutation() {
        ReferenceScheduler<String, String, Object> model =
                new ReferenceScheduler<>(RefundNumericRegressionTrace.config());
        FlowHandle anchor = registered(model.registerFlow("anchor", 1L));
        JobHandle anchorJob = accepted(model.enqueue(anchor, "anchor", new Object(), 1L));
        assertSame(anchorJob, model.dispatchUpTo(1).get(0).jobHandle());
        int enqueueAttempts = 1;

        int builderId = 0;
        for (long weight : RefundNumericRegressionTrace.builderWeights()) {
            FlowHandle builder = registered(model.registerFlow("builder-" + builderId, weight));
            JobHandle firstBuilder = accepted(model.enqueue(
                    builder, "builder-" + builderId + "-first", new Object(), 1L));
            JobHandle secondBuilder = accepted(model.enqueue(
                    builder, "builder-" + builderId + "-second", new Object(), 1L));
            enqueueAttempts += 2;
            assertSame(firstBuilder, model.dispatchUpTo(1).get(0).jobHandle());
            assertEquals(CompletionResult.COMPLETED, model.complete(firstBuilder));
            assertSame(secondBuilder, model.dispatchUpTo(1).get(0).jobHandle());
            assertEquals(CompletionResult.COMPLETED, model.complete(secondBuilder));
            builderId++;
        }

        FlowHandle target = registered(model.registerFlow("target", 6L));
        JobHandle first = accepted(model.enqueue(target, "first", new Object(), 3L));
        enqueueAttempts++;
        SchedulerSnapshot before = model.snapshot();
        List<JobHandle> queuedBefore = model.queuedHandles();
        assertEquals(1, before.runningJobs());
        assertEquals(1, before.queuedJobs());
        assertEquals(132L, before.acceptedTotal());

        assertEquals(EnqueueResult.Rejected.NUMERIC_LIMIT,
                model.enqueue(target, "rejected", new Object(), 1L));
        enqueueAttempts++;
        assertEquals(RefundNumericRegressionTrace.ENQUEUE_ATTEMPTS, enqueueAttempts);
        assertEquals(before, model.snapshot());
        assertEquals(queuedBefore, model.queuedHandles());
        assertEquals(List.of(first), queuedBefore);
        assertEquals(CancelResult.CANCELLED, model.cancel(first));
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
