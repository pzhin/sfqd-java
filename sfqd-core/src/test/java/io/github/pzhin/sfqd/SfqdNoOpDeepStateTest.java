package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SfqdNoOpDeepStateTest {
    @Test
    void registerRejectionsAndInvalidArgumentsPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture limited = fixture(4, 8);
        assertNoOp(
                limited.scheduler,
                RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID,
                () -> limited.scheduler.registerFlow("flow-a", 11L));
        assertNoOp(
                limited.scheduler,
                RegisterFlowResult.Rejected.FLOW_LIMIT,
                () -> limited.scheduler.registerFlow("over-limit", 1L));

        Fixture exhausted = fixture(6, 8);
        SfqdNumericBoundaryTest.NumericProbe.setLongForDeepState(
                exhausted.scheduler, "lastFlowSequence", Long.MAX_VALUE - 1L);
        RegisterFlowResult.Registered last = assertInstanceOf(
                RegisterFlowResult.Registered.class,
                exhausted.scheduler.registerFlow("last-public-flow", 13L));
        assertEquals(CloseFlowResult.CLOSED, exhausted.scheduler.closeFlow(last.flowHandle()));
        assertNoOp(
                exhausted.scheduler,
                RegisterFlowResult.Rejected.FLOW_SEQUENCE_EXHAUSTED,
                () -> exhausted.scheduler.registerFlow("after-sequence", 1L));

        Fixture invalid = fixture(5, 8);
        assertThrowsNoOp(NullPointerException.class, invalid.scheduler,
                () -> invalid.scheduler.registerFlow(nullReference(), 1L));
        assertThrowsNoOp(IllegalArgumentException.class, invalid.scheduler,
                () -> invalid.scheduler.registerFlow("zero-weight", 0L));
        assertThrowsNoOp(IllegalArgumentException.class, invalid.scheduler,
                () -> invalid.scheduler.registerFlow("negative-weight", -1L));
    }

    @Test
    void closeRejectionsAndInvalidArgumentPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture fixture = fixture(5, 8);
        SfqdScheduler<String, String, Object> foreign =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
        FlowHandle foreignFlow = registered(foreign.registerFlow("foreign", 1L));

        assertNoOp(
                fixture.scheduler,
                CloseFlowResult.FLOW_NOT_REGISTERED,
                () -> fixture.scheduler.closeFlow(foreignFlow));
        assertNoOp(
                fixture.scheduler,
                CloseFlowResult.FLOW_ACTIVE,
                () -> fixture.scheduler.closeFlow(fixture.flowA));
        assertNoOp(
                fixture.scheduler,
                CloseFlowResult.FAIRNESS_DEBT_ACTIVE,
                () -> fixture.scheduler.closeFlow(fixture.inactiveFlow));
        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.closeFlow(nullReference()));
    }

    @Test
    void enqueueRejectionsAndInvalidArgumentsPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture fixture = fixture(5, 8);
        SfqdScheduler<String, String, Object> foreign =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
        FlowHandle foreignFlow = registered(foreign.registerFlow("foreign", 1L));

        assertNoOp(
                fixture.scheduler,
                EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                () -> fixture.scheduler.enqueue(foreignFlow, "foreign-job", new Object(), 1L));
        assertNoOp(
                fixture.scheduler,
                EnqueueResult.Rejected.DUPLICATE_LIVE_ID,
                () -> fixture.scheduler.enqueue(fixture.inactiveFlow, "a1", new Object(), 1L));

        Fixture limited = fixture(5, 6);
        assertNoOp(
                limited.scheduler,
                EnqueueResult.Rejected.LIVE_LIMIT,
                () -> limited.scheduler.enqueue(limited.inactiveFlow, "over-limit", new Object(), 1L));

        Fixture exhausted = fixture(5, 8);
        SfqdNumericBoundaryTest.NumericProbe.setLongForDeepState(
                exhausted.scheduler, "lastJobSequence", Long.MAX_VALUE - 1L);
        assertInstanceOf(
                EnqueueResult.Accepted.class,
                exhausted.scheduler.enqueue(exhausted.inactiveFlow, "last-public-job", new Object(), 1L));
        assertNoOp(
                exhausted.scheduler,
                EnqueueResult.Rejected.SEQUENCE_EXHAUSTED,
                () -> exhausted.scheduler.enqueue(
                        exhausted.inactiveFlow, "after-sequence", new Object(), 1L));

        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.enqueue(nullReference(), "null-flow", new Object(), 1L));
        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.enqueue(fixture.inactiveFlow, nullReference(), new Object(), 1L));
        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.enqueue(fixture.inactiveFlow, "null-payload", nullReference(), 1L));
        assertThrowsNoOp(IllegalArgumentException.class, fixture.scheduler,
                () -> fixture.scheduler.enqueue(fixture.inactiveFlow, "zero-cost", new Object(), 0L));
        assertThrowsNoOp(IllegalArgumentException.class, fixture.scheduler,
                () -> fixture.scheduler.enqueue(fixture.inactiveFlow, "negative-cost", new Object(), -1L));
    }

    @Test
    void cancelNonSuccessOutcomesAndInvalidArgumentPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture fixture = fixture(5, 8);
        SfqdScheduler<String, String, Object> foreign =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
        FlowHandle foreignFlow = registered(foreign.registerFlow("foreign", 1L));
        JobHandle foreignJob = accepted(foreign.enqueue(foreignFlow, "foreign-job", new Object(), 1L));

        assertNoOp(fixture.scheduler, CancelResult.NOT_LIVE, () -> fixture.scheduler.cancel(foreignJob));
        assertNoOp(
                fixture.scheduler,
                CancelResult.NOT_LIVE,
                () -> fixture.scheduler.cancel(fixture.completedJob));
        assertNoOp(
                fixture.scheduler,
                CancelResult.TOO_LATE_ALREADY_DISPATCHED,
                () -> fixture.scheduler.cancel(fixture.runningJob));

        assertEquals(CancelResult.CANCELLED, fixture.scheduler.cancel(fixture.queuedTail));
        assertNoOp(
                fixture.scheduler,
                CancelResult.NOT_LIVE,
                () -> fixture.scheduler.cancel(fixture.queuedTail));
        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.cancel(nullReference()));
    }

    @Test
    void completionNonSuccessOutcomesAndInvalidArgumentPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture fixture = fixture(5, 8);
        SfqdScheduler<String, String, Object> foreign =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
        FlowHandle foreignFlow = registered(foreign.registerFlow("foreign", 1L));
        JobHandle foreignJob = accepted(foreign.enqueue(foreignFlow, "foreign-job", new Object(), 1L));

        assertNoOp(
                fixture.scheduler,
                CompletionResult.NOT_DISPATCHED,
                () -> fixture.scheduler.complete(fixture.queuedJob));
        assertNoOp(
                fixture.scheduler,
                CompletionResult.NOT_LIVE,
                () -> fixture.scheduler.complete(fixture.completedJob));
        assertNoOp(
                fixture.scheduler,
                CompletionResult.NOT_LIVE,
                () -> fixture.scheduler.complete(foreignJob));

        assertEquals(CompletionResult.COMPLETED, fixture.scheduler.complete(fixture.runningJob));
        assertNoOp(
                fixture.scheduler,
                CompletionResult.NOT_LIVE,
                () -> fixture.scheduler.complete(fixture.runningJob));
        assertThrowsNoOp(NullPointerException.class, fixture.scheduler,
                () -> fixture.scheduler.complete(nullReference()));
    }

    @Test
    void invalidAndZeroCapacityCallsPreserveCompleteProductionState()
            throws ReflectiveOperationException {
        Fixture fixture = fixture(5, 8);

        assertNoOp(fixture.scheduler, List.of(), () -> fixture.scheduler.capacityAvailable(0));
        assertThrowsNoOp(IllegalArgumentException.class, fixture.scheduler,
                () -> fixture.scheduler.capacityAvailable(-1));
        assertThrowsNoOp(IllegalArgumentException.class, fixture.scheduler,
                () -> fixture.scheduler.capacityAvailable(3));
    }

    private static Fixture fixture(int maxFlows, int maxLiveJobs) {
        SfqdScheduler<String, String, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, maxFlows, maxLiveJobs));
        FlowHandle flowA = registered(scheduler.registerFlow("flow-a", 3L));
        FlowHandle flowB = registered(scheduler.registerFlow("flow-b", 5L));
        FlowHandle flowC = registered(scheduler.registerFlow("flow-c", 7L));
        FlowHandle inactive = registered(scheduler.registerFlow("inactive", 11L));
        JobHandle a1 = accepted(scheduler.enqueue(flowA, "a1", new Object(), 2L));
        JobHandle a2 = accepted(scheduler.enqueue(flowA, "a2", new Object(), 3L));
        accepted(scheduler.enqueue(flowA, "a3", new Object(), 5L));
        JobHandle b1 = accepted(scheduler.enqueue(flowB, "b1", new Object(), 1L));
        JobHandle b2 = accepted(scheduler.enqueue(flowB, "b2", new Object(), 7L));
        assertEquals(List.of(a1, b1), scheduler.capacityAvailable(2).stream()
                .map(Dispatch::jobHandle)
                .toList());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(b1));
        assertEquals(List.of(b2), scheduler.capacityAvailable(1).stream()
                .map(Dispatch::jobHandle)
                .toList());
        JobHandle inactiveDebt = accepted(
                scheduler.enqueue(inactive, "inactive-debt", new Object(), 11L));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(inactiveDebt));
        accepted(scheduler.enqueue(flowC, "c1", new Object(), 1L));
        JobHandle c2 = accepted(scheduler.enqueue(flowC, "c2", new Object(), 2L));
        assertEquals(new SchedulerSnapshot(
                2, maxFlows, maxLiveJobs, 4, 4, 2, 0, 3, 2, 8L, 3L, 1L, 1L),
                scheduler.snapshot());
        return new Fixture(scheduler, flowA, inactive, a1, a2, c2, b1);
    }

    private static <T> void assertNoOp(
            SfqdScheduler<?, ?, ?> scheduler,
            T expected,
            Supplier<T> invocation) throws ReflectiveOperationException {
        Object before = SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler);

        assertEquals(expected, invocation.get());

        assertEquals(before, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));
    }

    private static <T extends Throwable> void assertThrowsNoOp(
            Class<T> expected,
            SfqdScheduler<?, ?, ?> scheduler,
            Executable invocation) throws ReflectiveOperationException {
        Object before = SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler);

        assertThrows(expected, invocation);

        assertEquals(before, SfqdNumericBoundaryTest.NumericProbe.captureDeepState(scheduler));
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }

    private static <T> T nullReference() {
        return new AtomicReference<T>().get();
    }

    private record Fixture(
            SfqdScheduler<String, String, Object> scheduler,
            FlowHandle flowA,
            FlowHandle inactiveFlow,
            JobHandle runningJob,
            JobHandle queuedJob,
            JobHandle queuedTail,
            JobHandle completedJob) {
    }
}
