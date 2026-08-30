package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SfqdDeterministicConcurrencyTest {
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void duplicateConcurrentEnqueueAcceptsExactlyOneIncarnation() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));

        RaceResult<EnqueueResult, EnqueueResult> race = race(
                () -> scheduler.enqueue(flow, "job", "first", 1L),
                () -> scheduler.enqueue(flow, "job", "second", 1L));

        List<EnqueueResult> results = List.of(race.first, race.second);
        assertEquals(1L, results.stream().filter(EnqueueResult.Accepted.class::isInstance).count());
        assertEquals(1L, results.stream()
                .filter(result -> result == EnqueueResult.Rejected.DUPLICATE_LIVE_ID).count());
        assertRealTimeWitness(
                race,
                () -> assertAcceptedThenRejected(race.first, race.second,
                        EnqueueResult.Rejected.DUPLICATE_LIVE_ID),
                () -> assertAcceptedThenRejected(race.second, race.first,
                        EnqueueResult.Rejected.DUPLICATE_LIVE_ID));
        JobHandle accepted = results.stream()
                .filter(EnqueueResult.Accepted.class::isInstance)
                .map(EnqueueResult.Accepted.class::cast)
                .map(EnqueueResult.Accepted::jobHandle)
                .findFirst()
                .orElseThrow();
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(accepted));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void concurrentEnqueueAtLiveLimitAcceptsOneDifferentIdentifier() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));

        RaceResult<EnqueueResult, EnqueueResult> race = race(
                () -> scheduler.enqueue(flow, "first", "first-p", 1L),
                () -> scheduler.enqueue(flow, "second", "second-p", 1L));

        List<EnqueueResult> results = List.of(race.first, race.second);
        assertEquals(1L, results.stream().filter(EnqueueResult.Accepted.class::isInstance).count());
        assertEquals(1L, results.stream().filter(result -> result == EnqueueResult.Rejected.LIVE_LIMIT).count());
        assertRealTimeWitness(
                race,
                () -> assertAcceptedThenRejected(race.first, race.second, EnqueueResult.Rejected.LIVE_LIMIT),
                () -> assertAcceptedThenRejected(race.second, race.first, EnqueueResult.Rejected.LIVE_LIMIT));
        JobHandle accepted = results.stream()
                .filter(EnqueueResult.Accepted.class::isInstance)
                .map(EnqueueResult.Accepted.class::cast)
                .map(EnqueueResult.Accepted::jobHandle)
                .findFirst()
                .orElseThrow();
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(accepted));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void sameTagConcurrentEnqueueDispatchesInOneAdmissionPermutation() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 2, 2);
        FlowHandle firstFlow = registered(scheduler.registerFlow("first-flow", 1L));
        FlowHandle secondFlow = registered(scheduler.registerFlow("second-flow", 1L));

        RaceResult<EnqueueResult, EnqueueResult> race = race(
                () -> scheduler.enqueue(firstFlow, "first", "first-p", 1L),
                () -> scheduler.enqueue(secondFlow, "second", "second-p", 1L));

        JobHandle first = accepted(race.first);
        JobHandle second = accepted(race.second);
        List<Dispatch<String, String, String>> batch = scheduler.dispatchUpTo(2);
        List<JobHandle> order = batch.stream().map(Dispatch::jobHandle).toList();
        assertTrue(order.equals(List.of(first, second)) || order.equals(List.of(second, first)));
        assertRealTimeWitness(
                race,
                () -> assertEquals(List.of(first, second), order),
                () -> assertEquals(List.of(second, first), order));
        assertEquals(Set.of(first, second), Set.copyOf(order));
        completeAll(scheduler, batch);
        assertConservation(scheduler.snapshot());
    }

    @Test
    void enqueueOfLowerStartTagRacingDispatchHasExactlyTwoLinearizations() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 2, 4);
        FlowHandle charged = registered(scheduler.registerFlow("charged", 1L));
        FlowHandle fresh = registered(scheduler.registerFlow("fresh", 1L));
        JobHandle virtualCharge = accepted(scheduler.enqueue(charged, "charge", "charge-p", 10L));
        accepted(scheduler.enqueue(charged, "old", "old-p", 1L));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(virtualCharge));

        RaceResult<EnqueueResult, List<Dispatch<String, String, String>>> race = race(
                () -> scheduler.enqueue(fresh, "new", "new-p", 1L),
                () -> scheduler.dispatchUpTo(1));

        JobHandle newJob = accepted(race.first);
        assertEquals(1, race.second.size());
        assertTrue(Set.of("old", "new").contains(race.second.get(0).jobId()));
        assertRealTimeWitness(
                race,
                () -> assertEquals("new", race.second.get(0).jobId()),
                () -> assertEquals("old", race.second.get(0).jobId()));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(race.second.get(0).jobHandle()));
        List<Dispatch<String, String, String>> remaining = scheduler.dispatchUpTo(1);
        assertEquals(1, remaining.size());
        assertEquals(Set.of("old", "new"),
                Set.of(race.second.get(0).jobId(), remaining.get(0).jobId()));
        assertTrue(race.second.get(0).jobHandle().equals(newJob)
                || remaining.get(0).jobHandle().equals(newJob));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(remaining.get(0).jobHandle()));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void cancelRacingReenqueueSameIdentifierCannotCreateAba() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle old = accepted(scheduler.enqueue(flow, "job", "old-p", 1L));

        RaceResult<CancelResult, EnqueueResult> race = race(
                () -> scheduler.cancel(old),
                () -> scheduler.enqueue(flow, "job", "new-p", 1L));

        assertEquals(CancelResult.CANCELLED, race.first);
        assertRealTimeWitness(
                race,
                () -> assertInstanceOf(EnqueueResult.Accepted.class, race.second),
                () -> assertEquals(EnqueueResult.Rejected.DUPLICATE_LIVE_ID, race.second));
        assertEquals(CancelResult.NOT_LIVE, scheduler.cancel(old));
        if (race.second instanceof EnqueueResult.Accepted newAdmission) {
            assertNotEquals(old, newAdmission.jobHandle());
            List<Dispatch<String, String, String>> batch = scheduler.dispatchUpTo(1);
            assertEquals(List.of(newAdmission.jobHandle()), batch.stream().map(Dispatch::jobHandle).toList());
            assertSame("new-p", batch.get(0).payload());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(newAdmission.jobHandle()));
        } else {
            assertEquals(EnqueueResult.Rejected.DUPLICATE_LIVE_ID, race.second);
            assertTrue(scheduler.dispatchUpTo(1).isEmpty());
        }
        assertConservation(scheduler.snapshot());
    }

    @Test
    void depthOneConcurrentDispatchIssuesJobAtMostOnce() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle job = accepted(scheduler.enqueue(flow, "job", "p", 1L));

        RaceResult<List<Dispatch<String, String, String>>, List<Dispatch<String, String, String>>> race = race(
                () -> scheduler.dispatchUpTo(1),
                () -> scheduler.dispatchUpTo(1));

        assertEquals(List.of(0, 1), sortedSizes(race.first, race.second));
        assertRealTimeWitness(
                race,
                () -> assertEquals(List.of(job), race.first.stream().map(Dispatch::jobHandle).toList()),
                () -> assertEquals(List.of(job), race.second.stream().map(Dispatch::jobHandle).toList()));
        assertEquals(List.of(job), combinedHandles(race.first, race.second));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(job));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void depthTwoConcurrentWholeBatchesNeverSplitAtomicBatch() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle first = accepted(scheduler.enqueue(flow, "first", "first-p", 1L));
        JobHandle second = accepted(scheduler.enqueue(flow, "second", "second-p", 1L));

        RaceResult<List<Dispatch<String, String, String>>, List<Dispatch<String, String, String>>> race = race(
                () -> scheduler.dispatchUpTo(2),
                () -> scheduler.dispatchUpTo(2));

        assertEquals(List.of(0, 2), sortedSizes(race.first, race.second));
        assertRealTimeWitness(
                race,
                () -> assertEquals(2, race.first.size()),
                () -> assertEquals(2, race.second.size()));
        assertEquals(Set.of(first, second), Set.copyOf(combinedHandles(race.first, race.second)));
        completeAll(scheduler, race.first);
        completeAll(scheduler, race.second);
        assertConservation(scheduler.snapshot());
    }

    @Test
    void depthTwoConcurrentSingleDispatchesUseDisjointCapacity() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle first = accepted(scheduler.enqueue(flow, "first", "first-p", 1L));
        JobHandle second = accepted(scheduler.enqueue(flow, "second", "second-p", 1L));

        RaceResult<List<Dispatch<String, String, String>>, List<Dispatch<String, String, String>>> race = race(
                () -> scheduler.dispatchUpTo(1),
                () -> scheduler.dispatchUpTo(1));

        assertEquals(List.of(1, 1), sortedSizes(race.first, race.second));
        List<JobHandle> handles = combinedHandles(race.first, race.second);
        assertEquals(Set.of(first, second), Set.copyOf(handles));
        assertEquals(2, new HashSet<>(handles).size());
        assertRealTimeWitness(
                race,
                () -> assertEquals(List.of(first), race.first.stream().map(Dispatch::jobHandle).toList()),
                () -> assertEquals(List.of(first), race.second.stream().map(Dispatch::jobHandle).toList()));
        completeAll(scheduler, race.first);
        completeAll(scheduler, race.second);
        assertConservation(scheduler.snapshot());
    }

    @Test
    void selectedCancelDispatchRaceReportsTheWinnerInCombinedHistory() {
        assertSelectedCancelDispatchRace(CancellationAccounting.CHARGE_RESERVED_COST);
    }

    @Test
    void selectedRefundCancelDispatchRaceReportsTheWinnerInCombinedHistory() {
        assertSelectedCancelDispatchRace(CancellationAccounting.REFUND_CANCELLED_COST);
    }

    private static void assertSelectedCancelDispatchRace(CancellationAccounting policy) {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 1, policy));
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle victim = accepted(scheduler.enqueue(flow, "victim", "p", 1L));

        RaceResult<List<Dispatch<String, String, String>>, CancelResult> race = race(
                () -> scheduler.dispatchUpTo(1),
                () -> scheduler.cancel(victim));

        if (race.second == CancelResult.CANCELLED) {
            assertTrue(race.first.isEmpty());
        } else {
            assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, race.second);
            assertEquals(List.of(victim), race.first.stream().map(Dispatch::jobHandle).toList());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(victim));
        }
        assertRealTimeWitness(
                race,
                () -> {
                    assertEquals(List.of(victim), race.first.stream().map(Dispatch::jobHandle).toList());
                    assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, race.second);
                },
                () -> {
                    assertTrue(race.first.isEmpty());
                    assertEquals(CancelResult.CANCELLED, race.second);
                });
        assertConservation(scheduler.snapshot());
    }

    @Test
    void dispatchOfFirstJobDoesNotBlockConcurrentCancelOfUnselectedSecond() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle first = accepted(scheduler.enqueue(flow, "first", "first-p", 1L));
        JobHandle second = accepted(scheduler.enqueue(flow, "second", "second-p", 1L));

        RaceResult<List<Dispatch<String, String, String>>, CancelResult> race = race(
                () -> scheduler.dispatchUpTo(1),
                () -> scheduler.cancel(second));

        assertEquals(CancelResult.CANCELLED, race.second);
        assertEquals(List.of(first), race.first.stream().map(Dispatch::jobHandle).toList());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(first));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void concurrentCancelSucceedsExactlyOnce() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle job = accepted(scheduler.enqueue(flow, "job", "p", 1L));

        RaceResult<CancelResult, CancelResult> race = race(
                () -> scheduler.cancel(job),
                () -> scheduler.cancel(job));

        assertEquals(Set.of(CancelResult.CANCELLED, CancelResult.NOT_LIVE), Set.of(race.first, race.second));
        assertRealTimeWitness(
                race,
                () -> {
                    assertEquals(CancelResult.CANCELLED, race.first);
                    assertEquals(CancelResult.NOT_LIVE, race.second);
                },
                () -> {
                    assertEquals(CancelResult.NOT_LIVE, race.first);
                    assertEquals(CancelResult.CANCELLED, race.second);
                });
        assertConservation(scheduler.snapshot());
    }

    @Test
    void concurrentCompletionSucceedsExactlyOnceAndReleasesOneSlot() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle job = accepted(scheduler.enqueue(flow, "job", "p", 1L));
        scheduler.dispatchUpTo(1);

        RaceResult<CompletionResult, CompletionResult> race = race(
                () -> scheduler.complete(job),
                () -> scheduler.complete(job));

        assertEquals(Set.of(CompletionResult.COMPLETED, CompletionResult.NOT_LIVE),
                Set.of(race.first, race.second));
        assertRealTimeWitness(
                race,
                () -> {
                    assertEquals(CompletionResult.COMPLETED, race.first);
                    assertEquals(CompletionResult.NOT_LIVE, race.second);
                },
                () -> {
                    assertEquals(CompletionResult.NOT_LIVE, race.first);
                    assertEquals(CompletionResult.COMPLETED, race.second);
                });
        SchedulerSnapshot snapshot = scheduler.snapshot();
        assertEquals(1, snapshot.freeSlots());
        assertEquals(1L, snapshot.completedTotal());
        assertConservation(snapshot);
    }

    @Test
    void completionDispatchRaceUsesFreedSlotOnlyWhenCompletionLinearizesFirst() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle running = accepted(scheduler.enqueue(flow, "running", "running-p", 1L));
        JobHandle next = accepted(scheduler.enqueue(flow, "next", "next-p", 1L));
        scheduler.dispatchUpTo(1);

        RaceResult<CompletionResult, List<Dispatch<String, String, String>>> race = race(
                () -> scheduler.complete(running),
                () -> scheduler.dispatchUpTo(1));

        assertEquals(CompletionResult.COMPLETED, race.first);
        assertRealTimeWitness(
                race,
                () -> assertEquals(List.of(next), race.second.stream().map(Dispatch::jobHandle).toList()),
                () -> assertTrue(race.second.isEmpty()));
        if (race.second.isEmpty()) {
            assertEquals(new SchedulerSnapshot(1, 1, 2, 1, 1, 0, 1, 1, 1, 2L, 1L, 0L, 1L),
                    scheduler.snapshot());
            assertEquals(List.of(next),
                    scheduler.dispatchUpTo(1).stream().map(Dispatch::jobHandle).toList());
        } else {
            assertEquals(List.of(next), race.second.stream().map(Dispatch::jobHandle).toList());
            assertEquals(new SchedulerSnapshot(1, 1, 2, 1, 0, 1, 0, 1, 0, 2L, 2L, 0L, 1L),
                    scheduler.snapshot());
        }
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(next));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void queuedCancelCompletionRaceNeverCompletesQueuedJob() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle job = accepted(scheduler.enqueue(flow, "job", "p", 1L));

        RaceResult<CancelResult, CompletionResult> race = race(
                () -> scheduler.cancel(job),
                () -> scheduler.complete(job));

        assertEquals(CancelResult.CANCELLED, race.first);
        assertTrue(race.second == CompletionResult.NOT_DISPATCHED || race.second == CompletionResult.NOT_LIVE);
        assertNotEquals(CompletionResult.COMPLETED, race.second);
        assertRealTimeWitness(
                race,
                () -> assertEquals(CompletionResult.NOT_LIVE, race.second),
                () -> assertEquals(CompletionResult.NOT_DISPATCHED, race.second));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void concurrentDifferentRegistrationsRespectFlowLimit() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        assertEquals(emptySnapshot(1, 1, 1), scheduler.snapshot());

        RaceResult<RegisterFlowResult, RegisterFlowResult> race = race(
                () -> scheduler.registerFlow("first", 1L),
                () -> scheduler.registerFlow("second", 1L));

        List<RegisterFlowResult> results = List.of(race.first, race.second);
        assertEquals(1L, results.stream().filter(RegisterFlowResult.Registered.class::isInstance).count());
        assertEquals(1L, results.stream().filter(result -> result == RegisterFlowResult.Rejected.FLOW_LIMIT).count());
        assertRealTimeWitness(
                race,
                () -> assertRegisteredThenRejected(race.first, race.second, RegisterFlowResult.Rejected.FLOW_LIMIT),
                () -> assertRegisteredThenRejected(race.second, race.first, RegisterFlowResult.Rejected.FLOW_LIMIT));
        FlowHandle registered = results.stream()
                .filter(RegisterFlowResult.Registered.class::isInstance)
                .map(RegisterFlowResult.Registered.class::cast)
                .map(RegisterFlowResult.Registered::flowHandle)
                .findFirst()
                .orElseThrow();
        assertEquals(snapshotWithRegistrations(1, 1, 1, 1), scheduler.snapshot());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(registered));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(registered));
        assertEquals(emptySnapshot(1, 1, 1), scheduler.snapshot());
        String losingId = race.first instanceof RegisterFlowResult.Registered ? "second" : "first";
        FlowHandle recycled = registered(scheduler.registerFlow(losingId, 1L));
        assertEquals(snapshotWithRegistrations(1, 1, 1, 1), scheduler.snapshot());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(recycled));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(recycled));
        assertEquals(emptySnapshot(1, 1, 1), scheduler.snapshot());
    }

    @Test
    void concurrentSameIdentifierRegistrationCreatesOneCapability() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 2, 1);
        assertEquals(emptySnapshot(1, 2, 1), scheduler.snapshot());

        RaceResult<RegisterFlowResult, RegisterFlowResult> race = race(
                () -> scheduler.registerFlow("flow", 1L),
                () -> scheduler.registerFlow("flow", 2L));

        List<RegisterFlowResult> results = List.of(race.first, race.second);
        assertEquals(1L, results.stream().filter(RegisterFlowResult.Registered.class::isInstance).count());
        assertEquals(1L, results.stream()
                .filter(result -> result == RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID).count());
        assertRealTimeWitness(
                race,
                () -> assertRegisteredThenRejected(
                        race.first, race.second, RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID),
                () -> assertRegisteredThenRejected(
                        race.second, race.first, RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID));
        FlowHandle registered = results.stream()
                .filter(RegisterFlowResult.Registered.class::isInstance)
                .map(RegisterFlowResult.Registered.class::cast)
                .map(RegisterFlowResult.Registered::flowHandle)
                .findFirst()
                .orElseThrow();
        assertEquals(snapshotWithRegistrations(1, 2, 1, 1), scheduler.snapshot());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(registered));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(registered));
        assertEquals(emptySnapshot(1, 2, 1), scheduler.snapshot());
        FlowHandle reincarnated = registered(scheduler.registerFlow("flow", 3L));
        assertNotEquals(registered, reincarnated);
        assertEquals(snapshotWithRegistrations(1, 2, 1, 1), scheduler.snapshot());
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(reincarnated));
        assertEquals(emptySnapshot(1, 2, 1), scheduler.snapshot());
    }

    @Test
    void closeEnqueueRaceOnIdleFlowHasOnlyCapabilityOrderedOutcomes() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));

        RaceResult<CloseFlowResult, EnqueueResult> race = race(
                () -> scheduler.closeFlow(flow),
                () -> scheduler.enqueue(flow, "job", "p", 1L));

        assertRealTimeWitness(
                race,
                () -> {
                    assertEquals(CloseFlowResult.CLOSED, race.first);
                    assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED, race.second);
                },
                () -> {
                    assertEquals(CloseFlowResult.FLOW_ACTIVE, race.first);
                    assertInstanceOf(EnqueueResult.Accepted.class, race.second);
                });
        if (race.first == CloseFlowResult.CLOSED) {
            assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED, race.second);
        } else {
            assertEquals(CloseFlowResult.FLOW_ACTIVE, race.first);
            JobHandle job = accepted(race.second);
            assertEquals(CancelResult.CANCELLED, scheduler.cancel(job));
            assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(flow));
        }
        assertConservation(scheduler.snapshot());
    }

    @Test
    void indebtedCloseRacingLastCompletionObservesWholeBusyPeriodTransition() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 2, 2);
        FlowHandle active = registered(scheduler.registerFlow("active", 1L));
        FlowHandle inactive = registered(scheduler.registerFlow("inactive", 1L));
        JobHandle debt = accepted(scheduler.enqueue(inactive, "debt", "debt-p", 1L));
        JobHandle running = accepted(scheduler.enqueue(active, "job", "p", 1L));
        assertEquals(CancelResult.CANCELLED, scheduler.cancel(debt));
        scheduler.dispatchUpTo(1);

        RaceResult<CloseFlowResult, CompletionResult> race = race(
                () -> scheduler.closeFlow(inactive),
                () -> scheduler.complete(running));

        assertEquals(CompletionResult.COMPLETED, race.second);
        assertTrue(race.first == CloseFlowResult.FAIRNESS_DEBT_ACTIVE || race.first == CloseFlowResult.CLOSED);
        assertRealTimeWitness(
                race,
                () -> assertEquals(CloseFlowResult.FAIRNESS_DEBT_ACTIVE, race.first),
                () -> assertEquals(CloseFlowResult.CLOSED, race.first));
        if (race.first == CloseFlowResult.FAIRNESS_DEBT_ACTIVE) {
            assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(inactive));
        } else {
            assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(inactive));
        }
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(active));
        assertConservation(scheduler.snapshot());
    }

    @Test
    void closeOldRegistrationRacingSameIdentifierRegisterNeverAliasesCapability() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle old = registered(scheduler.registerFlow("flow", 1L));

        RaceResult<CloseFlowResult, RegisterFlowResult> race = race(
                () -> scheduler.closeFlow(old),
                () -> scheduler.registerFlow("flow", 2L));

        assertEquals(CloseFlowResult.CLOSED, race.first);
        assertRealTimeWitness(
                race,
                () -> assertInstanceOf(RegisterFlowResult.Registered.class, race.second),
                () -> assertEquals(RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID, race.second));
        FlowHandle fresh;
        if (race.second instanceof RegisterFlowResult.Registered registered) {
            fresh = registered.flowHandle();
        } else {
            assertEquals(RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID, race.second);
            fresh = registered(scheduler.registerFlow("flow", 2L));
        }
        assertNotEquals(old, fresh);
        assertEquals(snapshotWithRegistrations(1, 1, 1, 1), scheduler.snapshot());
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(old));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(old));
        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(fresh));
        assertEquals(CloseFlowResult.FLOW_NOT_REGISTERED, scheduler.closeFlow(fresh));
        assertEquals(emptySnapshot(1, 1, 1), scheduler.snapshot());
    }

    @Test
    void snapshotRacingAtomicBatchSeesOnlyWholePreOrPostState() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        accepted(scheduler.enqueue(flow, "first", "first-p", 1L));
        accepted(scheduler.enqueue(flow, "second", "second-p", 1L));
        SchedulerSnapshot before = scheduler.snapshot();

        RaceResult<List<Dispatch<String, String, String>>, SchedulerSnapshot> race = race(
                () -> scheduler.dispatchUpTo(2),
                scheduler::snapshot);

        assertEquals(2, race.first.size());
        SchedulerSnapshot after = scheduler.snapshot();
        assertTrue(race.second.equals(before) || race.second.equals(after));
        assertRealTimeWitness(
                race,
                () -> assertEquals(after, race.second),
                () -> assertEquals(before, race.second));
        assertEquals(0, after.queuedJobs());
        assertEquals(2, after.runningJobs());
        completeAll(scheduler, race.first);
        assertConservation(scheduler.snapshot());
    }

    @Test
    void flowSnapshotRacingAtomicBatchSeesOnlyWholePreOrPostLifecycleCosts() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 1, 2);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        accepted(scheduler.enqueue(flow, "first", "first-p", 3L));
        accepted(scheduler.enqueue(flow, "second", "second-p", 5L));
        FlowSnapshot before = scheduler.snapshot(flow).orElseThrow();

        RaceResult<List<Dispatch<String, String, String>>, FlowSnapshot> race = race(
                () -> scheduler.dispatchUpTo(2),
                () -> scheduler.snapshot(flow).orElseThrow());

        assertEquals(2, race.first.size());
        FlowSnapshot after = scheduler.snapshot(flow).orElseThrow();
        assertTrue(race.second.equals(before) || race.second.equals(after));
        assertRealTimeWitness(
                race,
                () -> assertEquals(after, race.second),
                () -> assertEquals(before, race.second));
        assertEquals(new FlowSnapshot(
                0, 2, BigInteger.valueOf(8L), BigInteger.valueOf(8L), BigInteger.ZERO,
                BigInteger.valueOf(8L)), after);
        completeAll(scheduler, race.first);
        assertConservation(scheduler.snapshot());
    }

    @Test
    void flowSnapshotRacingCompletionSeesWholeRunningSuppliedCostTransition() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1, 1, 1);
        FlowHandle flow = registered(scheduler.registerFlow("flow", 1L));
        JobHandle running = accepted(scheduler.enqueue(flow, "job", "payload", 8L));
        scheduler.dispatchUpTo(1);
        FlowSnapshot before = scheduler.snapshot(flow).orElseThrow();

        RaceResult<CompletionResult, FlowSnapshot> race = race(
                () -> scheduler.complete(running),
                () -> scheduler.snapshot(flow).orElseThrow());

        assertEquals(CompletionResult.COMPLETED, race.first);
        FlowSnapshot after = scheduler.snapshot(flow).orElseThrow();
        assertTrue(race.second.equals(before) || race.second.equals(after));
        assertRealTimeWitness(
                race,
                () -> assertEquals(after, race.second),
                () -> assertEquals(before, race.second));
        assertEquals(BigInteger.ZERO, after.runningSuppliedCost());
        assertEquals(BigInteger.valueOf(8L), after.completedSuppliedCost());
        assertConservation(scheduler.snapshot());
    }

    @Test
    void numericLimitRejectionIsExactNoOpBesideConcurrentAdmission()
            throws NumericLimitException, ReflectiveOperationException {
        NumericNoOpFixture concurrent = numericNoOpFixture();
        NumericNoOpFixture serial = numericNoOpFixture();
        assertEquals(captureDeepState(serial.scheduler), captureDeepState(concurrent.scheduler));

        RaceResult<EnqueueResult, EnqueueResult> race = race(
                () -> concurrent.scheduler.enqueue(
                        concurrent.limited, "rejected", "rejected-p", Long.MAX_VALUE),
                () -> concurrent.scheduler.enqueue(concurrent.valid, "valid", "valid-p", 1L));
        EnqueueResult serialRejected = serial.scheduler.enqueue(
                serial.limited, "rejected", "rejected-p", Long.MAX_VALUE);
        EnqueueResult serialAccepted = serial.scheduler.enqueue(serial.valid, "valid", "valid-p", 1L);

        assertEquals(EnqueueResult.Rejected.NUMERIC_LIMIT, race.first);
        assertEquals(EnqueueResult.Rejected.NUMERIC_LIMIT, serialRejected);
        accepted(race.second);
        accepted(serialAccepted);
        assertEquals(captureDeepState(serial.scheduler), captureDeepState(concurrent.scheduler));
        assertEquals(drainFixture(serial.scheduler, serial.running),
                drainFixture(concurrent.scheduler, concurrent.running));
        assertConservation(concurrent.scheduler.snapshot());
    }

    @Test
    void successfulNonzeroRebaseRacingCloseMatchesWholeSerialTransition()
            throws NumericLimitException, ReflectiveOperationException {
        SuccessfulRebaseFixture concurrent = successfulRebaseFixture();
        DeepState before = captureDeepState(concurrent.scheduler);
        assertNotEquals(ExactTag.zero(), before.virtualTime);

        RaceResult<EnqueueResult, CloseFlowResult> race = race(
                () -> concurrent.scheduler.enqueue(concurrent.target, "target", "target-p", 1L),
                () -> concurrent.scheduler.closeFlow(concurrent.target));

        accepted(race.first);
        assertTrue(race.second == CloseFlowResult.FLOW_ACTIVE
                || race.second == CloseFlowResult.FAIRNESS_DEBT_ACTIVE);
        assertRealTimeWitness(
                race,
                () -> assertEquals(CloseFlowResult.FLOW_ACTIVE, race.second),
                () -> assertEquals(CloseFlowResult.FAIRNESS_DEBT_ACTIVE, race.second));

        SuccessfulRebaseFixture serial = successfulRebaseFixture();
        assertEquals(before, captureDeepState(serial.scheduler));
        EnqueueResult serialEnqueue;
        CloseFlowResult serialClose;
        if (race.second == CloseFlowResult.FLOW_ACTIVE) {
            serialEnqueue = serial.scheduler.enqueue(serial.target, "target", "target-p", 1L);
            serialClose = serial.scheduler.closeFlow(serial.target);
        } else {
            serialClose = serial.scheduler.closeFlow(serial.target);
            serialEnqueue = serial.scheduler.enqueue(serial.target, "target", "target-p", 1L);
        }
        accepted(serialEnqueue);
        assertEquals(race.second, serialClose);
        DeepState after = captureDeepState(concurrent.scheduler);
        assertEquals(captureDeepState(serial.scheduler), after);
        assertSuccessfulRebaseState(after, concurrent.base);
        assertEquals(List.of("a1", "b1", "a2", "target"),
                drainFixture(concurrent.scheduler, concurrent.running));
        assertEquals(List.of("a1", "b1", "a2", "target"),
                drainFixture(serial.scheduler, serial.running));
        assertConservation(concurrent.scheduler.snapshot());
    }

    private static NumericNoOpFixture numericNoOpFixture()
            throws NumericLimitException, ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 3, 4);
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        FlowHandle limited = registered(scheduler.registerFlow("limited", 1L));
        FlowHandle valid = registered(scheduler.registerFlow("valid", 1L));
        JobHandle running = accepted(scheduler.enqueue(anchor, "running", "running-p", 1L));
        scheduler.dispatchUpTo(1);
        ExactTag maximum = ExactTag.fromComponents(
                BigInteger.ONE.shiftLeft(4096).subtract(BigInteger.ONE), BigInteger.ONE);
        setFlowLastFinish(scheduler, limited, maximum);
        return new NumericNoOpFixture(scheduler, limited, valid, running);
    }

    private static SuccessfulRebaseFixture successfulRebaseFixture()
            throws NumericLimitException, ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler = scheduler(2, 5, 8);
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        FlowHandle first = registered(scheduler.registerFlow("a", 1L));
        FlowHandle second = registered(scheduler.registerFlow("b", 1L));
        FlowHandle target = registered(scheduler.registerFlow("target-flow", 1L));
        FlowHandle dormant = registered(scheduler.registerFlow("dormant", 1L));
        JobHandle running = accepted(scheduler.enqueue(anchor, "anchor", "anchor-p", 1L));
        scheduler.dispatchUpTo(1);
        accepted(scheduler.enqueue(first, "a1", "a1-p", 1L));
        accepted(scheduler.enqueue(first, "a2", "a2-p", 1L));
        accepted(scheduler.enqueue(second, "b1", "b1-p", 1L));
        ExactTag base = ExactTag.fromComponents(BigInteger.ONE.shiftLeft(4095), BigInteger.ONE);
        ExactTag targetFinish = ExactTag.fromComponents(
                BigInteger.ONE.shiftLeft(4096).subtract(BigInteger.ONE), BigInteger.ONE);
        setField(scheduler, "virtualTime", base);
        setFlowLastFinish(scheduler, anchor, base);
        shiftFlow(scheduler, first, base);
        shiftFlow(scheduler, second, base);
        setFlowLastFinish(scheduler, target, targetFinish);
        setFlowLastFinish(scheduler, dormant, base.add(ExactTag.fromCostAndWeight(7L, 1L)));
        return new SuccessfulRebaseFixture(scheduler, target, running, base);
    }

    private static void assertSuccessfulRebaseState(DeepState state, ExactTag base)
            throws NumericLimitException {
        ExactTag one = ExactTag.fromCostAndWeight(1L, 1L);
        ExactTag two = ExactTag.fromCostAndWeight(2L, 1L);
        ExactTag seven = ExactTag.fromCostAndWeight(7L, 1L);
        ExactTag targetStart = base.subtractNonNegative(one);
        assertEquals(ExactTag.zero(), state.virtualTime);
        assertEquals(Map.of(
                "anchor", ExactTag.zero(),
                "a", two,
                "b", one,
                "target-flow", base,
                "dormant", seven), state.flowLastFinishes);
        assertEquals(Map.of(
                "a1", new TagPair(ExactTag.zero(), one),
                "a2", new TagPair(one, two),
                "b1", new TagPair(ExactTag.zero(), one),
                "target", new TagPair(targetStart, base)), state.queuedTags);
        assertEquals(List.of("a1", "b1", "a2", "target"), state.priorityOrder);
        assertEquals(new SchedulerSnapshot(2, 5, 8, 5, 4, 1, 1, 4, 3, 5L, 1L, 0L, 0L),
                state.snapshot);
    }

    private static List<String> drainFixture(
            SfqdScheduler<String, String, String> scheduler,
            JobHandle running) {
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(running));
        List<String> order = new ArrayList<>();
        while (scheduler.snapshot().queuedJobs() != 0) {
            List<Dispatch<String, String, String>> batch = scheduler.dispatchUpTo(1);
            assertEquals(1, batch.size());
            order.add(batch.get(0).jobId());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(batch.get(0).jobHandle()));
        }
        return List.copyOf(order);
    }

    private static DeepState captureDeepState(SfqdScheduler<?, ?, ?> scheduler)
            throws ReflectiveOperationException {
        Map<String, ExactTag> flowTags = new LinkedHashMap<>();
        for (Object flow : mapField(scheduler, "registeredFlows").values()) {
            flowTags.put((String) getField(flow, "flowId"), (ExactTag) getField(flow, "lastFinish"));
        }
        List<Object> jobs = new ArrayList<>(mapField(scheduler, "queued").values());
        jobs.sort(Comparator
                .comparing((Object job) -> (ExactTag) getUnchecked(job, "start"),
                        SfqdDeterministicConcurrencyTest::compareUnchecked)
                .thenComparingLong(job -> (long) getUnchecked(job, "sequence")));
        Map<String, TagPair> queuedTags = new LinkedHashMap<>();
        List<String> priorityOrder = new ArrayList<>();
        for (Object job : jobs) {
            String jobId = (String) getField(job, "jobId");
            priorityOrder.add(jobId);
            queuedTags.put(jobId, new TagPair(
                    (ExactTag) getField(job, "start"),
                    (ExactTag) getField(job, "finish")));
        }
        return new DeepState(
                (ExactTag) getField(scheduler, "virtualTime"),
                Map.copyOf(flowTags),
                Map.copyOf(queuedTags),
                List.copyOf(priorityOrder),
                longField(scheduler, "lastJobSequence"),
                longField(scheduler, "lastFlowSequence"),
                longField(scheduler, "accepted"),
                longField(scheduler, "dispatched"),
                longField(scheduler, "cancelled"),
                longField(scheduler, "completed"),
                scheduler.snapshot());
    }

    private static void shiftFlow(
            SfqdScheduler<?, ?, ?> scheduler,
            FlowHandle handle,
            ExactTag base) throws ReflectiveOperationException {
        Object flow = mapField(scheduler, "registeredFlows").get(handle);
        Object job = getField(flow, "head");
        while (job != null) {
            setField(job, "start", addUnchecked(base, (ExactTag) getField(job, "start")));
            setField(job, "finish", addUnchecked(base, (ExactTag) getField(job, "finish")));
            job = getField(job, "next");
        }
        setField(flow, "lastFinish", addUnchecked(base, (ExactTag) getField(flow, "lastFinish")));
    }

    private static SfqdScheduler<String, String, String> scheduler(int depth, int flows, int jobs) {
        return new SfqdScheduler<>(new SchedulerConfig(depth, flows, jobs));
    }

    private static SchedulerSnapshot emptySnapshot(int depth, int maxFlows, int maxJobs) {
        return new SchedulerSnapshot(depth, maxFlows, maxJobs, 0, 0, 0, depth, 0, 0, 0L, 0L, 0L, 0L);
    }

    private static SchedulerSnapshot snapshotWithRegistrations(
            int depth,
            int maxFlows,
            int maxJobs,
            int registrations) {
        return new SchedulerSnapshot(
                depth, maxFlows, maxJobs, registrations, 0, 0, depth, 0, 0, 0L, 0L, 0L, 0L);
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }

    private static void assertAcceptedThenRejected(
            EnqueueResult first,
            EnqueueResult second,
            EnqueueResult.Rejected expectedRejection) {
        assertInstanceOf(EnqueueResult.Accepted.class, first);
        assertEquals(expectedRejection, second);
    }

    private static void assertRegisteredThenRejected(
            RegisterFlowResult first,
            RegisterFlowResult second,
            RegisterFlowResult.Rejected expectedRejection) {
        assertInstanceOf(RegisterFlowResult.Registered.class, first);
        assertEquals(expectedRejection, second);
    }

    private static void completeAll(
            SfqdScheduler<String, String, String> scheduler,
            List<Dispatch<String, String, String>> dispatches) {
        for (Dispatch<String, String, String> dispatch : dispatches) {
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(dispatch.jobHandle()));
        }
    }

    private static List<Integer> sortedSizes(List<?> first, List<?> second) {
        return List.of(first.size(), second.size()).stream().sorted().toList();
    }

    private static List<JobHandle> combinedHandles(
            List<Dispatch<String, String, String>> first,
            List<Dispatch<String, String, String>> second) {
        List<JobHandle> handles = new ArrayList<>(first.size() + second.size());
        first.stream().map(Dispatch::jobHandle).forEach(handles::add);
        second.stream().map(Dispatch::jobHandle).forEach(handles::add);
        return List.copyOf(handles);
    }

    private static void assertConservation(SchedulerSnapshot snapshot) {
        assertTrue(snapshot.runningJobs() <= snapshot.depth());
        assertEquals(snapshot.depth(), snapshot.runningJobs() + snapshot.freeSlots());
        assertEquals(snapshot.acceptedTotal(), snapshot.queuedJobs() + snapshot.runningJobs()
                + snapshot.cancelledTotal() + snapshot.completedTotal());
        assertEquals(snapshot.dispatchedTotal(), snapshot.runningJobs() + snapshot.completedTotal());
    }

    private static <A, B> RaceResult<A, B> race(Supplier<A> first, Supplier<B> second) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier start = new CyclicBarrier(3);
        AtomicLong clock = new AtomicLong();
        Future<CallWitness<A>> firstFuture = executor.submit(() -> afterBarrier(start, clock, first));
        Future<CallWitness<B>> secondFuture = executor.submit(() -> afterBarrier(start, clock, second));
        try {
            try {
                start.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                CallWitness<A> firstCall = firstFuture.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                CallWitness<B> secondCall = secondFuture.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                return new RaceResult<>(
                        firstCall.result, secondCall.result,
                        firstCall.invocation, firstCall.response,
                        secondCall.invocation, secondCall.response);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError("race coordinator interrupted", failure);
            } catch (BrokenBarrierException | ExecutionException | TimeoutException failure) {
                throw new AssertionError("race did not complete", failure);
            }
        } finally {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("race executor did not terminate");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while terminating race executor", failure);
            }
        }
    }

    private static <T> CallWitness<T> afterBarrier(
            CyclicBarrier barrier,
            AtomicLong clock,
            Supplier<T> operation) {
        try {
            barrier.await(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("race actor interrupted", failure);
        } catch (BrokenBarrierException | TimeoutException failure) {
            throw new AssertionError("race actor did not start", failure);
        }
        long invocation = clock.incrementAndGet();
        T result = operation.get();
        long response = clock.incrementAndGet();
        return new CallWitness<>(result, invocation, response);
    }

    private static void assertRealTimeWitness(
            RaceResult<?, ?> race,
            Runnable firstThenSecond,
            Runnable secondThenFirst) {
        if (race.firstResponse < race.secondInvocation) {
            firstThenSecond.run();
        }
        if (race.secondResponse < race.firstInvocation) {
            secondThenFirst.run();
        }
    }

    private static void setFlowLastFinish(
            SfqdScheduler<?, ?, ?> scheduler,
            FlowHandle handle,
            ExactTag value) throws ReflectiveOperationException {
        Object flow = mapField(scheduler, "registeredFlows").get(handle);
        setField(flow, "lastFinish", value);
    }

    private static ExactTag addUnchecked(ExactTag first, ExactTag second) {
        try {
            return first.add(second);
        } catch (NumericLimitException failure) {
            throw new AssertionError("fixture addition must fit", failure);
        }
    }

    private static int compareUnchecked(ExactTag first, ExactTag second) {
        try {
            return first.compareExact(second);
        } catch (NumericLimitException failure) {
            throw new AssertionError("persistent comparison must fit", failure);
        }
    }

    private static Object getUnchecked(Object target, String name) {
        try {
            return getField(target, name);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("test reflection failed", failure);
        }
    }

    private static Object getField(Object target, String name) throws ReflectiveOperationException {
        return field(target, name).get(target);
    }

    private static void setField(Object target, String name, Object value) throws ReflectiveOperationException {
        field(target, name).set(target, value);
    }

    private static long longField(Object target, String name) throws ReflectiveOperationException {
        return field(target, name).getLong(target);
    }

    @SuppressWarnings("unchecked") // Test reflection preserves the scheduler's declared map key/value types.
    private static Map<Object, Object> mapField(Object target, String name) throws ReflectiveOperationException {
        return (Map<Object, Object>) field(target, name).get(target);
    }

    private static Field field(Object target, String name) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private record CallWitness<T>(T result, long invocation, long response) {
    }

    private record NumericNoOpFixture(
            SfqdScheduler<String, String, String> scheduler,
            FlowHandle limited,
            FlowHandle valid,
            JobHandle running) {
    }

    private record SuccessfulRebaseFixture(
            SfqdScheduler<String, String, String> scheduler,
            FlowHandle target,
            JobHandle running,
            ExactTag base) {
    }

    private record TagPair(ExactTag start, ExactTag finish) {
    }

    private record DeepState(
            ExactTag virtualTime,
            Map<String, ExactTag> flowLastFinishes,
            Map<String, TagPair> queuedTags,
            List<String> priorityOrder,
            long lastJobSequence,
            long lastFlowSequence,
            long accepted,
            long dispatched,
            long cancelled,
            long completed,
            SchedulerSnapshot snapshot) {
    }

    private record RaceResult<A, B>(
            A first,
            B second,
            long firstInvocation,
            long firstResponse,
            long secondInvocation,
            long secondResponse) {
    }
}
