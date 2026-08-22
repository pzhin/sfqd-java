package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

class SfqdIndependentSchedulingProperties {
    @Property(tries = 120)
    void integerWeightsReceiveExactProportionsAtCompleteTagRounds(
            @ForAll("weightVectors") List<Integer> weights,
            @ForAll @IntRange(min = 1, max = 12) int rounds) {
        int observationJobs = rounds * weights.stream().mapToInt(Integer::intValue).sum();
        int liveJobs = observationJobs + weights.size();
        SfqdScheduler<String, String, Payload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, weights.size(), liveJobs));
        List<FlowHandle> flows = registerWeightedFlows(scheduler, weights);
        Map<JobHandle, Payload> payloads = new HashMap<>();
        Map<JobHandle, ExpectedJob> expectedJobs = new HashMap<>();
        Set<JobHandle> acceptedHandles = new HashSet<>();
        for (int flow = 0; flow < flows.size(); flow++) {
            int jobs = rounds * weights.get(flow) + 1;
            for (int job = 0; job < jobs; job++) {
                String jobId = "flow-" + flow + "-job-" + job;
                Payload payload = new Payload();
                JobHandle handle = accepted(scheduler.enqueue(flows.get(flow), jobId, payload, 1L));
                assertTrue(acceptedHandles.add(handle));
                payloads.put(handle, payload);
                expectedJobs.put(handle, new ExpectedJob(flow, "flow-" + flow, jobId));
            }
        }

        int[] completedByFlow = new int[flows.size()];
        Set<JobHandle> dispatchedHandles = new HashSet<>();
        for (int selected = 0; selected < observationJobs; selected++) {
            Dispatch<String, String, Payload> dispatch = scheduler.dispatchUpTo(1).getFirst();
            assertTrue(acceptedHandles.contains(dispatch.jobHandle()));
            assertTrue(dispatchedHandles.add(dispatch.jobHandle()));
            assertSame(payloads.get(dispatch.jobHandle()), dispatch.payload());
            ExpectedJob expected = expectedJobs.get(dispatch.jobHandle());
            assertNotNull(expected);
            assertEquals(expected.flowId, dispatch.flowId());
            assertEquals(expected.jobId, dispatch.jobId());
            assertEquals(1L, dispatch.cost());
            completedByFlow[expected.flowIndex]++;
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(dispatch.jobHandle()));
        }

        for (int flow = 0; flow < flows.size(); flow++) {
            assertEquals(rounds * weights.get(flow), completedByFlow[flow]);
        }
    }

    @Property(tries = 180)
    void dispatchUpToReturnsExactWorkConservingCount(
            @ForAll @IntRange(min = 1, max = 16) int depth,
            @ForAll @IntRange(min = 2, max = 6) int flowCount,
            @ForAll int stateCode) {
        SfqdScheduler<String, String, Payload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(depth, flowCount, 64));
        Map<JobHandle, Payload> payloads = new HashMap<>();
        Set<JobHandle> acceptedHandles = new HashSet<>();
        int totalJobs = 0;
        for (int flow = 0; flow < flowCount; flow++) {
            FlowHandle flowHandle = registered(scheduler.registerFlow("flow-" + flow, 1L + flow));
            int jobs = 1 + Math.floorMod(Integer.rotateLeft(stateCode, flow * 5), 8);
            for (int job = 0; job < jobs; job++) {
                Payload payload = new Payload();
                JobHandle handle = accepted(scheduler.enqueue(
                        flowHandle, "flow-" + flow + "-job-" + job, payload, 1L + job));
                assertTrue(acceptedHandles.add(handle));
                payloads.put(handle, payload);
                totalJobs++;
            }
        }
        int initialRunning = Math.floorMod(Integer.rotateRight(stateCode, 7), Math.min(depth, totalJobs) + 1);
        Set<JobHandle> dispatchedHandles = new HashSet<>();
        assertDispatchIdentity(
                scheduler.dispatchUpTo(initialRunning), acceptedHandles, payloads, dispatchedHandles);
        SchedulerSnapshot before = scheduler.snapshot();
        int requested = Math.floorMod(Integer.rotateLeft(stateCode, 11), depth + 1);
        int expected = Math.min(requested, Math.min(depth - before.runningJobs(), before.queuedJobs()));

        List<Dispatch<String, String, Payload>> selected = scheduler.dispatchUpTo(requested);

        assertEquals(expected, selected.size());
        assertDispatchIdentity(selected, acceptedHandles, payloads, dispatchedHandles);
        SchedulerSnapshot after = scheduler.snapshot();
        assertEquals(before.runningJobs() + expected, after.runningJobs());
        assertEquals(before.queuedJobs() - expected, after.queuedJobs());
        assertEquals(depth - after.runningJobs(), after.freeSlots());
    }

    @Property(tries = 100)
    void oneFlowFillsEveryDepthAndRefillsAfterAnyCompletionSubset(
            @ForAll("representativeDepths") int depth,
            @ForAll long completionPattern) {
        SfqdScheduler<String, String, Payload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(depth, 1, depth * 2));
        FlowHandle flow = registered(scheduler.registerFlow("only", 7L));
        Set<JobHandle> acceptedHandles = new HashSet<>();
        Map<JobHandle, Payload> payloads = new HashMap<>();
        for (int job = 0; job < depth * 2; job++) {
            Payload payload = new Payload();
            JobHandle handle = accepted(scheduler.enqueue(flow, "job-" + job, payload, 1L + job % 13));
            assertTrue(acceptedHandles.add(handle));
            payloads.put(handle, payload);
        }
        Set<JobHandle> dispatchedHandles = new HashSet<>();
        List<Dispatch<String, String, Payload>> initial = scheduler.dispatchUpTo(depth);
        assertEquals(depth, initial.size());
        assertDispatchIdentity(initial, acceptedHandles, payloads, dispatchedHandles);

        int completed = 0;
        for (int index = 0; index < initial.size(); index++) {
            long selector = Long.rotateLeft(completionPattern, index & 63) ^ (0x9E3779B97F4A7C15L * index);
            if ((selector & 1L) != 0L) {
                assertEquals(CompletionResult.COMPLETED, scheduler.complete(initial.get(index).jobHandle()));
                completed++;
            }
        }

        List<Dispatch<String, String, Payload>> refill = scheduler.dispatchUpTo(depth);
        assertEquals(completed, refill.size());
        assertDispatchIdentity(refill, acceptedHandles, payloads, dispatchedHandles);
        assertEquals(depth, scheduler.snapshot().runningJobs());
        assertEquals(0, scheduler.snapshot().freeSlots());
    }

    @Property(tries = 100)
    void globalIdleResetMakesNextBusyPeriodEquivalentToFreshScheduler(
            @ForAll @IntRange(min = 1, max = 8) int depth,
            @ForAll @IntRange(min = 2, max = 5) int flowCount,
            @ForAll @Size(min = 1, max = 32) List<@IntRange(min = -1_000, max = 1_000) Integer> prior,
            @ForAll @Size(min = 1, max = 40) List<@IntRange(min = -1_000, max = 1_000) Integer> future) {
        SchedulerConfig config = new SchedulerConfig(depth, flowCount, 64);
        SfqdScheduler<String, String, Payload> reused = new SfqdScheduler<>(config);
        SfqdScheduler<String, String, Payload> fresh = new SfqdScheduler<>(config);
        List<FlowHandle> reusedFlows = registerIndexedFlows(reused, flowCount);
        List<FlowHandle> freshFlows = registerIndexedFlows(fresh, flowCount);
        Set<JobHandle> reusedAccepted = new HashSet<>();
        Map<JobHandle, Payload> reusedPayloads = new HashMap<>();
        enqueueCommands(reused, reusedFlows, "prior", prior, reusedAccepted, reusedPayloads);
        drainAndComplete(reused, reusedAccepted, reusedPayloads);
        assertEquals(0, reused.snapshot().queuedJobs());
        assertEquals(0, reused.snapshot().runningJobs());
        assertEquals(depth, reused.snapshot().freeSlots());

        Set<JobHandle> freshAccepted = new HashSet<>();
        Map<JobHandle, Payload> freshPayloads = new HashMap<>();
        enqueueCommands(reused, reusedFlows, "future", future, reusedAccepted, reusedPayloads);
        enqueueCommands(fresh, freshFlows, "future", future, freshAccepted, freshPayloads);
        Set<JobHandle> reusedDispatched = new HashSet<>();
        Set<JobHandle> freshDispatched = new HashSet<>();
        while (reused.snapshot().queuedJobs() > 0) {
            List<Dispatch<String, String, Payload>> reusedBatch = reused.dispatchUpTo(depth);
            List<Dispatch<String, String, Payload>> freshBatch = fresh.dispatchUpTo(depth);
            assertEquals(reusedBatch.size(), freshBatch.size());
            assertDispatchIdentity(reusedBatch, reusedAccepted, reusedPayloads, reusedDispatched);
            assertDispatchIdentity(freshBatch, freshAccepted, freshPayloads, freshDispatched);
            for (int index = 0; index < reusedBatch.size(); index++) {
                Dispatch<String, String, Payload> reusedJob = reusedBatch.get(index);
                Dispatch<String, String, Payload> freshJob = freshBatch.get(index);
                assertEquals(reusedJob.jobId(), freshJob.jobId());
                assertEquals(reusedJob.flowId(), freshJob.flowId());
                assertEquals(reusedJob.cost(), freshJob.cost());
                assertEquals(CompletionResult.COMPLETED, reused.complete(reusedJob.jobHandle()));
                assertEquals(CompletionResult.COMPLETED, fresh.complete(freshJob.jobHandle()));
            }
        }
        assertEquals(0, fresh.snapshot().queuedJobs());
        assertEquals(0, reused.snapshot().runningJobs());
        assertEquals(0, fresh.snapshot().runningJobs());
    }

    @Property(tries = 120)
    void dormantCompetitorCanBypassVictimOnlyExactCeilingNumberOfTimes(
            @ForAll @IntRange(min = 1, max = 48) int victimWarmupCost,
            @ForAll @IntRange(min = 1, max = 8) int victimWeight,
            @ForAll @IntRange(min = 1, max = 8) int competitorCost,
            @ForAll @IntRange(min = 1, max = 8) int competitorWeight) {
        SfqdScheduler<String, String, Payload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 3, 4));
        FlowHandle victim = registered(scheduler.registerFlow("victim", victimWeight));
        FlowHandle anchor = registered(scheduler.registerFlow("anchor", 1L));
        FlowHandle competitor = registered(scheduler.registerFlow("competitor", competitorWeight));
        Set<JobHandle> acceptedHandles = new HashSet<>();
        Set<JobHandle> dispatchedHandles = new HashSet<>();
        Map<JobHandle, Payload> payloads = new HashMap<>();
        JobHandle warmup = enqueueTracked(
                scheduler, victim, "victim-warmup", victimWarmupCost, acceptedHandles, payloads);
        JobHandle anchorJob = enqueueTracked(scheduler, anchor, "anchor", 1L, acceptedHandles, payloads);
        List<Dispatch<String, String, Payload>> initial = scheduler.dispatchUpTo(2);
        assertDispatchIdentity(initial, acceptedHandles, payloads, dispatchedHandles);
        assertEquals(List.of("victim-warmup", "anchor"), initial.stream().map(Dispatch::jobId).toList());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(warmup));
        JobHandle victimHead = enqueueTracked(
                scheduler, victim, "victim-head", 1L, acceptedHandles, payloads);

        int bypassBound = exactCeiling(
                BigInteger.valueOf(victimWarmupCost).multiply(BigInteger.valueOf(competitorWeight)),
                BigInteger.valueOf(victimWeight).multiply(BigInteger.valueOf(competitorCost)));
        for (int bypass = 0; bypass < bypassBound; bypass++) {
            JobHandle competitorJob = enqueueTracked(
                    scheduler,
                    competitor,
                    "competitor-" + bypass,
                    competitorCost,
                    acceptedHandles,
                    payloads);
            Dispatch<String, String, Payload> selected = scheduler.dispatchUpTo(1).getFirst();
            assertDispatchIdentity(List.of(selected), acceptedHandles, payloads, dispatchedHandles);
            assertEquals(competitorJob, selected.jobHandle());
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(competitorJob));
        }
        JobHandle competitorAtBound = enqueueTracked(
                scheduler,
                competitor,
                "competitor-at-bound",
                competitorCost,
                acceptedHandles,
                payloads);

        Dispatch<String, String, Payload> selected = scheduler.dispatchUpTo(1).getFirst();

        assertDispatchIdentity(List.of(selected), acceptedHandles, payloads, dispatchedHandles);
        assertEquals(victimHead, selected.jobHandle());
        assertEquals("victim-head", selected.jobId());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(victimHead));
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(anchorJob));
        Dispatch<String, String, Payload> finalCompetitor = scheduler.dispatchUpTo(1).getFirst();
        assertEquals(competitorAtBound, finalCompetitor.jobHandle());
        assertEquals(CompletionResult.COMPLETED, scheduler.complete(competitorAtBound));
    }

    @Provide
    Arbitrary<List<Integer>> weightVectors() {
        return Arbitraries.integers().between(1, 8).list().ofMinSize(2).ofMaxSize(5);
    }

    @Provide
    Arbitrary<Integer> representativeDepths() {
        return Arbitraries.of(1, 2, 8, 64, 256);
    }

    private static List<FlowHandle> registerWeightedFlows(
            SfqdScheduler<String, String, Payload> scheduler, List<Integer> weights) {
        List<FlowHandle> flows = new ArrayList<>(weights.size());
        for (int flow = 0; flow < weights.size(); flow++) {
            flows.add(registered(scheduler.registerFlow("flow-" + flow, weights.get(flow))));
        }
        return flows;
    }

    private static List<FlowHandle> registerIndexedFlows(
            SfqdScheduler<String, String, Payload> scheduler, int flowCount) {
        List<FlowHandle> flows = new ArrayList<>(flowCount);
        for (int flow = 0; flow < flowCount; flow++) {
            flows.add(registered(scheduler.registerFlow("flow-" + flow, 1L + flow)));
        }
        return flows;
    }

    private static void enqueueCommands(
            SfqdScheduler<String, String, Payload> scheduler,
            List<FlowHandle> flows,
            String prefix,
            List<Integer> commands,
            Set<JobHandle> acceptedHandles,
            Map<JobHandle, Payload> payloads) {
        for (int index = 0; index < commands.size(); index++) {
            int command = commands.get(index);
            int flow = Math.floorMod(command, flows.size());
            long cost = 1L + Math.floorMod(Integer.rotateRight(command, 5), 17);
            enqueueTracked(
                    scheduler, flows.get(flow), prefix + '-' + index, cost, acceptedHandles, payloads);
        }
    }

    private static JobHandle enqueueTracked(
            SfqdScheduler<String, String, Payload> scheduler,
            FlowHandle flow,
            String jobId,
            long cost,
            Set<JobHandle> acceptedHandles,
            Map<JobHandle, Payload> payloads) {
        Payload payload = new Payload();
        JobHandle handle = accepted(scheduler.enqueue(flow, jobId, payload, cost));
        assertTrue(acceptedHandles.add(handle));
        payloads.put(handle, payload);
        return handle;
    }

    private static void drainAndComplete(
            SfqdScheduler<String, String, Payload> scheduler,
            Set<JobHandle> acceptedHandles,
            Map<JobHandle, Payload> payloads) {
        Set<JobHandle> dispatchedHandles = new HashSet<>();
        while (scheduler.snapshot().queuedJobs() > 0) {
            List<Dispatch<String, String, Payload>> batch =
                    scheduler.dispatchUpTo(scheduler.snapshot().depth());
            assertDispatchIdentity(batch, acceptedHandles, payloads, dispatchedHandles);
            for (Dispatch<String, String, Payload> dispatch : batch) {
                assertEquals(CompletionResult.COMPLETED, scheduler.complete(dispatch.jobHandle()));
            }
        }
    }

    private static int exactCeiling(BigInteger numerator, BigInteger denominator) {
        BigInteger[] quotientAndRemainder = numerator.divideAndRemainder(denominator);
        return quotientAndRemainder[0]
                .add(quotientAndRemainder[1].signum() == 0 ? BigInteger.ZERO : BigInteger.ONE)
                .intValueExact();
    }

    private static void assertDispatchIdentity(
            List<Dispatch<String, String, Payload>> dispatches,
            Set<JobHandle> acceptedHandles,
            Map<JobHandle, Payload> payloads,
            Set<JobHandle> dispatchedHandles) {
        for (Dispatch<String, String, Payload> dispatch : dispatches) {
            assertTrue(acceptedHandles.contains(dispatch.jobHandle()));
            assertTrue(dispatchedHandles.add(dispatch.jobHandle()));
            assertSame(payloads.get(dispatch.jobHandle()), dispatch.payload());
        }
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private static final class Payload {
    }

    private record ExpectedJob(int flowIndex, String flowId, String jobId) {
    }
}
