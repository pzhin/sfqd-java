package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SfqdLongRunNumericAccumulationTest {
    private static final int DEPTH = 2;
    private static final int FLOW_COUNT = 60;
    private static final int ROUNDS = 128;
    private static final int JOBS_PER_VISIT = 2;
    private static final int ORDER_PROBE_JOBS_PER_ROUND = 4;
    private static final int PERSISTENT_TAG_BITS = 4096;
    private static final int NUMERIC_STRESS_FLOOR_BITS = 3500;

    @Test
    void publicOperationsPreserveExactOrderAcrossLongFractionalAccumulationNearNumericBudget() {
        SchedulerConfig config = new SchedulerConfig(DEPTH, FLOW_COUNT + 1, 5);
        ReferenceScheduler<String, Long, Payload> reference = new ReferenceScheduler<>(config);
        SfqdScheduler<String, Long, Payload> production = new SfqdScheduler<>(config);
        List<FlowPair> flows = registerFractionalFlows(reference, production);
        Set<JobHandle> referenceAccepted = new HashSet<>();
        Set<JobHandle> productionAccepted = new HashSet<>();

        long nextJobId = 1L;
        FlowPair anchor = registerPair(reference, production, "anchor", 1L);
        JobPair anchorJob = recordUnique(
                enqueuePair(reference, production, anchor, nextJobId++, 1L),
                referenceAccepted,
                productionAccepted);
        DispatchPair anchorDispatch = dispatchOne(reference, production, 0L);
        assertEquals(anchorJob, anchorDispatch.jobs.get(0));

        int maximumReferenceDenominatorBits = 0;
        long dispatchEvent = 1L;
        for (int round = 0; round < ROUNDS; round++) {
            for (int flowIndex = 0; flowIndex < flows.size(); flowIndex++) {
                FlowPair flow = flows.get(flowIndex);
                JobPair first = recordUnique(
                        enqueuePair(reference, production, flow, nextJobId++, 1L),
                        referenceAccepted,
                        productionAccepted);
                JobPair second = recordUnique(
                        enqueuePair(reference, production, flow, nextJobId++, 1L),
                        referenceAccepted,
                        productionAccepted);

                DispatchPair firstDispatch = dispatchOne(reference, production, dispatchEvent++);
                assertEquals(first, firstDispatch.jobs.get(0));
                completePair(reference, production, first);

                DispatchPair secondDispatch = dispatchOne(reference, production, dispatchEvent++);
                assertEquals(second, secondDispatch.jobs.get(0));
                completePair(reference, production, second);

                maximumReferenceDenominatorBits = Math.max(
                        maximumReferenceDenominatorBits,
                        reference.virtualTime().denominator().bitLength());
                assertEquals(reference.snapshot(), production.snapshot(),
                        "aggregate drift after round=" + round + ", flowIndex=" + flowIndex);
            }

            FlowPair lowerWeight = flows.get(0);
            FlowPair higherWeight = flows.get(1);
            JobPair lowerFirst = recordUnique(
                    enqueuePair(reference, production, lowerWeight, nextJobId++, 1L),
                    referenceAccepted,
                    productionAccepted);
            JobPair lowerSecond = recordUnique(
                    enqueuePair(reference, production, lowerWeight, nextJobId++, 1L),
                    referenceAccepted,
                    productionAccepted);
            JobPair higherFirst = recordUnique(
                    enqueuePair(reference, production, higherWeight, nextJobId++, 1L),
                    referenceAccepted,
                    productionAccepted);
            JobPair higherSecond = recordUnique(
                    enqueuePair(reference, production, higherWeight, nextJobId++, 1L),
                    referenceAccepted,
                    productionAccepted);
            List<JobPair> exactProbeOrder = List.of(lowerFirst, higherFirst, higherSecond, lowerSecond);
            for (JobPair expected : exactProbeOrder) {
                assertEquals(expected, dispatchOne(reference, production, dispatchEvent++).jobs.get(0),
                        "cross-flow exact-order drift in round " + round);
                completePair(reference, production, expected);
            }
            assertEquals(reference.snapshot(), production.snapshot(),
                    "aggregate drift after cross-flow order probe in round " + round);
        }

        assertEquals(CompletionResult.COMPLETED, reference.complete(anchorJob.referenceHandle));
        assertEquals(CompletionResult.COMPLETED, production.complete(anchorJob.productionHandle));
        assertTrue(maximumReferenceDenominatorBits > NUMERIC_STRESS_FLOOR_BITS,
                "reference accumulation did not approach the production persistent budget: "
                        + maximumReferenceDenominatorBits);
        assertTrue(maximumReferenceDenominatorBits <= PERSISTENT_TAG_BITS,
                "test trace exceeded the documented production persistent budget: "
                        + maximumReferenceDenominatorBits);

        long expectedJobs = 1L + (long) ROUNDS
                * (FLOW_COUNT * JOBS_PER_VISIT + ORDER_PROBE_JOBS_PER_ROUND);
        SchedulerSnapshot expected = new SchedulerSnapshot(
                DEPTH,
                FLOW_COUNT + 1,
                5,
                FLOW_COUNT + 1,
                0,
                0,
                DEPTH,
                0,
                0,
                expectedJobs,
                expectedJobs,
                0L,
                expectedJobs);
        assertEquals(expected, reference.snapshot());
        assertEquals(expected, production.snapshot());
        assertEquals(expectedJobs, referenceAccepted.size());
        assertEquals(expectedJobs, productionAccepted.size());
        assertEquals(nextJobId, expectedJobs + 1L);
    }

    private static List<FlowPair> registerFractionalFlows(
            ReferenceScheduler<String, Long, Payload> reference,
            SfqdScheduler<String, Long, Payload> production) {
        List<FlowPair> flows = new ArrayList<>(FLOW_COUNT);
        BigInteger nextWeight = BigInteger.ONE.shiftLeft(60).nextProbablePrime();
        for (int index = 0; index < FLOW_COUNT; index++) {
            long weight = nextWeight.longValueExact();
            flows.add(registerPair(reference, production, "fractional-" + index, weight));
            nextWeight = nextWeight.nextProbablePrime();
        }
        return List.copyOf(flows);
    }

    private static FlowPair registerPair(
            ReferenceScheduler<String, Long, Payload> reference,
            SfqdScheduler<String, Long, Payload> production,
            String flowId,
            long weight) {
        RegisterFlowResult.Registered expected = assertInstanceOf(
                RegisterFlowResult.Registered.class, reference.registerFlow(flowId, weight));
        RegisterFlowResult.Registered actual = assertInstanceOf(
                RegisterFlowResult.Registered.class, production.registerFlow(flowId, weight));
        return new FlowPair(expected.flowHandle(), actual.flowHandle());
    }

    private static JobPair enqueuePair(
            ReferenceScheduler<String, Long, Payload> reference,
            SfqdScheduler<String, Long, Payload> production,
            FlowPair flow,
            long jobId,
            long cost) {
        Payload payload = new Payload(jobId);
        EnqueueResult.Accepted expectedAccepted = assertInstanceOf(
                EnqueueResult.Accepted.class,
                reference.enqueue(flow.referenceHandle, jobId, payload, cost),
                "reference admission failed for job " + jobId);
        EnqueueResult.Accepted actualAccepted = assertInstanceOf(
                EnqueueResult.Accepted.class,
                production.enqueue(flow.productionHandle, jobId, payload, cost),
                "production admission failed for job " + jobId);
        return new JobPair(expectedAccepted.jobHandle(), actualAccepted.jobHandle(), jobId, payload, cost);
    }

    private static DispatchPair dispatchOne(
            ReferenceScheduler<String, Long, Payload> reference,
            SfqdScheduler<String, Long, Payload> production,
            long event) {
        List<Dispatch<String, Long, Payload>> expected = reference.dispatchUpTo(1);
        List<Dispatch<String, Long, Payload>> actual = production.dispatchUpTo(1);
        assertEquals(1, expected.size(), "reference failed to dispatch at event " + event);
        assertEquals(1, actual.size(), "production failed to dispatch at event " + event);
        Dispatch<String, Long, Payload> expectedDispatch = expected.get(0);
        Dispatch<String, Long, Payload> actualDispatch = actual.get(0);
        assertEquals(expectedDispatch.jobId(), actualDispatch.jobId(), "job-order drift at event " + event);
        assertEquals(expectedDispatch.flowId(), actualDispatch.flowId(), "flow-order drift at event " + event);
        assertSame(expectedDispatch.payload(), actualDispatch.payload(), "payload drift at event " + event);
        assertEquals(expectedDispatch.cost(), actualDispatch.cost(), "cost drift at event " + event);
        return new DispatchPair(List.of(new JobPair(
                expectedDispatch.jobHandle(),
                actualDispatch.jobHandle(),
                expectedDispatch.jobId(),
                expectedDispatch.payload(),
                expectedDispatch.cost())));
    }

    private static JobPair recordUnique(
            JobPair job,
            Set<JobHandle> referenceAccepted,
            Set<JobHandle> productionAccepted) {
        assertTrue(referenceAccepted.add(job.referenceHandle),
                "reference reused an accepted handle for job " + job.jobId);
        assertTrue(productionAccepted.add(job.productionHandle),
                "production reused an accepted handle for job " + job.jobId);
        return job;
    }

    private static void completePair(
            ReferenceScheduler<String, Long, Payload> reference,
            SfqdScheduler<String, Long, Payload> production,
            JobPair job) {
        assertEquals(CompletionResult.COMPLETED, reference.complete(job.referenceHandle));
        assertEquals(CompletionResult.COMPLETED, production.complete(job.productionHandle));
    }

    private record FlowPair(FlowHandle referenceHandle, FlowHandle productionHandle) {
    }

    private record JobPair(
            JobHandle referenceHandle,
            JobHandle productionHandle,
            long jobId,
            Payload payload,
            long cost) {
    }

    private record DispatchPair(List<JobPair> jobs) {
    }

    private record Payload(long jobId) {
    }
}
