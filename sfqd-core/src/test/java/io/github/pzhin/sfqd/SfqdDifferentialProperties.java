package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

class SfqdDifferentialProperties {
    @Property(tries = 250)
    void boundedProductionMatchesUnboundedReferenceForEverySmallTrace(
            @ForAll("depths") int depth,
            @ForAll @Size(min = 50, max = 180) List<@IntRange(min = -10_000, max = 10_000) Integer> commands) {
        Harness harness = new Harness(depth);

        for (int event = 0; event < commands.size(); event++) {
            harness.apply(commands.get(event), event);
            assertEquals(harness.reference.snapshot(), harness.production.snapshot());
            harness.assertFlowSnapshotsEqual();
        }
    }

    @Example
    void depthOnePreservesSfQSequenceTiesAndCrossFlowStartOrdering() {
        SchedulerConfig config = new SchedulerConfig(1, 2, 4);
        ReferenceScheduler<String, String, Payload> reference = new ReferenceScheduler<>(config);
        SfqdScheduler<String, String, Payload> production = new SfqdScheduler<>(config);
        FlowHandle referenceA = registered(reference.registerFlow("a", 1L));
        FlowHandle productionA = registered(production.registerFlow("a", 1L));
        FlowHandle referenceB = registered(reference.registerFlow("b", 1L));
        FlowHandle productionB = registered(production.registerFlow("b", 1L));
        Payload a1 = new Payload(1);
        Payload a2 = new Payload(2);
        Payload b1 = new Payload(3);
        JobHandle referenceA1 = accepted(reference.enqueue(referenceA, "a1", a1, 5L));
        JobHandle productionA1 = accepted(production.enqueue(productionA, "a1", a1, 5L));
        reference.enqueue(referenceA, "a2", a2, 1L);
        production.enqueue(productionA, "a2", a2, 1L);
        reference.enqueue(referenceB, "b1", b1, 1L);
        production.enqueue(productionB, "b1", b1, 1L);

        assertDispatchIds(reference.dispatchUpTo(1), production.dispatchUpTo(1), List.of("a1"));
        assertEquals(reference.complete(referenceA1), production.complete(productionA1));
        assertDispatchIds(reference.dispatchUpTo(1), production.dispatchUpTo(1), List.of("b1"));
        assertEquals(reference.snapshot(), production.snapshot());
    }

    @Provide
    Arbitrary<Integer> depths() {
        return Arbitraries.of(1, 2, 3, 8);
    }

    private static final class Harness {
        private final SchedulerConfig config;
        private final ReferenceScheduler<String, String, Payload> reference;
        private final SfqdScheduler<String, String, Payload> production;
        private final List<FlowPair> flows = new ArrayList<>();
        private final List<JobPair> jobs = new ArrayList<>();
        private final Map<JobHandle, JobHandle> referenceToProduction = new HashMap<>();

        private Harness(int depth) {
            config = new SchedulerConfig(depth, 8, 24);
            reference = new ReferenceScheduler<>(config);
            production = new SfqdScheduler<>(config);
            ReferenceScheduler<String, String, Payload> foreignReference = new ReferenceScheduler<>(config);
            SfqdScheduler<String, String, Payload> foreignProduction = new SfqdScheduler<>(config);
            FlowHandle referenceFlow = registered(foreignReference.registerFlow("foreign", 1L));
            FlowHandle productionFlow = registered(foreignProduction.registerFlow("foreign", 1L));
            flows.add(new FlowPair(referenceFlow, productionFlow));
            JobHandle referenceJob = accepted(foreignReference.enqueue(
                    referenceFlow, "foreign-job", new Payload(-1), 1L));
            JobHandle productionJob = accepted(foreignProduction.enqueue(
                    productionFlow, "foreign-job", new Payload(-1), 1L));
            jobs.add(new JobPair(referenceJob, productionJob));
        }

        private void apply(int command, int event) {
            int operation = Math.floorMod(command, 7);
            int selector = Math.floorMod(command / 7, 97);
            switch (operation) {
                case 0 -> register(selector);
                case 1 -> close(selector);
                case 2 -> enqueue(selector, event);
                case 3 -> cancel(selector);
                case 4 -> dispatch(selector);
                case 5 -> complete(selector);
                case 6 -> assertEquals(reference.snapshot(), production.snapshot());
                default -> throw new AssertionError("unreachable operation");
            }
        }

        private void register(int selector) {
            String flowId = "flow-" + selector % 10;
            long weight = 1L + selector % 5;
            RegisterFlowResult expected = reference.registerFlow(flowId, weight);
            RegisterFlowResult actual = production.registerFlow(flowId, weight);
            if (expected instanceof RegisterFlowResult.Registered expectedRegistered) {
                RegisterFlowResult.Registered actualRegistered =
                        assertInstanceOf(RegisterFlowResult.Registered.class, actual);
                flows.add(new FlowPair(expectedRegistered.flowHandle(), actualRegistered.flowHandle()));
            } else {
                assertEquals(expected, actual);
            }
        }

        private void close(int selector) {
            FlowPair flow = flows.get(selector % flows.size());
            assertEquals(reference.closeFlow(flow.reference), production.closeFlow(flow.production));
        }

        private void enqueue(int selector, int event) {
            FlowPair flow = flows.get(selector % flows.size());
            String jobId = "job-" + selector % 13;
            Payload payload = new Payload(event);
            long cost = 1L + selector % 9;
            EnqueueResult expected = reference.enqueue(flow.reference, jobId, payload, cost);
            EnqueueResult actual = production.enqueue(flow.production, jobId, payload, cost);
            if (expected instanceof EnqueueResult.Accepted expectedAccepted) {
                EnqueueResult.Accepted actualAccepted = assertInstanceOf(EnqueueResult.Accepted.class, actual);
                JobPair pair = new JobPair(expectedAccepted.jobHandle(), actualAccepted.jobHandle());
                jobs.add(pair);
                referenceToProduction.put(pair.reference, pair.production);
            } else {
                assertEquals(expected, actual);
            }
        }

        private void cancel(int selector) {
            JobPair job = jobs.get(selector % jobs.size());
            assertEquals(reference.cancel(job.reference), production.cancel(job.production));
        }

        private void dispatch(int selector) {
            int capacity = selector % (config.depth() + 1);
            List<Dispatch<String, String, Payload>> expected = reference.dispatchUpTo(capacity);
            List<Dispatch<String, String, Payload>> actual = production.dispatchUpTo(capacity);
            assertEquals(expected.size(), actual.size());
            for (int index = 0; index < expected.size(); index++) {
                Dispatch<String, String, Payload> expectedJob = expected.get(index);
                Dispatch<String, String, Payload> actualJob = actual.get(index);
                assertEquals(referenceToProduction.get(expectedJob.jobHandle()), actualJob.jobHandle());
                assertEquals(expectedJob.jobId(), actualJob.jobId());
                assertEquals(expectedJob.flowId(), actualJob.flowId());
                assertSame(expectedJob.payload(), actualJob.payload());
                assertEquals(expectedJob.cost(), actualJob.cost());
            }
        }

        private void complete(int selector) {
            JobPair job = jobs.get(selector % jobs.size());
            assertEquals(reference.complete(job.reference), production.complete(job.production));
        }

        private void assertFlowSnapshotsEqual() {
            for (FlowPair flow : flows) {
                assertEquals(reference.snapshot(flow.reference), production.snapshot(flow.production));
            }
        }
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private static void assertDispatchIds(
            List<Dispatch<String, String, Payload>> expected,
            List<Dispatch<String, String, Payload>> actual,
            List<String> expectedIds) {
        assertEquals(expectedIds, expected.stream().map(Dispatch::jobId).toList());
        assertEquals(expectedIds, actual.stream().map(Dispatch::jobId).toList());
        assertSame(expected.get(0).payload(), actual.get(0).payload());
        assertEquals(expected.get(0).cost(), actual.get(0).cost());
    }

    private record FlowPair(FlowHandle reference, FlowHandle production) {
    }

    private record JobPair(JobHandle reference, JobHandle production) {
    }

    private record Payload(int event) {
    }
}
