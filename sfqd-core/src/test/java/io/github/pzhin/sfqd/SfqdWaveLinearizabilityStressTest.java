package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SfqdWaveLinearizabilityStressTest {
    private static final int FRONTIER_LIMIT = 10_000;

    @Test
    @Timeout(120)
    void randomizedTwoAndThreeActorWavesMatchTheReferenceModel()
            throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
        for (int depth : List.of(1, 2, 3)) {
            for (long seed : List.of(0x51F0D1L, 0xBAD5EEDL, 0xC0FFEE42L)) {
                runRandomScenario(depth, seed);
            }
        }
    }

    @Test
    void twoActorPermutationFilterPreservesARealTimeEdge() {
        Observation first = new Observation(0, Command.SNAPSHOT, null, 1L, 2L, "first");
        Observation second = new Observation(1, Command.SNAPSHOT, null, 3L, 4L, "second");

        List<List<Observation>> permutations = legalPermutations(List.of(first, second));

        assertEquals(1, permutations.size());
        assertEquals(List.of(first, second), permutations.getFirst());
    }

    @Test
    void threeOverlappingActorsExposeAllSixPermutations() {
        Observation first = new Observation(0, Command.REGISTER, null, 1L, 6L, "first");
        Observation second = new Observation(1, Command.ENQUEUE, null, 2L, 5L, "second");
        Observation third = new Observation(2, Command.DISPATCH, null, 3L, 4L, "third");

        assertEquals(6, legalPermutations(List.of(first, second, third)).size());
    }

    @Test
    void postWaveSnapshotIsHardAfterEveryActor() {
        Observation first = new Observation(0, Command.ENQUEUE, null, 1L, 4L, "first");
        Observation second = new Observation(1, Command.DISPATCH, null, 2L, 3L, "second");
        Observation snapshot = new Observation(-1, Command.SNAPSHOT, null, 5L, 6L, "snapshot");

        List<List<Observation>> permutations = legalPermutations(List.of(first, second, snapshot));

        assertEquals(2, permutations.size());
        assertTrue(permutations.stream().allMatch(order -> order.getLast() == snapshot));
    }

    @Test
    void replayRejectsAReusedAcceptedJobCapability() {
        OwnerToken owner = new OwnerToken();
        FlowHandle flow = new FlowHandle(owner, 1L);
        JobHandle reusedJob = new JobHandle(owner, 1L);
        Operation register = Operation.register("flow", 1L);
        Operation first = Operation.enqueue(flow, "first", new Payload(1), 1L);
        Operation second = Operation.enqueue(flow, "second", new Payload(2), 1L);
        List<Observation> trace = List.of(
                new Observation(0, Command.REGISTER, register, 1L, 2L,
                        new RegisterFlowResult.Registered(flow)),
                new Observation(0, Command.ENQUEUE, first, 3L, 4L,
                        new EnqueueResult.Accepted(reusedJob)),
                new Observation(0, Command.ENQUEUE, second, 5L, 6L,
                        new EnqueueResult.Accepted(reusedJob)));

        assertEquals(null, replay(
                new SchedulerConfig(1, 2, 2), trace, Map.of(flow, 1), Map.of(reusedJob, 1)));
    }

    @Test
    void replayRejectsAReusedRegisteredFlowCapability() {
        FlowHandle reusedFlow = new FlowHandle(new OwnerToken(), 1L);
        Operation first = Operation.register("first", 1L);
        Operation close = Operation.close(reusedFlow);
        Operation second = Operation.register("second", 1L);
        List<Observation> trace = List.of(
                new Observation(0, Command.REGISTER, first, 1L, 2L,
                        new RegisterFlowResult.Registered(reusedFlow)),
                new Observation(0, Command.CLOSE, close, 3L, 4L, CloseFlowResult.CLOSED),
                new Observation(0, Command.REGISTER, second, 5L, 6L,
                        new RegisterFlowResult.Registered(reusedFlow)));

        assertEquals(null, replay(
                new SchedulerConfig(1, 2, 2), trace, Map.of(reusedFlow, 1), Map.of()));
    }

    private static void runRandomScenario(int depth, long seed)
            throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
        SchedulerConfig config = new SchedulerConfig(depth, 8, 16);
        SfqdScheduler<String, String, Payload> scheduler = new SfqdScheduler<>(config);
        Random random = new Random(seed ^ depth);
        List<FlowHandle> flows = new ArrayList<>();
        List<JobHandle> jobs = new ArrayList<>();
        Map<FlowHandle, Integer> flowLogical = new HashMap<>();
        Map<JobHandle, Integer> jobLogical = new HashMap<>();
        List<List<Observation>> frontier = List.of(List.of());
        List<List<Observation>> observedWaves = new ArrayList<>();
        Set<FlowHandle> seenFlowHandles = new HashSet<>();
        Set<JobHandle> seenJobHandles = new HashSet<>();
        Set<JobHandle> dispatched = new HashSet<>();
        Set<JobHandle> running = new HashSet<>();
        Set<JobHandle> cancelled = new HashSet<>();
        Set<JobHandle> completed = new HashSet<>();
        Set<Command> exercised = EnumSet.noneOf(Command.class);
        AtomicLong markers = new AtomicLong();
        int nextFlow = 1;
        int nextJob = 1;
        long acceptedCount = 0L;
        try (ExecutorService executor = Executors.newFixedThreadPool(3)) {
            for (int wave = 0; wave < 40; wave++) {
                int actors = wave % 2 == 0 ? 2 : 3;
                List<Invocation> invocations = new ArrayList<>();
                for (int actor = 0; actor < actors; actor++) {
                    Operation operation = generateOperation(
                            wave, actor, depth, random, flows, jobs, running, cancelled, completed);
                    exercised.add(operation.command());
                    invocations.add(operation.invocation(scheduler));
                }
                List<Observation> observations = new ArrayList<>(RaceWave.run(executor, markers, invocations));
                observations.add(RaceWave.snapshotObservation(scheduler, markers));
                observedWaves.add(List.copyOf(observations));
                for (Observation observation : observations) {
                    if (observation.result() instanceof RegisterFlowResult.Registered registered) {
                        FlowHandle handle = registered.flowHandle();
                        String diagnostic = failure(
                                seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                                "reused flow handle");
                        assertTrue(seenFlowHandles.add(handle), diagnostic);
                        assertEquals(null, flowLogical.putIfAbsent(handle, nextFlow++), diagnostic);
                        flows.add(handle);
                    } else if (observation.result() instanceof EnqueueResult.Accepted accepted) {
                        JobHandle handle = accepted.jobHandle();
                        String diagnostic = failure(
                                seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                                "reused job handle");
                        assertTrue(seenJobHandles.add(handle), diagnostic);
                        assertEquals(null, jobLogical.putIfAbsent(handle, nextJob++), diagnostic);
                        jobs.add(handle);
                        acceptedCount++;
                    } else if (observation.result() instanceof List<?> batch) {
                        for (Object item : batch) {
                            JobHandle handle = ((Dispatch<?, ?, ?>) item).jobHandle();
                            String diagnostic = failure(
                                    seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                                    "duplicate or cancelled dispatch");
                            assertTrue(dispatched.add(handle), diagnostic);
                            assertTrue(!cancelled.contains(handle), diagnostic);
                            running.add(handle);
                        }
                    } else if (observation.result() == CancelResult.CANCELLED) {
                        Operation operation = (Operation) observation.descriptor();
                        String diagnostic = failure(
                                seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                                "duplicate successful cancellation");
                        assertTrue(cancelled.add(operation.job()), diagnostic);
                        assertTrue(!dispatched.contains(operation.job()), diagnostic);
                    } else if (observation.result() == CompletionResult.COMPLETED) {
                        Operation operation = (Operation) observation.descriptor();
                        String diagnostic = failure(
                                seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                                "duplicate successful completion");
                        assertTrue(completed.add(operation.job()), diagnostic);
                    }
                }
                running.removeAll(completed);
                running.removeAll(cancelled);
                Map<String, List<Observation>> next = new LinkedHashMap<>();
                for (List<Observation> prior : frontier) {
                    for (List<Observation> order : legalPermutations(observations)) {
                        List<Observation> trace = new ArrayList<>(prior);
                        trace.addAll(order);
                        String fingerprint = replay(config, trace, flowLogical, jobLogical);
                        if (fingerprint != null) {
                            next.putIfAbsent(fingerprint, List.copyOf(trace));
                        }
                    }
                }
                assertTrue(!next.isEmpty(), failure(
                        seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                        "no reference linearization"));
                assertTrue(next.size() <= FRONTIER_LIMIT, failure(
                        seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical,
                        "frontier cap exceeded: " + next.size()));
                frontier = List.copyOf(next.values());
                SchedulerSnapshot snapshot = assertInstanceOf(
                        SchedulerSnapshot.class, observations.getLast().result());
                String diagnostic = failure(
                        seed, depth, wave, observedWaves, frontier, flowLogical, jobLogical, "invariant");
                assertTrue(cancelled.stream().noneMatch(dispatched::contains), diagnostic);
                assertTrue(dispatched.containsAll(completed), diagnostic);
                assertEquals(acceptedCount, snapshot.acceptedTotal(), diagnostic);
                assertEquals(snapshot.acceptedTotal(), snapshot.queuedJobs() + snapshot.runningJobs()
                        + snapshot.cancelledTotal() + snapshot.completedTotal(), diagnostic);
                assertEquals(
                        snapshot.dispatchedTotal(), snapshot.runningJobs() + snapshot.completedTotal(), diagnostic);
                assertEquals(snapshot.depth(), snapshot.runningJobs() + snapshot.freeSlots(), diagnostic);
                assertTrue(snapshot.registeredFlows() <= config.maxFlows(), diagnostic);
                assertTrue(snapshot.queuedJobs() + snapshot.runningJobs() <= config.maxLiveJobs(), diagnostic);
            }
        }
        assertEquals(EnumSet.allOf(Command.class), exercised,
                "fixed stress schedule must exercise the complete public operation vocabulary");
        assertTrue(!cancelled.isEmpty(), "fixed prefix must exercise successful cancellation");
        assertTrue(!completed.isEmpty(), "fixed prefix must exercise successful completion");
        assertTrue(observedWaves.stream().flatMap(List::stream).anyMatch(observation ->
                        observation.result() == CancelResult.NOT_LIVE
                                || observation.result() == CompletionResult.NOT_LIVE),
                "fixed prefix must exercise a duplicate terminal NOT_LIVE outcome");
    }

    private static Operation generateOperation(
            int wave,
            int actor,
            int depth,
            Random random,
            List<FlowHandle> flows,
            List<JobHandle> jobs,
            Set<JobHandle> running,
            Set<JobHandle> cancelled,
            Set<JobHandle> completed) {
        if (wave == 0) {
            return Operation.register("prefix-flow-" + actor, 1L + actor);
        }
        if (wave == 1) {
            return actor < 2
                    ? Operation.enqueue(flows.get(actor), "prefix-job-" + actor, new Payload(actor), 1L)
                    : Operation.snapshot();
        }
        if (wave == 2) {
            return actor == 0 ? Operation.dispatch(1) : Operation.snapshot();
        }
        if (wave == 3) {
            if (actor == 0) {
                return Operation.complete(running.iterator().next());
            }
            if (actor == 1) {
                JobHandle queued = jobs.stream()
                        .filter(job -> !running.contains(job))
                        .findFirst().orElseThrow();
                return Operation.cancel(queued);
            }
            return Operation.snapshot();
        }
        if (wave == 4) {
            return actor == 0
                    ? Operation.complete(completed.iterator().next())
                    : Operation.cancel(cancelled.iterator().next());
        }
        int selector = Math.floorMod(wave * 3 + actor + random.nextInt(7), 7);
        return switch (selector) {
            case 0 -> Operation.register("flow-" + random.nextInt(12), 1L + random.nextInt(5));
            case 1 -> flows.isEmpty() ? Operation.snapshot()
                    : Operation.close(flows.get(random.nextInt(flows.size())));
            case 2 -> flows.isEmpty() ? Operation.register("flow-" + random.nextInt(12), 1L + random.nextInt(5))
                    : Operation.enqueue(
                            flows.get(random.nextInt(flows.size())),
                            "job-" + random.nextInt(20),
                            new Payload(wave * 3 + actor),
                            1L + random.nextInt(9));
            case 3 -> jobs.isEmpty() ? Operation.snapshot()
                    : Operation.cancel(jobs.get(random.nextInt(jobs.size())));
            case 4 -> Operation.dispatch(random.nextInt(depth + 1));
            case 5 -> jobs.isEmpty() ? Operation.snapshot()
                    : Operation.complete(jobs.get(random.nextInt(jobs.size())));
            case 6 -> Operation.snapshot();
            default -> throw new AssertionError("unreachable operation");
        };
    }

    private static String failure(
            long seed,
            int depth,
            int wave,
            List<List<Observation>> observedWaves,
            List<List<Observation>> frontier,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs,
            String problem) {
        return problem + "; seed=" + seed + ", depth=" + depth + ", wave=" + wave
                + ", observedWaves=" + renderWaves(observedWaves, flows, jobs)
                + ", frontierSize=" + frontier.size()
                + ", representativeSerialTraces=" + frontier.stream().limit(3)
                        .map(trace -> renderTrace(trace, flows, jobs)).toList();
    }

    private static String renderWaves(
            List<List<Observation>> waves,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs) {
        List<String> rendered = new ArrayList<>();
        for (int wave = 0; wave < waves.size(); wave++) {
            rendered.add("wave-" + wave + '=' + renderTrace(waves.get(wave), flows, jobs));
        }
        return rendered.toString();
    }

    private static String renderTrace(
            List<Observation> trace,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs) {
        return trace.stream().map(observation -> renderObservation(observation, flows, jobs)).toList().toString();
    }

    private static String renderObservation(
            Observation observation,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs) {
        return "{actor=" + observation.actor()
                + ", command=" + observation.command()
                + ", descriptor=" + renderDescriptor(observation.descriptor(), flows, jobs)
                + ", invocation=" + observation.invocationMarker()
                + ", response=" + observation.responseMarker()
                + ", result=" + renderResult(observation.result(), flows, jobs) + '}';
    }

    private static String renderDescriptor(
            Object descriptor,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs) {
        if (!(descriptor instanceof Operation operation)) {
            return String.valueOf(descriptor);
        }
        return "Operation[id=" + operation.identifier()
                + ", flow=" + flowToken(operation.flow(), flows)
                + ", job=" + jobToken(operation.job(), jobs)
                + ", payload=" + payloadToken(operation.payload())
                + ", number=" + operation.number() + ']';
    }

    private static String renderResult(
            Object result,
            Map<FlowHandle, Integer> flows,
            Map<JobHandle, Integer> jobs) {
        if (result instanceof RegisterFlowResult.Registered registered) {
            return "Registered[" + flowToken(registered.flowHandle(), flows) + ']';
        }
        if (result instanceof EnqueueResult.Accepted accepted) {
            return "Accepted[" + jobToken(accepted.jobHandle(), jobs) + ']';
        }
        if (result instanceof List<?> list) {
            return list.stream().map(item -> {
                Dispatch<?, ?, ?> dispatch = (Dispatch<?, ?, ?>) item;
                return "Dispatch[jobHandle=" + jobToken(dispatch.jobHandle(), jobs)
                        + ", jobId=" + dispatch.jobId()
                        + ", flowId=" + dispatch.flowId()
                        + ", payload=" + payloadToken(dispatch.payload())
                        + ", cost=" + dispatch.cost() + ']';
            }).toList().toString();
        }
        return String.valueOf(result);
    }

    private static String flowToken(FlowHandle handle, Map<FlowHandle, Integer> flows) {
        return handle == null ? "-" : "F" + flows.getOrDefault(handle, -1);
    }

    private static String jobToken(JobHandle handle, Map<JobHandle, Integer> jobs) {
        return handle == null ? "-" : "J" + jobs.getOrDefault(handle, -1);
    }

    private static String payloadToken(Object payload) {
        return payload instanceof Payload value ? "P" + value.identity() : String.valueOf(payload);
    }

    private static List<List<Observation>> legalPermutations(List<Observation> observations) {
        List<List<Observation>> result = new ArrayList<>();
        permute(observations, new boolean[observations.size()], new ArrayList<>(), result);
        return result.stream().filter(SfqdWaveLinearizabilityStressTest::respectsHardEdges).toList();
    }

    private static void permute(
            List<Observation> source,
            boolean[] used,
            List<Observation> current,
            List<List<Observation>> result) {
        if (current.size() == source.size()) {
            result.add(List.copyOf(current));
            return;
        }
        for (int index = 0; index < source.size(); index++) {
            if (!used[index]) {
                used[index] = true;
                current.add(source.get(index));
                permute(source, used, current, result);
                current.removeLast();
                used[index] = false;
            }
        }
    }

    private static boolean respectsHardEdges(List<Observation> order) {
        for (int predecessor = 0; predecessor < order.size(); predecessor++) {
            for (int successor = 0; successor < order.size(); successor++) {
                if (order.get(predecessor).responseMarker() < order.get(successor).invocationMarker()
                        && predecessor > successor) {
                    return false;
                }
            }
        }
        return true;
    }

    private enum Command {
        REGISTER,
        CLOSE,
        ENQUEUE,
        CANCEL,
        DISPATCH,
        COMPLETE,
        SNAPSHOT
    }

    private static String replay(
            SchedulerConfig config,
            List<Observation> trace,
            Map<FlowHandle, Integer> flowLogical,
            Map<JobHandle, Integer> jobLogical) {
        ReplayModel model = new ReplayModel(config, flowLogical, jobLogical);
        for (Observation observation : trace) {
            if (!model.apply(observation)) {
                return null;
            }
        }
        return model.fingerprint();
    }

    private record Operation(
            Command command,
            String identifier,
            FlowHandle flow,
            JobHandle job,
            Payload payload,
            long number) {
        static Operation register(String identifier, long weight) {
            return new Operation(Command.REGISTER, identifier, null, null, null, weight);
        }

        static Operation close(FlowHandle flow) {
            return new Operation(Command.CLOSE, null, flow, null, null, 0L);
        }

        static Operation enqueue(FlowHandle flow, String identifier, Payload payload, long cost) {
            return new Operation(Command.ENQUEUE, identifier, flow, null, payload, cost);
        }

        static Operation cancel(JobHandle job) {
            return new Operation(Command.CANCEL, null, null, job, null, 0L);
        }

        static Operation dispatch(int capacity) {
            return new Operation(Command.DISPATCH, null, null, null, null, capacity);
        }

        static Operation complete(JobHandle job) {
            return new Operation(Command.COMPLETE, null, null, job, null, 0L);
        }

        static Operation snapshot() {
            return new Operation(Command.SNAPSHOT, null, null, null, null, 0L);
        }

        Invocation invocation(SfqdScheduler<String, String, Payload> scheduler) {
            return new Invocation(command, this, () -> switch (command) {
                case REGISTER -> scheduler.registerFlow(identifier, number);
                case CLOSE -> scheduler.closeFlow(flow);
                case ENQUEUE -> scheduler.enqueue(flow, identifier, payload, number);
                case CANCEL -> scheduler.cancel(job);
                case DISPATCH -> scheduler.capacityAvailable((int) number);
                case COMPLETE -> scheduler.complete(job);
                case SNAPSHOT -> scheduler.snapshot();
            });
        }
    }

    private static final class ReplayModel {
        private final ReferenceScheduler<String, String, Payload> reference;
        private final Map<FlowHandle, Integer> flowLogical;
        private final Map<JobHandle, Integer> jobLogical;
        private final Map<FlowHandle, FlowHandle> flowMapping = new HashMap<>();
        private final Map<JobHandle, JobHandle> jobMapping = new HashMap<>();
        private final Map<Integer, FlowFacts> flows = new HashMap<>();
        private final Map<Integer, JobFacts> jobs = new HashMap<>();
        private long flowSequence;
        private long jobSequence;

        private ReplayModel(
                SchedulerConfig config,
                Map<FlowHandle, Integer> flowLogical,
                Map<JobHandle, Integer> jobLogical) {
            reference = new ReferenceScheduler<>(config);
            this.flowLogical = flowLogical;
            this.jobLogical = jobLogical;
        }

        private boolean apply(Observation observation) {
            Operation operation = (Operation) observation.descriptor();
            boolean matches = switch (operation.command()) {
                case REGISTER -> register(operation, observation.result());
                case CLOSE -> close(operation, observation.result());
                case ENQUEUE -> enqueue(operation, observation.result());
                case CANCEL -> cancel(operation, observation.result());
                case DISPATCH -> dispatch(operation, observation.result());
                case COMPLETE -> complete(operation, observation.result());
                case SNAPSHOT -> reference.snapshot().equals(observation.result());
            };
            if (matches && reference.snapshot().queuedJobs() == 0 && reference.snapshot().runningJobs() == 0) {
                for (FlowFacts facts : flows.values()) {
                    if (facts.registered) {
                        facts.lastFinish = "0";
                    }
                }
            }
            return matches;
        }

        private boolean register(Operation operation, Object actual) {
            RegisterFlowResult expected = reference.registerFlow(operation.identifier(), operation.number());
            if (actual instanceof RegisterFlowResult.Registered actualRegistered) {
                if (!(expected instanceof RegisterFlowResult.Registered expectedRegistered)) {
                    return false;
                }
                Integer logical = flowLogical.get(actualRegistered.flowHandle());
                if (logical == null) {
                    return false;
                }
                FlowHandle previousMapping = flowMapping.putIfAbsent(
                        actualRegistered.flowHandle(), expectedRegistered.flowHandle());
                FlowFacts previousFacts = flows.putIfAbsent(logical, new FlowFacts(
                        ++flowSequence, operation.identifier(), operation.number()));
                return previousMapping == null && previousFacts == null;
            }
            return expected.equals(actual);
        }

        private boolean close(Operation operation, Object actual) {
            FlowHandle mapped = flowMapping.get(operation.flow());
            CloseFlowResult expected = mapped == null
                    ? CloseFlowResult.FLOW_NOT_REGISTERED : reference.closeFlow(mapped);
            if (!expected.equals(actual)) {
                return false;
            }
            if (expected == CloseFlowResult.CLOSED) {
                flows.get(flowLogical.get(operation.flow())).registered = false;
            }
            return true;
        }

        private boolean enqueue(Operation operation, Object actual) {
            FlowHandle mapped = flowMapping.get(operation.flow());
            EnqueueResult expected = mapped == null
                    ? EnqueueResult.Rejected.FLOW_NOT_REGISTERED
                    : reference.enqueue(mapped, operation.identifier(), operation.payload(), operation.number());
            if (actual instanceof EnqueueResult.Accepted actualAccepted) {
                if (!(expected instanceof EnqueueResult.Accepted expectedAccepted)) {
                    return false;
                }
                Integer logical = jobLogical.get(actualAccepted.jobHandle());
                Integer logicalFlow = flowLogical.get(operation.flow());
                if (logical == null || logicalFlow == null) {
                    return false;
                }
                ReferenceScheduler.QueuedState<String, String, Payload> state =
                        reference.queuedState(expectedAccepted.jobHandle());
                JobHandle previousMapping = jobMapping.putIfAbsent(
                        actualAccepted.jobHandle(), expectedAccepted.jobHandle());
                JobFacts previousFacts = jobs.putIfAbsent(logical, new JobFacts(
                        ++jobSequence,
                        operation.identifier(),
                        logicalFlow,
                        operation.payload(),
                        operation.number(),
                        "Q",
                        state.start().toString(),
                        state.finish().toString()));
                if (previousMapping != null || previousFacts != null) {
                    return false;
                }
                flows.get(logicalFlow).lastFinish = state.finish().toString();
                return true;
            }
            return expected.equals(actual);
        }

        private boolean cancel(Operation operation, Object actual) {
            JobHandle mapped = jobMapping.get(operation.job());
            CancelResult expected = mapped == null ? CancelResult.NOT_LIVE : reference.cancel(mapped);
            if (!expected.equals(actual)) {
                return false;
            }
            if (expected == CancelResult.CANCELLED) {
                jobs.get(jobLogical.get(operation.job())).state = "T";
            }
            return true;
        }

        private boolean dispatch(Operation operation, Object actualObject) {
            List<Dispatch<String, String, Payload>> expected =
                    reference.capacityAvailable((int) operation.number());
            if (!(actualObject instanceof List<?> actual) || expected.size() != actual.size()) {
                return false;
            }
            for (int index = 0; index < expected.size(); index++) {
                if (!(actual.get(index) instanceof Dispatch<?, ?, ?> actualJob)) {
                    return false;
                }
                Dispatch<String, String, Payload> expectedJob = expected.get(index);
                if (!expectedJob.jobHandle().equals(jobMapping.get(actualJob.jobHandle()))
                        || !expectedJob.jobId().equals(actualJob.jobId())
                        || !expectedJob.flowId().equals(actualJob.flowId())
                        || expectedJob.payload() != actualJob.payload()
                        || expectedJob.cost() != actualJob.cost()) {
                    return false;
                }
                jobs.get(jobLogical.get(actualJob.jobHandle())).state = "R";
            }
            return true;
        }

        private boolean complete(Operation operation, Object actual) {
            JobHandle mapped = jobMapping.get(operation.job());
            CompletionResult expected = mapped == null
                    ? CompletionResult.NOT_LIVE : reference.complete(mapped);
            if (!expected.equals(actual)) {
                return false;
            }
            if (expected == CompletionResult.COMPLETED) {
                jobs.get(jobLogical.get(operation.job())).state = "T";
            }
            return true;
        }

        private String fingerprint() {
            StringBuilder result = new StringBuilder(reference.snapshot().toString())
                    .append("|V=").append(reference.virtualTime());
            flows.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                    result.append("|F").append(entry.getKey()).append('=').append(entry.getValue()));
            jobs.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                    result.append("|J").append(entry.getKey()).append('=').append(entry.getValue()));
            result.append("|Q=");
            for (JobHandle queued : reference.queuedHandles()) {
                result.append(logicalJob(queued)).append(',');
            }
            return result.toString();
        }

        private int logicalJob(JobHandle referenceHandle) {
            for (Map.Entry<JobHandle, JobHandle> entry : jobMapping.entrySet()) {
                if (entry.getValue().equals(referenceHandle)) {
                    return jobLogical.get(entry.getKey());
                }
            }
            throw new AssertionError("reference job lacks a logical mapping");
        }
    }

    private static final class FlowFacts {
        private final long sequence;
        private final String identifier;
        private final long weight;
        private boolean registered = true;
        private String lastFinish = "0";

        private FlowFacts(long sequence, String identifier, long weight) {
            this.sequence = sequence;
            this.identifier = identifier;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return sequence + ":" + identifier + ':' + weight + ':' + registered + ':' + lastFinish;
        }
    }

    private static final class JobFacts {
        private final long sequence;
        private final String identifier;
        private final int flow;
        private final Payload payload;
        private final long cost;
        private String state;
        private final String start;
        private final String finish;

        private JobFacts(
                long sequence,
                String identifier,
                int flow,
                Payload payload,
                long cost,
                String state,
                String start,
                String finish) {
            this.sequence = sequence;
            this.identifier = identifier;
            this.flow = flow;
            this.payload = payload;
            this.cost = cost;
            this.state = state;
            this.start = start;
            this.finish = finish;
        }

        @Override
        public String toString() {
            return sequence + ":" + identifier + ':' + flow + ':' + payload + ':' + cost + ':'
                    + state + ':' + start + ':' + finish;
        }
    }

    private record Invocation(Command command, Object descriptor, Callable<Object> action) {
    }

    private record Observation(
            int actor,
            Command command,
            Object descriptor,
            long invocationMarker,
            long responseMarker,
            Object result) {
    }

    private static final class RaceWave {
        private RaceWave() {
        }

        private static List<Observation> run(
                ExecutorService executor,
                AtomicLong markers,
                List<Invocation> invocations)
                throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
            CyclicBarrier barrier = new CyclicBarrier(invocations.size() + 1);
            List<Future<Observation>> futures = new ArrayList<>();
            for (int actor = 0; actor < invocations.size(); actor++) {
                int actorIndex = actor;
                Invocation invocation = invocations.get(actor);
                futures.add(executor.submit(() -> {
                    barrier.await();
                    long invocationMarker = markers.incrementAndGet();
                    Object result = invocation.action().call();
                    long responseMarker = markers.incrementAndGet();
                    return new Observation(
                            actorIndex,
                            invocation.command(),
                            invocation.descriptor(),
                            invocationMarker,
                            responseMarker,
                            result);
                }));
            }
            barrier.await(5L, TimeUnit.SECONDS);
            List<Observation> observations = new ArrayList<>();
            for (Future<Observation> future : futures) {
                observations.add(future.get(5L, TimeUnit.SECONDS));
            }
            return List.copyOf(observations);
        }

        private static Observation snapshotObservation(
                SfqdScheduler<String, String, Payload> scheduler, AtomicLong markers) {
            long invocation = markers.incrementAndGet();
            SchedulerSnapshot snapshot = scheduler.snapshot();
            long response = markers.incrementAndGet();
            return new Observation(-1, Command.SNAPSHOT, Operation.snapshot(), invocation, response, snapshot);
        }
    }

    private record Payload(int identity) {
    }
}
