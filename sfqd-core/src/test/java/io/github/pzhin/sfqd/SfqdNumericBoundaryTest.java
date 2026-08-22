package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class SfqdNumericBoundaryTest {
    @Test
    void canonicalRebaseTransformsAllStateAndMatchesLogicalReference()
            throws NumericLimitException, ReflectiveOperationException {
        SchedulerConfig config = new SchedulerConfig(2, 4, 8);
        SfqdScheduler<String, String, String> scheduler = new SfqdScheduler<>(config);
        ReferenceScheduler<String, String, String> reference = new ReferenceScheduler<>(config);
        FlowHandles productionFlows = registerFour(scheduler);
        FlowHandles referenceFlows = registerFour(reference);
        JobHandle running = accepted(scheduler.enqueue(productionFlows.anchor, "anchor", "anchor-p", 1L));
        JobHandle referenceRunning = accepted(reference.enqueue(referenceFlows.anchor, "anchor", "anchor-p", 1L));
        scheduler.dispatchUpTo(1);
        reference.dispatchUpTo(1);
        JobHandle a1 = accepted(scheduler.enqueue(productionFlows.first, "a1", "a1-p", 1L));
        JobHandle a2 = accepted(scheduler.enqueue(productionFlows.first, "a2", "a2-p", 1L));
        JobHandle b1 = accepted(scheduler.enqueue(productionFlows.second, "b1", "b1-p", 1L));
        reference.enqueue(referenceFlows.first, "a1", "a1-p", 1L);
        reference.enqueue(referenceFlows.first, "a2", "a2-p", 1L);
        reference.enqueue(referenceFlows.second, "b1", "b1-p", 1L);
        BigInteger baseInteger = BigInteger.ONE.shiftLeft(4095);
        ExactTag base = ExactTag.fromComponents(baseInteger, BigInteger.ONE);
        ExactTag targetFinish = ExactTag.fromComponents(
                BigInteger.ONE.shiftLeft(4096).subtract(BigInteger.ONE), BigInteger.ONE);
        NumericProbe.shiftQueuedAndFlowState(scheduler, base, productionFlows, targetFinish);
        NumericState before = NumericProbe.capture(scheduler);
        assertEquals(List.of(a1, b1, a2), before.queuedOrder);

        EnqueueResult result = scheduler.enqueue(productionFlows.target, "target", "target-p", 1L);

        EnqueueResult.Accepted acceptedResult = assertInstanceOf(EnqueueResult.Accepted.class, result);
        NumericState after = NumericProbe.capture(scheduler);
        assertEquals(ExactTag.zero(), after.virtualTime);
        assertEquals(before.lastJobSequence + 1L, after.lastJobSequence);
        assertEquals(before.accepted + 1L, after.accepted);
        assertEquals(before.dispatched, after.dispatched);
        assertEquals(before.cancelled, after.cancelled);
        assertEquals(before.completed, after.completed);
        assertEquals(before.lastFlowSequence, after.lastFlowSequence);
        assertRebasedBy(before, after, base, productionFlows.target);
        ExactTag rebasedTargetStart = ExactTag.fromComponents(baseInteger.subtract(BigInteger.ONE), BigInteger.ONE);
        assertEquals(new TagPair(rebasedTargetStart, base), after.queuedTags.get(acceptedResult.jobHandle()));
        assertEquals(List.of(a1, b1, a2, acceptedResult.jobHandle()), after.queuedOrder);
        assertEquals(before.backloggedHeads,
                after.backloggedHeads.subList(0, before.backloggedHeads.size()));
        for (Map.Entry<FlowHandle, List<JobHandle>> entry : before.flowQueues.entrySet()) {
            if (!entry.getKey().equals(productionFlows.target)) {
                assertEquals(entry.getValue(), after.flowQueues.get(entry.getKey()));
            }
        }
        for (Map.Entry<JobHandle, LinkPair> entry : before.links.entrySet()) {
            assertEquals(entry.getValue(), after.links.get(entry.getKey()));
        }
        assertEquals(new SchedulerSnapshot(2, 4, 8, 4, 4, 1, 1, 4, 3, 5L, 1L, 0L, 0L),
                after.snapshot);

        ExactRational referenceTargetStart = ExactRational.of(
                BigInteger.ONE.shiftLeft(4095).subtract(BigInteger.ONE), BigInteger.ONE);
        NumericProbe.setReferenceLastFinish(reference, referenceFlows.target, referenceTargetStart);
        reference.enqueue(referenceFlows.target, "target", "target-p", 1L);
        DispatchPair first = assertDispatches(reference.dispatchUpTo(1), scheduler.dispatchUpTo(1));
        assertEquals(reference.complete(referenceRunning), scheduler.complete(running));
        complete(reference, scheduler, first);
        DispatchPair middle = assertDispatches(reference.dispatchUpTo(2), scheduler.dispatchUpTo(2));
        complete(reference, scheduler, middle);
        DispatchPair last = assertDispatches(reference.dispatchUpTo(2), scheduler.dispatchUpTo(2));
        complete(reference, scheduler, last);
        assertEquals(reference.snapshot(), scheduler.snapshot());
    }

    @Test
    void failedRebaseLeavesEveryHiddenNumericAndOrderingFieldUnchanged()
            throws NumericLimitException, ReflectiveOperationException {
        RejectionFixture fixture = rejectionFixture(false);
        NumericState before = NumericProbe.capture(fixture.scheduler);
        assertTrue(before.queuedOrder.size() >= 3);
        assertEquals(before.queuedOrder.get(1), before.backloggedHeads.get(0));
        assertEquals(before.queuedOrder.get(0), before.backloggedHeads.get(1));

        assertEquals(EnqueueResult.Rejected.NUMERIC_LIMIT,
                fixture.scheduler.enqueue(fixture.target, "rejected", "rejected-p", Long.MAX_VALUE));

        assertEquals(before, NumericProbe.capture(fixture.scheduler));
    }

    @Test
    void sequenceExhaustionPrecedesNumericLimitForTheSameCandidate()
            throws NumericLimitException, ReflectiveOperationException {
        RejectionFixture fixture = rejectionFixture(true);
        NumericState before = NumericProbe.capture(fixture.scheduler);

        assertEquals(EnqueueResult.Rejected.SEQUENCE_EXHAUSTED,
                fixture.scheduler.enqueue(fixture.target, "both-limits", "p", Long.MAX_VALUE));

        assertEquals(before, NumericProbe.capture(fixture.scheduler));
    }

    @Test
    void flowAndJobSequencesFailClosedAtLongBoundary() throws ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 2, 2));
        NumericProbe.setLong(scheduler, "lastJobSequence", Long.MAX_VALUE - 1L);
        NumericProbe.setLong(scheduler, "lastFlowSequence", Long.MAX_VALUE - 1L);
        FlowHandle flow = registered(scheduler.registerFlow("last-flow", 1L));

        assertEquals(RegisterFlowResult.Rejected.FLOW_SEQUENCE_EXHAUSTED,
                scheduler.registerFlow("never-registered", 1L));
        assertInstanceOf(EnqueueResult.Accepted.class, scheduler.enqueue(flow, "last-job", "p", 1L));
        assertEquals(EnqueueResult.Rejected.SEQUENCE_EXHAUSTED,
                scheduler.enqueue(flow, "never-accepted", "p", 1L));
        assertEquals(new SchedulerSnapshot(1, 2, 2, 1, 1, 0, 1, 1, 1, 1L, 0L, 0L, 0L),
                scheduler.snapshot());
    }

    @Test
    void runtimeArtifactExposesOnlyOnePublicConfigurationConstructorAndNoTestingMethods() {
        assertTrue(List.of(SfqdScheduler.class.getDeclaredMethods()).stream()
                .noneMatch(method -> method.getName().contains("ForTesting")));
        assertEquals(1, SfqdScheduler.class.getDeclaredConstructors().length);
        assertTrue(Modifier.isPublic(SfqdScheduler.class.getDeclaredConstructors()[0].getModifiers()));
        assertEquals(List.of(SchedulerConfig.class),
                List.of(SfqdScheduler.class.getDeclaredConstructors()[0].getParameterTypes()));
        assertFalse(SfqdScheduler.class.getDeclaredConstructors()[0].isSynthetic());
    }

    private static RejectionFixture rejectionFixture(boolean exhaustSequence)
            throws NumericLimitException, ReflectiveOperationException {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(2, 4, 8));
        FlowHandles flows = registerFour(scheduler);
        scheduler.enqueue(flows.anchor, "anchor", "anchor-p", 1L);
        scheduler.dispatchUpTo(1);
        scheduler.enqueue(flows.first, "a1", "a1-p", 1L);
        scheduler.enqueue(flows.first, "a2", "a2-p", 1L);
        scheduler.enqueue(flows.second, "b1", "b1-p", 1L);
        BigInteger denominator = BigInteger.ONE.shiftLeft(4095);
        ExactTag larger = ExactTag.fromComponents(BigInteger.ONE, denominator);
        ExactTag smaller = ExactTag.fromComponents(BigInteger.ONE, denominator.add(BigInteger.ONE));
        NumericProbe.seedRejectedRebase(scheduler, smaller, larger, flows);
        NumericProbe.reverseBackloggedIndex(scheduler);
        if (exhaustSequence) {
            NumericProbe.setLong(scheduler, "lastJobSequence", Long.MAX_VALUE);
        }
        return new RejectionFixture(scheduler, flows.target);
    }

    private static void assertRebasedBy(
            NumericState before,
            NumericState after,
            ExactTag base,
            FlowHandle enqueuedFlow)
            throws NumericLimitException {
        for (Map.Entry<FlowHandle, ExactTag> entry : before.flowLastFinishes.entrySet()) {
            ExactTag expected = entry.getValue().compareExact(base) < 0
                    ? ExactTag.zero() : entry.getValue().subtractNonNegative(base);
            if (entry.getKey().equals(enqueuedFlow)) {
                expected = expected.add(ExactTag.fromCostAndWeight(1L, 1L));
            }
            assertEquals(expected, after.flowLastFinishes.get(entry.getKey()));
        }
        for (Map.Entry<JobHandle, TagPair> entry : before.queuedTags.entrySet()) {
            TagPair expected = new TagPair(
                    entry.getValue().start.subtractNonNegative(base),
                    entry.getValue().finish.subtractNonNegative(base));
            assertEquals(expected, after.queuedTags.get(entry.getKey()));
        }
        assertEquals(before.queuedOrder, after.queuedOrder.subList(0, before.queuedOrder.size()));
    }

    private static DispatchPair assertDispatches(
            List<Dispatch<String, String, String>> expected,
            List<Dispatch<String, String, String>> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).jobId(), actual.get(index).jobId());
            assertEquals(expected.get(index).flowId(), actual.get(index).flowId());
            assertSame(expected.get(index).payload(), actual.get(index).payload());
            assertEquals(expected.get(index).cost(), actual.get(index).cost());
        }
        return new DispatchPair(expected, actual);
    }

    private static void complete(
            ReferenceScheduler<String, String, String> reference,
            SfqdScheduler<String, String, String> scheduler,
            DispatchPair pair) {
        for (int index = 0; index < pair.reference.size(); index++) {
            assertEquals(
                    reference.complete(pair.reference.get(index).jobHandle()),
                    scheduler.complete(pair.production.get(index).jobHandle()));
        }
    }

    private static FlowHandles registerFour(SfqdScheduler<String, String, String> scheduler) {
        return new FlowHandles(
                registered(scheduler.registerFlow("anchor", 1L)),
                registered(scheduler.registerFlow("a", 1L)),
                registered(scheduler.registerFlow("b", 1L)),
                registered(scheduler.registerFlow("target", 1L)));
    }

    private static FlowHandles registerFour(ReferenceScheduler<String, String, String> scheduler) {
        return new FlowHandles(
                registered(scheduler.registerFlow("anchor", 1L)),
                registered(scheduler.registerFlow("a", 1L)),
                registered(scheduler.registerFlow("b", 1L)),
                registered(scheduler.registerFlow("target", 1L)));
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private record FlowHandles(FlowHandle anchor, FlowHandle first, FlowHandle second, FlowHandle target) {
    }

    private record RejectionFixture(SfqdScheduler<String, String, String> scheduler, FlowHandle target) {
    }

    private record DispatchPair(
            List<Dispatch<String, String, String>> reference,
            List<Dispatch<String, String, String>> production) {
    }

    private record TagPair(ExactTag start, ExactTag finish) {
    }

    private record LinkPair(JobHandle previous, JobHandle next, FlowHandle flow) {
    }

    private record IdentityEntry(IdentityRef key, IdentityRef value) {
    }

    private record FlowRecord(
            IdentityRef handle,
            IdentityRef flowId,
            long weight,
            ExactTag lastFinish,
            int queuedCount,
            int runningCount,
            IdentityRef head,
            IdentityRef tail) {
    }

    private record QueuedRecord(
            IdentityRef handle,
            IdentityRef jobId,
            IdentityRef flow,
            IdentityRef payload,
            long cost,
            ExactTag start,
            ExactTag finish,
            long sequence,
            IdentityRef previous,
            IdentityRef next) {
    }

    private record RunningRecord(
            IdentityRef handle,
            IdentityRef jobId,
            IdentityRef flowHandle,
            long cost) {
    }

    private record NumericState(
            ExactTag virtualTime,
            Set<IdentityEntry> registeredById,
            Set<IdentityEntry> registeredFlows,
            Map<FlowHandle, FlowRecord> flowStates,
            Set<IdentityEntry> liveById,
            Set<IdentityEntry> queuedIndex,
            Map<JobHandle, QueuedRecord> queuedRecords,
            Set<IdentityEntry> runningIndex,
            Map<JobHandle, RunningRecord> runningRecords,
            Map<FlowHandle, ExactTag> flowLastFinishes,
            Map<JobHandle, TagPair> queuedTags,
            List<JobHandle> queuedOrder,
            Map<FlowHandle, List<JobHandle>> flowQueues,
            Map<JobHandle, LinkPair> links,
            List<JobHandle> backloggedHeads,
            List<IdentityRef> backloggedHeadNodes,
            List<Integer> backloggedComparatorSigns,
            Object backloggedComparatorIdentity,
            long lastJobSequence,
            long lastFlowSequence,
            long accepted,
            long dispatched,
            long cancelled,
            long completed,
            int activeFlowCount,
            SchedulerSnapshot snapshot) {
    }

    private static final class IdentityRef {
        private final Object reference;

        private IdentityRef(Object reference) {
            this.reference = reference;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof IdentityRef identity && reference == identity.reference;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(reference);
        }

        @Override
        public String toString() {
            return reference.getClass().getName() + '@' + Integer.toHexString(hashCode());
        }
    }

    static final class NumericProbe {
        private NumericProbe() {
        }

        static Object captureDeepState(SfqdScheduler<?, ?, ?> scheduler)
                throws ReflectiveOperationException {
            return capture(scheduler);
        }

        static void setLongForDeepState(Object target, String name, long value)
                throws ReflectiveOperationException {
            setLong(target, name, value);
        }

        private static void shiftQueuedAndFlowState(
                SfqdScheduler<?, ?, ?> scheduler,
                ExactTag base,
                FlowHandles flows,
                ExactTag targetFinish) throws ReflectiveOperationException {
            set(scheduler, "virtualTime", base);
            Map<FlowHandle, Object> registered = map(scheduler, "registeredFlows");
            set(registered.get(flows.anchor), "lastFinish", base);
            shiftFlow(registered.get(flows.first), base);
            shiftFlow(registered.get(flows.second), base);
            set(registered.get(flows.target), "lastFinish", targetFinish);
        }

        private static void seedRejectedRebase(
                SfqdScheduler<?, ?, ?> scheduler,
                ExactTag virtualTime,
                ExactTag targetFinish,
                FlowHandles flows) throws ReflectiveOperationException, NumericLimitException {
            set(scheduler, "virtualTime", virtualTime);
            Map<FlowHandle, Object> registered = map(scheduler, "registeredFlows");
            set(registered.get(flows.anchor), "lastFinish", virtualTime);
            seedFlow(registered.get(flows.first), ExactTag.fromCostAndWeight(3L, 1L));
            seedFlow(registered.get(flows.second), ExactTag.fromCostAndWeight(2L, 1L));
            set(registered.get(flows.target), "lastFinish", targetFinish);
        }

        @SuppressWarnings("unchecked") // Test fixture deliberately replaces the private index with reversed ordering.
        private static void reverseBackloggedIndex(SfqdScheduler<?, ?, ?> scheduler)
                throws ReflectiveOperationException {
            NavigableSet<Object> original = (NavigableSet<Object>) get(scheduler, "backlogged");
            Comparator<Object> reversed = original.comparator().reversed();
            NavigableSet<Object> replacement = new TreeSet<>(reversed);
            replacement.addAll(original);
            set(scheduler, "backlogged", replacement);
        }

        private static void shiftFlow(Object flow, ExactTag base) throws ReflectiveOperationException {
            Object job = get(flow, "head");
            while (job != null) {
                set(job, "start", addUnchecked(base, (ExactTag) get(job, "start")));
                set(job, "finish", addUnchecked(base, (ExactTag) get(job, "finish")));
                job = get(job, "next");
            }
            set(flow, "lastFinish", addUnchecked(base, (ExactTag) get(flow, "lastFinish")));
        }

        private static void seedFlow(Object flow, ExactTag lastFinish)
                throws ReflectiveOperationException, NumericLimitException {
            Object job = get(flow, "head");
            long tag = 1L;
            while (job != null) {
                set(job, "start", ExactTag.fromCostAndWeight(tag, 1L));
                set(job, "finish", ExactTag.fromCostAndWeight(tag + 1L, 1L));
                tag++;
                job = get(job, "next");
            }
            set(flow, "lastFinish", lastFinish);
        }

        private static NumericState capture(SfqdScheduler<?, ?, ?> scheduler)
                throws ReflectiveOperationException {
            Set<IdentityEntry> registeredIndex = captureIdentityIndex(map(scheduler, "registeredById"));
            Set<IdentityEntry> liveIndex = captureIdentityIndex(map(scheduler, "liveById"));
            Map<FlowHandle, Object> registeredFlows = map(scheduler, "registeredFlows");
            Set<IdentityEntry> registeredFlowIndex = captureIdentityIndex(registeredFlows);
            Map<FlowHandle, FlowRecord> flowRecords = new LinkedHashMap<>();
            Map<FlowHandle, ExactTag> finishes = new LinkedHashMap<>();
            for (Map.Entry<FlowHandle, Object> entry : registeredFlows.entrySet()) {
                Object flow = entry.getValue();
                ExactTag finish = (ExactTag) get(flow, "lastFinish");
                finishes.put(entry.getKey(), finish);
                flowRecords.put(entry.getKey(), new FlowRecord(
                        identity(get(flow, "handle")),
                        identity(get(flow, "flowId")),
                        (long) get(flow, "weight"),
                        finish,
                        (int) get(flow, "queuedCount"),
                        (int) get(flow, "runningCount"),
                        identityOrNull(get(flow, "head")),
                        identityOrNull(get(flow, "tail"))));
            }
            Map<JobHandle, Object> queued = map(scheduler, "queued");
            Set<IdentityEntry> queuedIndex = captureIdentityIndex(queued);
            Map<JobHandle, QueuedRecord> queuedRecords = new LinkedHashMap<>();
            Map<JobHandle, TagPair> tags = new LinkedHashMap<>();
            List<Object> jobs = new ArrayList<>(queued.values());
            jobs.sort(Comparator
                    .comparing((Object job) -> (ExactTag) getUnchecked(job, "start"), NumericProbe::compareUnchecked)
                    .thenComparingLong(job -> (long) getUnchecked(job, "sequence")));
            List<JobHandle> order = new ArrayList<>(jobs.size());
            for (Object job : jobs) {
                JobHandle handle = (JobHandle) get(job, "handle");
                order.add(handle);
                ExactTag start = (ExactTag) get(job, "start");
                ExactTag finish = (ExactTag) get(job, "finish");
                tags.put(handle, new TagPair(start, finish));
                queuedRecords.put(handle, new QueuedRecord(
                        identity(handle),
                        identity(get(job, "jobId")),
                        identity(get(job, "flow")),
                        identity(get(job, "payload")),
                        (long) get(job, "cost"),
                        start,
                        finish,
                        (long) get(job, "sequence"),
                        identityOrNull(get(job, "previous")),
                        identityOrNull(get(job, "next"))));
            }
            Map<JobHandle, Object> running = map(scheduler, "running");
            Set<IdentityEntry> runningIndex = captureIdentityIndex(running);
            Map<JobHandle, RunningRecord> runningRecords = captureRunningRecords(running);
            Map<FlowHandle, List<JobHandle>> flowQueues = new LinkedHashMap<>();
            Map<JobHandle, LinkPair> links = new LinkedHashMap<>();
            for (Map.Entry<FlowHandle, Object> entry : NumericProbe.<FlowHandle, Object>map(
                    scheduler, "registeredFlows").entrySet()) {
                flowQueues.put(entry.getKey(), captureFlowQueue(entry.getKey(), entry.getValue(), links));
            }
            NavigableSet<Object> backlogged = navigableSet(scheduler, "backlogged");
            List<Object> heads = new ArrayList<>(backlogged);
            List<JobHandle> headHandles = heads.stream()
                    .map(head -> (JobHandle) getUnchecked(head, "handle"))
                    .toList();
            List<IdentityRef> headNodes = heads.stream().map(NumericProbe::identity).toList();
            Set<JobHandle> expectedHeads = flowQueues.values().stream()
                    .filter(queue -> !queue.isEmpty())
                    .map(queue -> queue.get(0))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            if (!Set.copyOf(headHandles).equals(expectedHeads)) {
                throw new AssertionError("backlogged index is not exactly the set of flow heads");
            }
            List<Integer> comparatorSigns = comparatorSigns(backlogged.comparator(), heads);
            return new NumericState(
                    (ExactTag) get(scheduler, "virtualTime"), registeredIndex, registeredFlowIndex,
                    Map.copyOf(flowRecords), liveIndex, queuedIndex, Map.copyOf(queuedRecords),
                    runningIndex, Map.copyOf(runningRecords),
                    Map.copyOf(finishes), Map.copyOf(tags),
                    List.copyOf(order), Map.copyOf(flowQueues), Map.copyOf(links), headHandles, headNodes,
                    comparatorSigns, backlogged.comparator(), (long) get(scheduler, "lastJobSequence"),
                    (long) get(scheduler, "lastFlowSequence"),
                    (long) get(scheduler, "accepted"), (long) get(scheduler, "dispatched"),
                    (long) get(scheduler, "cancelled"), (long) get(scheduler, "completed"),
                    (int) get(scheduler, "activeFlowCount"),
                    scheduler.snapshot());
        }

        private static Set<IdentityEntry> captureIdentityIndex(Map<?, ?> index) {
            java.util.HashSet<IdentityEntry> entries = new java.util.HashSet<>();
            for (Map.Entry<?, ?> entry : index.entrySet()) {
                entries.add(new IdentityEntry(identity(entry.getKey()), identity(entry.getValue())));
            }
            return Set.copyOf(entries);
        }

        private static Map<JobHandle, RunningRecord> captureRunningRecords(Map<JobHandle, Object> running)
                throws ReflectiveOperationException {
            Map<JobHandle, RunningRecord> records = new LinkedHashMap<>();
            for (Map.Entry<JobHandle, Object> entry : running.entrySet()) {
                Object job = entry.getValue();
                records.put(entry.getKey(), new RunningRecord(
                        identity(entry.getKey()),
                        identity(get(job, "jobId")),
                        identity(get(job, "flowHandle")),
                        (long) get(job, "cost")));
            }
            return records;
        }

        private static IdentityRef identity(Object reference) {
            return new IdentityRef(reference);
        }

        private static IdentityRef identityOrNull(Object reference) {
            return reference == null ? null : identity(reference);
        }

        private static List<JobHandle> captureFlowQueue(
                FlowHandle flowHandle,
                Object flow,
                Map<JobHandle, LinkPair> links) throws ReflectiveOperationException {
            List<JobHandle> order = new ArrayList<>();
            IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
            Object current = get(flow, "head");
            Object previous = null;
            while (current != null) {
                if (visited.put(current, Boolean.TRUE) != null || get(current, "previous") != previous) {
                    throw new AssertionError("per-flow links are not a single acyclic chain");
                }
                JobHandle handle = (JobHandle) get(current, "handle");
                Object next = get(current, "next");
                links.put(handle, new LinkPair(
                        previous == null ? null : (JobHandle) get(previous, "handle"),
                        next == null ? null : (JobHandle) get(next, "handle"),
                        flowHandle));
                order.add(handle);
                previous = current;
                current = next;
            }
            if (get(flow, "tail") != previous) {
                throw new AssertionError("flow tail does not terminate its linked queue");
            }
            if ((int) get(flow, "queuedCount") != order.size()) {
                throw new AssertionError("flow queuedCount does not match its linked queue");
            }
            return List.copyOf(order);
        }

        @SuppressWarnings("unchecked") // Comparator belongs to the reflected queued-node TreeSet.
        private static List<Integer> comparatorSigns(Comparator<?> comparator, List<Object> heads) {
            Comparator<Object> typed = (Comparator<Object>) comparator;
            List<Integer> signs = new ArrayList<>();
            for (int left = 0; left < heads.size(); left++) {
                for (int right = 0; right < heads.size(); right++) {
                    signs.add(Integer.signum(typed.compare(heads.get(left), heads.get(right))));
                }
            }
            return List.copyOf(signs);
        }

        private static void setReferenceLastFinish(
                ReferenceScheduler<?, ?, ?> reference,
                FlowHandle handle,
                ExactRational finish) throws ReflectiveOperationException {
            set(map(reference, "registeredFlows").get(handle), "lastFinish", finish);
        }

        private static ExactTag addUnchecked(ExactTag first, ExactTag second) {
            try {
                return first.add(second);
            } catch (NumericLimitException failure) {
                throw new AssertionError("test fixture value must fit", failure);
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
                return get(target, name);
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError("test reflection failed", failure);
            }
        }

        private static Object get(Object target, String name) throws ReflectiveOperationException {
            return field(target, name).get(target);
        }

        private static void set(Object target, String name, Object value) throws ReflectiveOperationException {
            field(target, name).set(target, value);
        }

        private static void setLong(Object target, String name, long value) throws ReflectiveOperationException {
            field(target, name).setLong(target, value);
        }

        @SuppressWarnings("unchecked") // Test reflection preserves the declared map key/value types at each call.
        private static <K, V> Map<K, V> map(Object target, String name) throws ReflectiveOperationException {
            return (Map<K, V>) get(target, name);
        }

        @SuppressWarnings("unchecked") // Test reflection preserves the private NavigableSet element type.
        private static <E> NavigableSet<E> navigableSet(Object target, String name)
                throws ReflectiveOperationException {
            return (NavigableSet<E>) get(target, name);
        }

        private static Field field(Object target, String name) throws ReflectiveOperationException {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field;
        }
    }
}
