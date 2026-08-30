package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

final class ReferenceLifecycleProperties {
    @Property(tries = 250)
    void oneFlowDispatchIsWorkConservingAndConservesLifecycleCounts(
            @ForAll @IntRange(min = 1, max = 16) int depth,
            @ForAll @IntRange(min = 1, max = 64) int jobs,
            @ForAll @IntRange(min = 0, max = 16) int requested,
            @ForAll("cancellationPolicies") CancellationAccounting policy) {
        ReferenceScheduler<String, Integer, Integer> model =
                new ReferenceScheduler<>(new SchedulerConfig(depth, 1, Math.max(depth, jobs), policy));
        FlowHandle flow = assertInstanceOf(
                RegisterFlowResult.Registered.class, model.registerFlow("flow", 1L)).flowHandle();
        for (int index = 0; index < jobs; index++) {
            assertInstanceOf(EnqueueResult.Accepted.class, model.enqueue(flow, index, index, 1L));
        }

        int permitted = Math.min(requested, depth);
        int expected = Math.min(permitted, jobs);
        assertEquals(expected, model.dispatchUpTo(permitted).size());
        SchedulerSnapshot snapshot = model.snapshot();
        assertEquals(jobs, snapshot.queuedJobs() + snapshot.runningJobs());
        assertEquals(jobs, snapshot.acceptedTotal());
        assertEquals(snapshot.runningJobs(), snapshot.dispatchedTotal());
        assertTrue(snapshot.runningJobs() <= depth);
        assertEquals(depth, snapshot.runningJobs() + snapshot.freeSlots());
    }

    @Property(tries = 200)
    void mixedCommandTraceConservesStateAfterEveryEvent(
            @ForAll("commandTraces") List<Integer> commands,
            @ForAll("cancellationPolicies") CancellationAccounting policy) {
        LifecycleHarness harness = new LifecycleHarness(policy);
        for (int command : commands) {
            harness.apply(command);
            harness.assertConservation();
        }
    }

    @Provide
    Arbitrary<List<Integer>> commandTraces() {
        return Arbitraries.integers().between(0, 15).list().ofMinSize(1).ofMaxSize(80)
                .map(randomCommands -> {
                    List<Integer> trace = new ArrayList<>(randomCommands.size() + 3);
                    trace.add(14);
                    trace.add(15);
                    trace.add(2);
                    trace.addAll(randomCommands);
                    return List.copyOf(trace);
                });
    }

    @Provide
    Arbitrary<CancellationAccounting> cancellationPolicies() {
        return Arbitraries.of(CancellationAccounting.values());
    }

    private enum JobPhase {
        QUEUED,
        RUNNING
    }

    private static final class JobState {
        private final int jobId;
        private final FlowHandle flowHandle;
        private JobPhase phase = JobPhase.QUEUED;

        private JobState(int jobId, FlowHandle flowHandle) {
            this.jobId = jobId;
            this.flowHandle = flowHandle;
        }
    }

    private static final class LifecycleHarness {
        private static final String FIRST_FLOW = "first";
        private static final String SECOND_FLOW = "second";

        private final ReferenceScheduler<String, Integer, Integer> model;
        private final FlowHandle foreignFlow = new FlowHandle(new OwnerToken(), 1L);
        private final JobHandle foreignJob = new JobHandle(new OwnerToken(), 1L);
        private final Map<String, FlowHandle> flows = new LinkedHashMap<>();
        private final Map<JobHandle, JobState> jobs = new LinkedHashMap<>();
        private final List<Integer> terminalIds = new ArrayList<>();
        private int nextJobId;
        private long accepted;
        private long dispatched;
        private long cancelled;
        private long completed;

        private LifecycleHarness(CancellationAccounting policy) {
            model = new ReferenceScheduler<>(new SchedulerConfig(2, 2, 8, policy));
        }

        private void apply(int command) {
            switch (command) {
                case 0 -> register(FIRST_FLOW);
                case 1 -> register(SECOND_FLOW);
                case 2 -> close(FIRST_FLOW);
                case 3 -> close(SECOND_FLOW);
                case 4 -> enqueueFresh(FIRST_FLOW);
                case 5 -> enqueueFresh(SECOND_FLOW);
                case 6 -> enqueueDuplicate();
                case 7 -> dispatch(1);
                case 8 -> dispatch(2);
                case 9 -> cancel(JobPhase.QUEUED);
                case 10 -> cancel(JobPhase.RUNNING);
                case 11 -> complete(JobPhase.RUNNING);
                case 12 -> complete(JobPhase.QUEUED);
                case 13 -> enqueueTerminalId();
                case 14 -> assertEquals(CancelResult.NOT_LIVE, model.cancel(foreignJob));
                case 15 -> assertEquals(CompletionResult.NOT_LIVE, model.complete(foreignJob));
                default -> throw new AssertionError("unknown generated command " + command);
            }
        }

        private void register(String flowId) {
            RegisterFlowResult result = model.registerFlow(flowId, flowId.equals(FIRST_FLOW) ? 1L : 2L);
            if (result instanceof RegisterFlowResult.Registered registered) {
                flows.put(flowId, registered.flowHandle());
            }
        }

        private void close(String flowId) {
            FlowHandle handle = flows.getOrDefault(flowId, foreignFlow);
            if (model.closeFlow(handle) == CloseFlowResult.CLOSED) {
                flows.remove(flowId);
            }
        }

        private void enqueueFresh(String flowId) {
            enqueue(flows.getOrDefault(flowId, foreignFlow), nextJobId++);
        }

        private void enqueueDuplicate() {
            Optional<JobState> live = jobs.values().stream().findFirst();
            if (live.isPresent()) {
                JobState job = live.orElseThrow();
                assertEquals(EnqueueResult.Rejected.DUPLICATE_LIVE_ID,
                        model.enqueue(job.flowHandle, job.jobId, job.jobId, 1L));
            } else {
                assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                        model.enqueue(foreignFlow, -1, -1, 1L));
            }
        }

        private void enqueueTerminalId() {
            if (terminalIds.isEmpty()) {
                assertEquals(EnqueueResult.Rejected.FLOW_NOT_REGISTERED,
                        model.enqueue(foreignFlow, -2, -2, 1L));
                return;
            }
            int jobId = terminalIds.get(terminalIds.size() - 1);
            FlowHandle flow = flows.values().stream().findFirst().orElse(foreignFlow);
            if (enqueue(flow, jobId)) {
                terminalIds.remove(terminalIds.size() - 1);
            }
        }

        private boolean enqueue(FlowHandle flow, int jobId) {
            EnqueueResult result = model.enqueue(flow, jobId, jobId, Math.floorMod(jobId, 5) + 1L);
            if (result instanceof EnqueueResult.Accepted admitted) {
                jobs.put(admitted.jobHandle(), new JobState(jobId, flow));
                accepted++;
                return true;
            }
            return false;
        }

        private void dispatch(int capacity) {
            List<Dispatch<String, Integer, Integer>> batch = model.dispatchUpTo(capacity);
            for (Dispatch<String, Integer, Integer> item : batch) {
                JobState job = jobs.get(item.jobHandle());
                assertNotNull(job);
                assertEquals(JobPhase.QUEUED, job.phase);
                job.phase = JobPhase.RUNNING;
                dispatched++;
            }
        }

        private void cancel(JobPhase target) {
            JobHandle handle = find(target).orElse(foreignJob);
            CancelResult result = model.cancel(handle);
            if (result == CancelResult.CANCELLED) {
                terminalize(handle, target);
                cancelled++;
            } else if (target == JobPhase.RUNNING && handle != foreignJob) {
                assertEquals(CancelResult.TOO_LATE_ALREADY_DISPATCHED, result);
            } else {
                assertEquals(CancelResult.NOT_LIVE, result);
            }
        }

        private void complete(JobPhase target) {
            JobHandle handle = find(target).orElse(foreignJob);
            CompletionResult result = model.complete(handle);
            if (result == CompletionResult.COMPLETED) {
                terminalize(handle, target);
                completed++;
            } else if (target == JobPhase.QUEUED && handle != foreignJob) {
                assertEquals(CompletionResult.NOT_DISPATCHED, result);
            } else {
                assertEquals(CompletionResult.NOT_LIVE, result);
            }
        }

        private Optional<JobHandle> find(JobPhase target) {
            return jobs.entrySet().stream()
                    .filter(entry -> entry.getValue().phase == target)
                    .map(Map.Entry::getKey)
                    .findFirst();
        }

        private void terminalize(JobHandle handle, JobPhase expectedPhase) {
            JobState removed = jobs.remove(handle);
            assertNotNull(removed);
            assertEquals(expectedPhase, removed.phase);
            terminalIds.add(removed.jobId);
        }

        private void assertConservation() {
            SchedulerSnapshot snapshot = model.snapshot();
            long queued = jobs.values().stream().filter(job -> job.phase == JobPhase.QUEUED).count();
            long running = jobs.values().stream().filter(job -> job.phase == JobPhase.RUNNING).count();
            long active = jobs.values().stream().map(job -> job.flowHandle).distinct().count();
            long backlogged = jobs.values().stream()
                    .filter(job -> job.phase == JobPhase.QUEUED)
                    .map(job -> job.flowHandle)
                    .distinct()
                    .count();

            assertEquals(flows.size(), snapshot.registeredFlows());
            assertEquals(queued, snapshot.queuedJobs());
            assertEquals(running, snapshot.runningJobs());
            assertEquals(2L - running, snapshot.freeSlots());
            assertEquals(active, snapshot.activeFlows());
            assertEquals(backlogged, snapshot.backloggedFlows());
            assertEquals(accepted, snapshot.acceptedTotal());
            assertEquals(dispatched, snapshot.dispatchedTotal());
            assertEquals(cancelled, snapshot.cancelledTotal());
            assertEquals(completed, snapshot.completedTotal());
            assertEquals(accepted, queued + running + cancelled + completed);
            assertEquals(dispatched, running + completed);
        }
    }
}
