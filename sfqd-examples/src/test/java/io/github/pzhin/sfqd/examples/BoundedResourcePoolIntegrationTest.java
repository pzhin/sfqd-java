package io.github.pzhin.sfqd.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedResourcePoolIntegrationTest {
    @Test
    void requiresPoolParallelismToEqualSchedulerDepth() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2);
        RecordingPool<String> pool = new RecordingPool<>();

        assertThrows(IllegalArgumentException.class,
                () -> new BoundedResourcePoolIntegration<>(scheduler, pool, 1));

        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, 2);
        assertThrows(IllegalArgumentException.class, () -> integration.onResourcesAvailable(-1));
        assertThrows(IllegalArgumentException.class, () -> integration.onResourcesAvailable(3));
    }

    @Test
    void dispatchesOnlyRealCapacityAndLeavesSurplusResourcesFree() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2);
        FlowHandle flow = registered(scheduler);
        assertAccepted(scheduler.enqueue(flow, "job-1", "task-1", 5L));
        RecordingPool<String> pool = new RecordingPool<>();
        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, 2);

        int unused = integration.onResourcesAvailable(2);

        assertEquals(1, unused);
        assertEquals(List.of("task-1"), pool.tasks);
        assertEquals(1, scheduler.snapshot().runningJobs());
        assertEquals(0, integration.onResourcesAvailable(0));
    }

    @Test
    void terminalCallbackCompletesAndOffersTheReleasedResourceAgain() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2);
        FlowHandle flow = registered(scheduler);
        assertAccepted(scheduler.enqueue(flow, "job-1", "task-1", 1L));
        assertAccepted(scheduler.enqueue(flow, "job-2", "task-2", 1L));
        assertAccepted(scheduler.enqueue(flow, "job-3", "task-3", 1L));
        RecordingPool<String> pool = new RecordingPool<>();
        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, 2);

        assertEquals(0, integration.onResourcesAvailable(2));
        assertEquals(List.of("task-1", "task-2"), pool.tasks);
        assertEquals(2, scheduler.snapshot().runningJobs());

        pool.terminate(0);

        assertEquals(List.of("task-1", "task-2", "task-3"), pool.tasks);
        assertEquals(2, scheduler.snapshot().runningJobs());
        assertEquals(0, scheduler.snapshot().queuedJobs());
        assertEquals(3L, scheduler.snapshot().dispatchedTotal());
        assertEquals(1L, scheduler.snapshot().completedTotal());

        pool.terminate(1);
        pool.terminate(2);
        assertEquals(0, scheduler.snapshot().runningJobs());
        assertEquals(3L, scheduler.snapshot().completedTotal());
    }

    @Test
    void submissionFailureCompletesOnlyThePulledDispatchAndDoesNotRollItBack() {
        SfqdScheduler<String, String, String> scheduler = scheduler(2);
        FlowHandle flow = registered(scheduler);
        JobHandle rejected = accepted(scheduler.enqueue(flow, "job-1", "task-1", 1L));
        assertAccepted(scheduler.enqueue(flow, "job-2", "task-2", 1L));
        RecordingPool<String> pool = new RecordingPool<>();
        pool.rejectNext = true;
        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, 2);

        assertThrows(SubmissionFailure.class, () -> integration.onResourcesAvailable(2));

        SchedulerSnapshot afterFailure = scheduler.snapshot();
        assertEquals(0, afterFailure.runningJobs());
        assertEquals(1, afterFailure.queuedJobs());
        assertEquals(1L, afterFailure.dispatchedTotal());
        assertEquals(1L, afterFailure.completedTotal());
        assertEquals(CompletionResult.NOT_LIVE, scheduler.complete(rejected));

        assertEquals(1, integration.onResourcesAvailable(2));
        assertEquals(List.of("task-2"), pool.tasks);
        pool.terminate(0);
        assertEquals(0, scheduler.snapshot().runningJobs());
    }

    @Test
    void poolObservesTheJobAsRunningBeforeExecute() {
        SfqdScheduler<String, String, String> scheduler = scheduler(1);
        FlowHandle flow = registered(scheduler);
        assertAccepted(scheduler.enqueue(flow, "job-1", "task-1", 1L));
        List<Runnable> terminals = new ArrayList<>();
        BoundedResourcePoolIntegration.ResourcePool<String> pool = (task, onTerminal) -> {
            assertEquals("task-1", task);
            assertEquals(1, scheduler.snapshot().runningJobs());
            terminals.add(onTerminal);
        };
        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, 1);

        assertEquals(0, integration.onResourcesAvailable(1));
        terminals.getFirst().run();
        assertEquals(0, scheduler.snapshot().runningJobs());
    }

    private static SfqdScheduler<String, String, String> scheduler(int depth) {
        return new SfqdScheduler<>(new SchedulerConfig(depth, 2, 10));
    }

    private static FlowHandle registered(SfqdScheduler<String, String, String> scheduler) {
        return ((RegisterFlowResult.Registered) scheduler.registerFlow("flow", 1L)).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private static void assertAccepted(EnqueueResult result) {
        assertEquals(EnqueueResult.Accepted.class, result.getClass());
    }

    private static final class RecordingPool<T> implements BoundedResourcePoolIntegration.ResourcePool<T> {
        private final List<T> tasks = new ArrayList<>();
        private final List<Runnable> terminals = new ArrayList<>();
        private boolean rejectNext;

        @Override
        public void execute(T task, Runnable onTerminal) {
            if (rejectNext) {
                rejectNext = false;
                throw new SubmissionFailure();
            }
            tasks.add(task);
            terminals.add(onTerminal);
        }

        private void terminate(int index) {
            terminals.get(index).run();
        }
    }

    private static final class SubmissionFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
