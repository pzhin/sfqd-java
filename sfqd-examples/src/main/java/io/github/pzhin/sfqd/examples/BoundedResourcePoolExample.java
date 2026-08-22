package io.github.pzhin.sfqd.examples;

import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.util.ArrayDeque;
import java.util.Deque;

/** Runnable deterministic walkthrough of the bounded resource-pool lifecycle. */
public final class BoundedResourcePoolExample {
    private static final int RESOURCE_PARALLELISM = 2;
    private static final String REJECTED_TASK = "task-2";

    private BoundedResourcePoolExample() {
    }

    /**
     * Runs the integration walkthrough and fails if any lifecycle invariant is violated.
     *
     * @param arguments ignored command-line arguments
     */
    public static void main(String[] arguments) {
        SfqdScheduler<String, String, String> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(RESOURCE_PARALLELISM, 1, 4));
        FlowHandle flow = registered(scheduler.registerFlow("tenant", 1L));
        enqueue(scheduler, flow, "job-1", "task-1", 1L);
        enqueue(scheduler, flow, "job-2", REJECTED_TASK, 1L);
        enqueue(scheduler, flow, "job-3", "task-3", 1L);

        DemonstrationPool pool = new DemonstrationPool();
        BoundedResourcePoolIntegration<String, String, String> integration =
                new BoundedResourcePoolIntegration<>(scheduler, pool, RESOURCE_PARALLELISM);

        System.out.println("scheduler depth=2, resource parallelism=2");
        try {
            integration.onResourcesAvailable(RESOURCE_PARALLELISM);
            throw new IllegalStateException("the configured submission failure did not occur");
        } catch (SubmissionFailure expected) {
            requireStateAfterSubmissionFailure(scheduler.snapshot());
            System.out.println("submission failure completed the irrevocably dispatched job");
        }

        pool.terminateNext("success");
        pool.terminateNext("task failure");

        enqueue(scheduler, flow, "job-4", "task-4", 1L);
        int unusedResources = integration.onResourcesAvailable(RESOURCE_PARALLELISM);
        if (unusedResources != 1) {
            throw new IllegalStateException("one of two reported resources must remain unused");
        }
        System.out.println("unused resources=" + unusedResources);
        pool.terminateNext("success");

        SchedulerSnapshot finalState = scheduler.snapshot();
        requireFinalState(finalState, pool);
        System.out.println("final queued=" + finalState.queuedJobs()
                + ", running=" + finalState.runningJobs()
                + ", dispatched=" + finalState.dispatchedTotal()
                + ", completed=" + finalState.completedTotal());
        System.out.println("BOUNDED_RESOURCE_POOL_EXAMPLE PASS");
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        if (result instanceof RegisterFlowResult.Registered registered) {
            return registered.flowHandle();
        }
        throw new IllegalStateException("flow registration rejected: " + result);
    }

    private static void enqueue(
            SfqdScheduler<String, String, String> scheduler,
            FlowHandle flow,
            String jobId,
            String task,
            long applicationSuppliedCost) {
        EnqueueResult result = scheduler.enqueue(flow, jobId, task, applicationSuppliedCost);
        if (!(result instanceof EnqueueResult.Accepted)) {
            throw new IllegalStateException("enqueue rejected: " + result);
        }
    }

    private static void requireStateAfterSubmissionFailure(SchedulerSnapshot state) {
        if (state.queuedJobs() != 1
                || state.runningJobs() != 1
                || state.dispatchedTotal() != 2L
                || state.completedTotal() != 1L) {
            throw new IllegalStateException("unexpected state after submission failure: " + state);
        }
    }

    private static void requireFinalState(SchedulerSnapshot state, DemonstrationPool pool) {
        if (state.queuedJobs() != 0
                || state.runningJobs() != 0
                || state.acceptedTotal() != 4L
                || state.dispatchedTotal() != 4L
                || state.completedTotal() != 4L
                || pool.hasPendingTasks()) {
            throw new IllegalStateException("lifecycle did not drain completely: " + state);
        }
    }

    private static final class DemonstrationPool
            implements BoundedResourcePoolIntegration.ResourcePool<String> {
        private final Deque<PendingTask> pending = new ArrayDeque<>();

        @Override
        public void execute(String task, Runnable onTerminal) {
            if (REJECTED_TASK.equals(task)) {
                System.out.println("pool rejected " + task + " before ownership transfer");
                throw new SubmissionFailure();
            }
            System.out.println("pool accepted " + task);
            pending.addLast(new PendingTask(task, onTerminal));
        }

        private void terminateNext(String outcome) {
            PendingTask task = pending.removeFirst();
            System.out.println("pool terminal " + task.name + ", outcome=" + outcome);
            task.onTerminal.run();
        }

        private boolean hasPendingTasks() {
            return !pending.isEmpty();
        }
    }

    private static final class PendingTask {
        private final String name;
        private final Runnable onTerminal;

        private PendingTask(String name, Runnable onTerminal) {
            this.name = name;
            this.onTerminal = onTerminal;
        }
    }

    private static final class SubmissionFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
