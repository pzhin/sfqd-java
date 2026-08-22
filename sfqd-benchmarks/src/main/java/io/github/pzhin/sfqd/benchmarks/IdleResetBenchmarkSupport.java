package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.PAYLOAD;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCancelled;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCompleted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireRegistered;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.FlowKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Payload;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Public-API-only state construction and validation for global-idle-reset benchmarks. */
final class IdleResetBenchmarkSupport {
    private IdleResetBenchmarkSupport() {
    }

    enum TerminalOperation {
        COMPLETE,
        CANCEL
    }

    /** Reusable fixture with exactly one terminal live job at every measured boundary. */
    static final class TerminalFixture {
        private final int flowCount;
        private final int depth;
        private final boolean allTagged;
        private final TerminalOperation operation;
        private final SfqdScheduler<FlowKey, JobKey, Payload> scheduler;
        private final List<FlowHandle> flows;
        private final List<JobKey> jobs;
        private JobHandle terminalHandle;

        TerminalFixture(int flowCount, int depth, boolean allTagged, TerminalOperation operation) {
            if (flowCount < 1 || depth < 1) {
                throw new IllegalArgumentException("flowCount and depth must be positive");
            }
            this.flowCount = flowCount;
            this.depth = depth;
            this.allTagged = allTagged;
            this.operation = operation;
            this.scheduler = new SfqdScheduler<>(new SchedulerConfig(depth, flowCount, Math.max(depth, flowCount)));
            this.flows = new ArrayList<>(flowCount);
            this.jobs = new ArrayList<>(flowCount);
            for (int index = 0; index < flowCount; index++) {
                flows.add(requireRegistered(scheduler.registerFlow(new FlowKey(index), 1L)));
                jobs.add(new JobKey(index + 1L));
            }
            establishTerminalState();
        }

        CompletionResult completeLastRunning() {
            requireOperation(TerminalOperation.COMPLETE);
            return scheduler.complete(terminalHandle);
        }

        CancelResult cancelLastQueued() {
            requireOperation(TerminalOperation.CANCEL);
            return scheduler.cancel(terminalHandle);
        }

        void restoreAfterCompletion(CompletionResult result) {
            requireOperation(TerminalOperation.COMPLETE);
            requireCompleted(result);
            verifyIdle();
            establishTerminalState();
        }

        void restoreAfterCancellation(CancelResult result) {
            requireOperation(TerminalOperation.CANCEL);
            requireCancelled(result);
            verifyIdle();
            establishTerminalState();
        }

        void verifyPrepared() {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            int expectedQueued = operation == TerminalOperation.CANCEL ? 1 : 0;
            int expectedRunning = operation == TerminalOperation.COMPLETE ? 1 : 0;
            int expectedBacklogged = operation == TerminalOperation.CANCEL ? 1 : 0;
            if (snapshot.registeredFlows() != flowCount
                    || snapshot.queuedJobs() != expectedQueued
                    || snapshot.runningJobs() != expectedRunning
                    || snapshot.freeSlots() != depth - expectedRunning
                    || snapshot.activeFlows() != 1
                    || snapshot.backloggedFlows() != expectedBacklogged) {
                throw new IllegalStateException("terminal fixture shape diverged: " + snapshot);
            }
            if (operation == TerminalOperation.COMPLETE) {
                if (snapshot.cancelledTotal() != 0
                        || snapshot.acceptedTotal() != snapshot.completedTotal() + 1
                        || snapshot.dispatchedTotal() != snapshot.acceptedTotal()) {
                    throw new IllegalStateException("completion fixture counters diverged: " + snapshot);
                }
            } else if (snapshot.acceptedTotal()
                    != snapshot.completedTotal() + snapshot.cancelledTotal() + 1
                    || snapshot.dispatchedTotal() != snapshot.completedTotal()) {
                throw new IllegalStateException("cancellation fixture counters diverged: " + snapshot);
            }
        }

        private void establishTerminalState() {
            int taggedFlows = allTagged ? flowCount : 1;
            Set<JobHandle> liveHandles = new HashSet<>(taggedFlows * 2);
            for (int index = 0; index < taggedFlows; index++) {
                JobHandle handle = requireAccepted(scheduler.enqueue(flows.get(index), jobs.get(index), PAYLOAD, 1L));
                if (!liveHandles.add(handle)) {
                    throw new IllegalStateException("accepted job handles must be unique");
                }
            }
            for (int index = 1; index < taggedFlows; index++) {
                Dispatch<FlowKey, JobKey, Payload> dispatch = dispatchOne();
                if (!liveHandles.remove(dispatch.jobHandle())) {
                    throw new IllegalStateException("dispatched handle absent from public caller model");
                }
                requireCompleted(scheduler.complete(dispatch.jobHandle()));
                SchedulerSnapshot snapshot = scheduler.snapshot();
                int expectedLive = taggedFlows - index;
                if (snapshot.queuedJobs() != expectedLive
                        || snapshot.runningJobs() != 0
                        || snapshot.activeFlows() != expectedLive
                        || snapshot.backloggedFlows() != expectedLive) {
                    throw new IllegalStateException("all-tagged preparation shape diverged: " + snapshot);
                }
            }
            if (liveHandles.size() != 1) {
                throw new IllegalStateException("terminal preparation did not leave one live job");
            }
            terminalHandle = liveHandles.iterator().next();
            if (operation == TerminalOperation.COMPLETE) {
                Dispatch<FlowKey, JobKey, Payload> dispatch = dispatchOne();
                if (!dispatch.jobHandle().equals(terminalHandle)) {
                    throw new IllegalStateException("terminal completion dispatched an unexpected handle");
                }
            }
            verifyPrepared();
        }

        private Dispatch<FlowKey, JobKey, Payload> dispatchOne() {
            List<Dispatch<FlowKey, JobKey, Payload>> dispatches = scheduler.capacityAvailable(1);
            if (dispatches.size() != 1) {
                throw new IllegalStateException("terminal fixture requires one dispatch");
            }
            return dispatches.getFirst();
        }

        private void verifyIdle() {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            if (snapshot.registeredFlows() != flowCount
                    || snapshot.queuedJobs() != 0
                    || snapshot.runningJobs() != 0
                    || snapshot.freeSlots() != depth
                    || snapshot.activeFlows() != 0
                    || snapshot.backloggedFlows() != 0
                    || snapshot.acceptedTotal() != snapshot.completedTotal() + snapshot.cancelledTotal()
                    || snapshot.dispatchedTotal() != snapshot.completedTotal()) {
                throw new IllegalStateException("terminal operation did not reach global idle: " + snapshot);
            }
        }

        private void requireOperation(TerminalOperation expected) {
            if (operation != expected) {
                throw new IllegalStateException("fixture operation mismatch");
            }
        }
    }

    /** Reusable idle scheduler for first-busy-period admission and enqueue-cancel cycles. */
    static final class FirstBusyPeriodFixture {
        private final int flowCount;
        private final int depth;
        private final SfqdScheduler<FlowKey, JobKey, Payload> scheduler;
        private final FlowHandle flow;
        private final JobKey job = new JobKey(1L);

        FirstBusyPeriodFixture(int flowCount, int depth) {
            if (flowCount < 1 || depth < 1) {
                throw new IllegalArgumentException("flowCount and depth must be positive");
            }
            this.flowCount = flowCount;
            this.depth = depth;
            this.scheduler = new SfqdScheduler<>(new SchedulerConfig(depth, flowCount, depth));
            FlowHandle first = null;
            for (int index = 0; index < flowCount; index++) {
                FlowHandle registered = requireRegistered(scheduler.registerFlow(new FlowKey(index), 1L));
                if (index == 0) {
                    first = registered;
                }
            }
            this.flow = first;
            verifyIdle();
        }

        EnqueueResult enqueueFirstBusyPeriod() {
            return scheduler.enqueue(flow, job, PAYLOAD, 1L);
        }

        void restoreAfterEnqueue(EnqueueResult result) {
            JobHandle handle = requireAccepted(result);
            verifyOneQueued();
            requireCancelled(scheduler.cancel(handle));
            verifyIdle();
        }

        int enqueueCancelCycle() {
            JobHandle handle = requireAccepted(enqueueFirstBusyPeriod());
            requireCancelled(scheduler.cancel(handle));
            return 1;
        }

        void verifyIdle() {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            if (snapshot.registeredFlows() != flowCount
                    || snapshot.queuedJobs() != 0
                    || snapshot.runningJobs() != 0
                    || snapshot.freeSlots() != depth
                    || snapshot.activeFlows() != 0
                    || snapshot.backloggedFlows() != 0
                    || snapshot.acceptedTotal() != snapshot.cancelledTotal()
                    || snapshot.dispatchedTotal() != 0
                    || snapshot.completedTotal() != 0) {
                throw new IllegalStateException("first-busy-period fixture is not idle: " + snapshot);
            }
        }

        private void verifyOneQueued() {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            if (snapshot.registeredFlows() != flowCount
                    || snapshot.queuedJobs() != 1
                    || snapshot.runningJobs() != 0
                    || snapshot.freeSlots() != depth
                    || snapshot.activeFlows() != 1
                    || snapshot.backloggedFlows() != 1
                    || snapshot.acceptedTotal() != snapshot.cancelledTotal() + 1
                    || snapshot.dispatchedTotal() != 0
                    || snapshot.completedTotal() != 0) {
                throw new IllegalStateException("first admission did not establish one queued job: " + snapshot);
            }
        }
    }
}
