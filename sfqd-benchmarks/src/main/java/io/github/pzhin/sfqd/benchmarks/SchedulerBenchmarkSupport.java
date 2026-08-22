package io.github.pzhin.sfqd.benchmarks;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** Shared deterministic fixtures for the scheduler benchmarks. */
public final class SchedulerBenchmarkSupport {
    static final Payload PAYLOAD = Payload.INSTANCE;
    static final int[] POWERS_OF_TWO = {1, 2, 4, 8, 16, 32};
    static final int[] COSTS = {1, 2, 4, 8, 16, 32, 64};
    static final int MAX_PAIRWISE_COPRIME_FLOWS = 10_000;

    private SchedulerBenchmarkSupport() {
    }

    /** Workload distributions with an explicit active-flow meaning. */
    public enum Scenario {
        /** Equal weights/costs, one anchor per flow, and reserve jobs spread round-robin. */
        UNIFORM,
        /** All configured flows are registered, but only flow zero is active and backlogged. */
        ONE_HOT,
        /** Every flow starts with three anchors and receives round-robin reserve jobs. */
        ALL_BACKLOGGED,
        /** Flow weights repeat {@code 1,2,4,8,16,32}. */
        SKEWED_WEIGHTS,
        /** Every active flow receives a distinct prime weight. */
        PAIRWISE_COPRIME_WEIGHTS,
        /** Job costs repeat {@code 1,2,4,8,16,32,64}. */
        SKEWED_COSTS
    }

    /** Weight populations used by the explicit performance-scale matrix. */
    public enum WeightModel {
        /** Every flow has weight one. */
        EQUAL,
        /** Every flow receives a distinct prime weight, making all weights pairwise coprime. */
        PAIRWISE_COPRIME
    }

    /**
     * Immutable flow identifier preallocated outside measured operations.
     *
     * @param index zero-based fixture flow index
     */
    record FlowKey(int index) {
    }

    /**
     * Immutable job identifier preallocated outside measured operations.
     *
     * @param sequence fixture-local identifier sequence
     */
    record JobKey(long sequence) {
    }

    /** Singleton payload; caller payload allocation is not benchmarked. */
    enum Payload {
        INSTANCE
    }

    /** Mutable harness record used to rotate a bounded set of job identifiers. */
    static final class JobRecord {
        private final JobKey jobId;
        private final int flowIndex;
        private final long cost;
        private JobHandle handle;

        JobRecord(JobKey jobId, int flowIndex, long cost, JobHandle handle) {
            this.jobId = jobId;
            this.flowIndex = flowIndex;
            this.cost = cost;
            this.handle = handle;
        }

        JobKey jobId() {
            return jobId;
        }

        int flowIndex() {
            return flowIndex;
        }

        long cost() {
            return cost;
        }

        JobHandle handle() {
            return handle;
        }

        void replaceHandle(JobHandle replacement) {
            handle = replacement;
        }
    }

    /** Bounded scheduler plus caller-side records needed to restore benchmark state. */
    static final class Fixture {
        private final Scenario scenario;
        private final SfqdScheduler<FlowKey, JobKey, Payload> scheduler;
        private final List<FlowKey> flowIds;
        private final List<FlowHandle> flowHandles;
        private final List<ArrayDeque<JobRecord>> queuedByFlow;
        private long nextJobSequence;
        private int initialQueued;

        Fixture(int flowCount, int depth, Scenario scenario, int inactiveRegistrations) {
            this(flowCount, depth, scenario, inactiveRegistrations, 1);
        }

        Fixture(int flowCount, int depth, Scenario scenario, int inactiveRegistrations, int minimumAnchors) {
            if (flowCount < 1 || inactiveRegistrations < 0) {
                throw new IllegalArgumentException("flow counts must be valid");
            }
            this.scenario = scenario;
            int registered = flowCount + inactiveRegistrations;
            int anchors = Math.max(scenario == Scenario.ALL_BACKLOGGED ? 3 : 1, minimumAnchors);
            int reserve = depth + 1;
            int initiallyActive = expectedActiveFlows(flowCount, scenario);
            int plannedJobs = Math.addExact(Math.multiplyExact(initiallyActive, anchors), reserve);
            this.scheduler = new SfqdScheduler<>(new SchedulerConfig(
                    depth, registered, Math.addExact(plannedJobs, depth + 64)));
            this.flowIds = new ArrayList<>(registered);
            this.flowHandles = new ArrayList<>(registered);
            this.queuedByFlow = new ArrayList<>(registered);
            for (int index = 0; index < registered; index++) {
                FlowKey flowId = new FlowKey(index);
                flowIds.add(flowId);
                flowHandles.add(requireRegistered(scheduler.registerFlow(flowId, weight(index, scenario))));
                queuedByFlow.add(new ArrayDeque<>(anchors + reserve + 2));
            }
            for (int flow = 0; flow < initiallyActive; flow++) {
                for (int anchor = 0; anchor < anchors; anchor++) {
                    enqueueNew(flow);
                }
            }
            for (int index = 0; index < reserve; index++) {
                int flow = scenario == Scenario.ONE_HOT ? 0 : index % flowCount;
                enqueueNew(flow);
            }
            initialQueued = scheduler.snapshot().queuedJobs();
            assertSteadyShape(initialQueued, initiallyActive);
        }

        SfqdScheduler<FlowKey, JobKey, Payload> scheduler() {
            return scheduler;
        }

        FlowHandle flowHandle(int index) {
            return flowHandles.get(index);
        }

        FlowKey flowId(int index) {
            return flowIds.get(index);
        }

        ArrayDeque<JobRecord> queue(int index) {
            return queuedByFlow.get(index);
        }

        int initialQueued() {
            return initialQueued;
        }

        /**
         * Returns the caller-model count of flows that still have at least one queued job.
         *
         * @return exact count of nonempty caller queues
         */
        int backloggedFlowCount() {
            int count = 0;
            for (ArrayDeque<JobRecord> queue : queuedByFlow) {
                if (!queue.isEmpty()) {
                    count++;
                }
            }
            return count;
        }

        JobRecord allocateRecord(int flowIndex) {
            return new JobRecord(new JobKey(++nextJobSequence), flowIndex, cost(flowIndex), null);
        }

        void enqueueRecord(JobRecord record) {
            EnqueueResult result = scheduler.enqueue(
                    flowHandles.get(record.flowIndex()), record.jobId(), PAYLOAD, record.cost());
            record.replaceHandle(requireAccepted(result));
            queuedByFlow.get(record.flowIndex()).addLast(record);
        }

        void restoreDispatches(List<Dispatch<FlowKey, JobKey, Payload>> dispatches) {
            for (Dispatch<FlowKey, JobKey, Payload> dispatch : dispatches) {
                ArrayDeque<JobRecord> queue = queuedByFlow.get(dispatch.flowId().index());
                JobRecord record = removeByHandle(queue, dispatch.jobHandle());
                requireCompleted(scheduler.complete(dispatch.jobHandle()));
                EnqueueResult replacement = scheduler.enqueue(
                        flowHandles.get(record.flowIndex()), record.jobId(), PAYLOAD, record.cost());
                record.replaceHandle(requireAccepted(replacement));
                queue.addLast(record);
            }
        }

        void assertSteadyShape(int expectedQueued, int expectedActiveFlows) {
            SchedulerSnapshot snapshot = scheduler.snapshot();
            if (snapshot.registeredFlows() != flowHandles.size()
                    || snapshot.queuedJobs() != expectedQueued
                    || snapshot.runningJobs() != 0
                    || snapshot.activeFlows() != expectedActiveFlows
                    || snapshot.backloggedFlows() != expectedActiveFlows) {
                throw new IllegalStateException("unexpected bounded fixture state: " + snapshot);
            }
        }

        private JobRecord enqueueNew(int flowIndex) {
            JobRecord record = allocateRecord(flowIndex);
            enqueueRecord(record);
            return record;
        }

        private long cost(int flowIndex) {
            return scenario == Scenario.SKEWED_COSTS ? COSTS[flowIndex % COSTS.length] : 1L;
        }
    }

    static long weight(int flowIndex, Scenario scenario) {
        if (scenario == Scenario.PAIRWISE_COPRIME_WEIGHTS) {
            return weight(flowIndex, WeightModel.PAIRWISE_COPRIME);
        }
        return scenario == Scenario.SKEWED_WEIGHTS
                ? POWERS_OF_TWO[flowIndex % POWERS_OF_TWO.length] : 1L;
    }

    static long weight(int flowIndex, WeightModel weightModel) {
        if (weightModel == WeightModel.EQUAL) {
            return 1L;
        }
        if (flowIndex < 0 || flowIndex >= PairwiseCoprimeWeights.VALUES.length) {
            throw new IllegalArgumentException("pairwise-coprime flow index is outside the benchmark matrix");
        }
        return PairwiseCoprimeWeights.VALUES[flowIndex];
    }

    static int expectedActiveFlows(int flowCount, Scenario scenario) {
        return scenario == Scenario.ONE_HOT ? 1 : flowCount;
    }

    static JobHandle requireAccepted(EnqueueResult result) {
        if (result instanceof EnqueueResult.Accepted accepted) {
            return accepted.jobHandle();
        }
        throw new IllegalStateException("unexpected enqueue rejection: " + result);
    }

    static FlowHandle requireRegistered(RegisterFlowResult result) {
        if (result instanceof RegisterFlowResult.Registered registered) {
            return registered.flowHandle();
        }
        throw new IllegalStateException("unexpected registration rejection: " + result);
    }

    static void requireCancelled(CancelResult result) {
        if (result != CancelResult.CANCELLED) {
            throw new IllegalStateException("unexpected cancellation result: " + result);
        }
    }

    static void requireCompleted(CompletionResult result) {
        if (result != CompletionResult.COMPLETED) {
            throw new IllegalStateException("unexpected completion result: " + result);
        }
    }

    static JobRecord removeByHandle(ArrayDeque<JobRecord> queue, JobHandle handle) {
        for (JobRecord record : queue) {
            if (record.handle().equals(handle)) {
                if (!queue.remove(record)) {
                    throw new IllegalStateException("queued record disappeared");
                }
                return record;
            }
        }
        throw new IllegalStateException("dispatched handle absent from caller model");
    }

    private static final class PairwiseCoprimeWeights {
        private static final int SIEVE_LIMIT = 110_000;
        private static final int[] VALUES = generate();

        private PairwiseCoprimeWeights() {
        }

        private static int[] generate() {
            boolean[] composite = new boolean[SIEVE_LIMIT + 1];
            int[] primes = new int[MAX_PAIRWISE_COPRIME_FLOWS + 1];
            int count = 0;
            for (int candidate = 2; candidate <= SIEVE_LIMIT && count < primes.length; candidate++) {
                if (composite[candidate]) {
                    continue;
                }
                primes[count++] = candidate;
                if ((long) candidate * candidate <= SIEVE_LIMIT) {
                    for (int multiple = candidate * candidate; multiple <= SIEVE_LIMIT; multiple += candidate) {
                        composite[multiple] = true;
                    }
                }
            }
            if (count != primes.length) {
                throw new IllegalStateException("prime sieve does not cover the benchmark flow matrix");
            }
            return primes;
        }
    }
}
