package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class ReferenceScheduler<F, J, P> {
    private final SchedulerConfig config;
    private final OwnerToken ownerToken = new OwnerToken();
    private final Map<F, FlowHandle> registeredById = new LinkedHashMap<>();
    private final Map<FlowHandle, FlowState<F>> registeredFlows = new LinkedHashMap<>();
    private final Map<J, JobHandle> liveById = new LinkedHashMap<>();
    private final Map<JobHandle, QueuedJob<F, J, P>> queued = new LinkedHashMap<>();
    private final Map<JobHandle, RunningJob<J>> running = new LinkedHashMap<>();
    private final List<JobHandle> priority = new ArrayList<>();
    private ExactRational virtualTime = ExactRational.ZERO;
    private long lastJobSequence;
    private long lastFlowSequence;
    private long accepted;
    private long dispatched;
    private long cancelled;
    private long completed;

    ReferenceScheduler(SchedulerConfig config) {
        this(config, 0L, 0L);
    }

    private ReferenceScheduler(SchedulerConfig config, long lastJobSequence, long lastFlowSequence) {
        this.config = Objects.requireNonNull(config, "config");
        this.lastJobSequence = requireSequence(lastJobSequence);
        this.lastFlowSequence = requireSequence(lastFlowSequence);
    }

    static <F, J, P> ReferenceScheduler<F, J, P> withSequences(
            SchedulerConfig config, long lastJobSequence, long lastFlowSequence) {
        return new ReferenceScheduler<>(config, lastJobSequence, lastFlowSequence);
    }

    RegisterFlowResult registerFlow(F flowId, long weight) {
        Objects.requireNonNull(flowId, "flowId");
        requirePositive(weight, "weight");
        if (registeredById.containsKey(flowId)) {
            return RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID;
        }
        if (registeredFlows.size() == config.maxFlows()) {
            return RegisterFlowResult.Rejected.FLOW_LIMIT;
        }
        if (lastFlowSequence == Long.MAX_VALUE) {
            return RegisterFlowResult.Rejected.FLOW_SEQUENCE_EXHAUSTED;
        }
        FlowHandle handle = new FlowHandle(ownerToken, lastFlowSequence + 1L);
        FlowState<F> state = new FlowState<>(flowId, weight);
        registeredById.put(flowId, handle);
        registeredFlows.put(handle, state);
        lastFlowSequence++;
        return new RegisterFlowResult.Registered(handle);
    }

    CloseFlowResult closeFlow(FlowHandle flowHandle) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        FlowState<F> flow = registeredFlows.get(flowHandle);
        if (flow == null) {
            return CloseFlowResult.FLOW_NOT_REGISTERED;
        }
        if (flow.queuedCount != 0 || flow.runningCount != 0) {
            return CloseFlowResult.FLOW_ACTIVE;
        }
        if (!liveById.isEmpty()) {
            return CloseFlowResult.BUSY_PERIOD_ACTIVE;
        }
        registeredFlows.remove(flowHandle);
        registeredById.remove(flow.flowId);
        return CloseFlowResult.CLOSED;
    }

    EnqueueResult enqueue(FlowHandle flowHandle, J jobId, P payload, long cost) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(payload, "payload");
        requirePositive(cost, "cost");
        FlowState<F> flow = registeredFlows.get(flowHandle);
        if (flow == null) {
            return EnqueueResult.Rejected.FLOW_NOT_REGISTERED;
        }
        if (liveById.containsKey(jobId)) {
            return EnqueueResult.Rejected.DUPLICATE_LIVE_ID;
        }
        if (liveById.size() == config.maxLiveJobs()) {
            return EnqueueResult.Rejected.LIVE_LIMIT;
        }
        if (lastJobSequence == Long.MAX_VALUE) {
            return EnqueueResult.Rejected.SEQUENCE_EXHAUSTED;
        }
        ExactRational start = virtualTime.max(flow.lastFinish);
        ExactRational finish = start.add(ExactRational.of(cost, flow.weight));
        long sequence = lastJobSequence + 1L;
        JobHandle handle = new JobHandle(ownerToken, sequence);
        QueuedJob<F, J, P> job = new QueuedJob<>(
                handle, jobId, flowHandle, flow.flowId, payload, cost, start, finish, sequence);
        queued.put(handle, job);
        priority.add(handle);
        priority.sort(this::compareQueued);
        liveById.put(jobId, handle);
        flow.lastFinish = finish;
        flow.queuedCount++;
        flow.acceptedCost = flow.acceptedCost.add(BigInteger.valueOf(cost));
        lastJobSequence++;
        accepted++;
        return new EnqueueResult.Accepted(handle);
    }

    List<Dispatch<F, J, P>> capacityAvailable(int capacity) {
        if (capacity < 0 || capacity > config.depth()) {
            throw new IllegalArgumentException("capacity must be in [0, depth]");
        }
        int count = Math.min(capacity, Math.min(config.depth() - running.size(), queued.size()));
        List<Dispatch<F, J, P>> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            JobHandle handle = priority.removeFirst();
            QueuedJob<F, J, P> job = Objects.requireNonNull(queued.remove(handle), "queued job");
            virtualTime = job.start;
            FlowState<F> flow = registeredFlows.get(job.flowHandle);
            flow.queuedCount--;
            flow.runningCount++;
            flow.dispatchedCost = flow.dispatchedCost.add(BigInteger.valueOf(job.cost));
            running.put(handle, new RunningJob<>(handle, job.jobId, job.flowHandle, job.cost));
            dispatched++;
            result.add(new Dispatch<>(handle, job.jobId, job.flowId, job.payload, job.cost));
        }
        return List.copyOf(result);
    }

    CancelResult cancel(JobHandle handle) {
        Objects.requireNonNull(handle, "handle");
        QueuedJob<F, J, P> job = queued.remove(handle);
        if (job != null) {
            priority.remove(handle);
            liveById.remove(job.jobId);
            FlowState<F> flow = registeredFlows.get(job.flowHandle);
            flow.queuedCount--;
            flow.cancelledCost = flow.cancelledCost.add(BigInteger.valueOf(job.cost));
            cancelled++;
            resetIfIdle();
            return CancelResult.CANCELLED;
        }
        return running.containsKey(handle) ? CancelResult.TOO_LATE_ALREADY_DISPATCHED : CancelResult.NOT_LIVE;
    }

    CompletionResult complete(JobHandle handle) {
        Objects.requireNonNull(handle, "handle");
        RunningJob<J> job = running.remove(handle);
        if (job == null) {
            return queued.containsKey(handle) ? CompletionResult.NOT_DISPATCHED : CompletionResult.NOT_LIVE;
        }
        liveById.remove(job.jobId);
        FlowState<F> flow = registeredFlows.get(job.flowHandle);
        flow.runningCount--;
        completed++;
        resetIfIdle();
        return CompletionResult.COMPLETED;
    }

    SchedulerSnapshot snapshot() {
        int active = 0;
        int backlogged = 0;
        for (FlowState<F> flow : registeredFlows.values()) {
            if (flow.queuedCount + flow.runningCount > 0) {
                active++;
            }
            if (flow.queuedCount > 0) {
                backlogged++;
            }
        }
        return new SchedulerSnapshot(
                config.depth(), config.maxFlows(), config.maxLiveJobs(), registeredFlows.size(), queued.size(),
                running.size(), config.depth() - running.size(), active, backlogged,
                accepted, dispatched, cancelled, completed);
    }

    Optional<FlowSnapshot> snapshot(FlowHandle flowHandle) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        FlowState<F> flow = registeredFlows.get(flowHandle);
        return flow == null
                ? Optional.empty()
                : Optional.of(new FlowSnapshot(
                        flow.queuedCount, flow.runningCount,
                        flow.acceptedCost, flow.dispatchedCost, flow.cancelledCost));
    }

    List<JobHandle> queuedHandles() {
        return List.copyOf(priority);
    }

    ExactRational startTag(JobHandle handle) {
        return queuedJob(handle).start;
    }

    ExactRational finishTag(JobHandle handle) {
        return queuedJob(handle).finish;
    }

    QueuedState<F, J, P> queuedState(JobHandle handle) {
        QueuedJob<F, J, P> job = queuedJob(handle);
        return new QueuedState<>(
                job.handle, job.jobId, job.flowHandle, job.payload, job.cost, job.start, job.finish, job.sequence);
    }

    ExactRational virtualTime() {
        return virtualTime;
    }

    RunningState<J> runningState(JobHandle handle) {
        RunningJob<J> job = Objects.requireNonNull(running.get(handle), "running job");
        return new RunningState<>(job.handle, job.jobId, job.flowHandle, job.cost);
    }

    private QueuedJob<F, J, P> queuedJob(JobHandle handle) {
        return Objects.requireNonNull(queued.get(handle), "queued job");
    }

    private int compareQueued(JobHandle first, JobHandle second) {
        QueuedJob<F, J, P> firstJob = queuedJob(first);
        QueuedJob<F, J, P> secondJob = queuedJob(second);
        int startOrder = firstJob.start.compareTo(secondJob.start);
        return startOrder != 0 ? startOrder : Long.compare(firstJob.sequence, secondJob.sequence);
    }

    private void resetIfIdle() {
        if (!liveById.isEmpty()) {
            return;
        }
        virtualTime = ExactRational.ZERO;
        for (FlowState<F> flow : registeredFlows.values()) {
            flow.lastFinish = ExactRational.ZERO;
        }
    }

    private static long requireSequence(long sequence) {
        if (sequence < 0L) {
            throw new IllegalArgumentException("sequence must be non-negative");
        }
        return sequence;
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    record QueuedState<F, J, P>(
            JobHandle handle,
            J jobId,
            FlowHandle flowHandle,
            P payload,
            long cost,
            ExactRational start,
            ExactRational finish,
            long sequence) {
    }

    record RunningState<J>(JobHandle handle, J jobId, FlowHandle flowHandle, long cost) {
    }

    private static final class FlowState<F> {
        private final F flowId;
        private final long weight;
        private ExactRational lastFinish = ExactRational.ZERO;
        private int queuedCount;
        private int runningCount;
        private BigInteger acceptedCost = BigInteger.ZERO;
        private BigInteger dispatchedCost = BigInteger.ZERO;
        private BigInteger cancelledCost = BigInteger.ZERO;

        private FlowState(F flowId, long weight) {
            this.flowId = flowId;
            this.weight = weight;
        }
    }

    private static final class QueuedJob<F, J, P> {
        private final JobHandle handle;
        private final J jobId;
        private final FlowHandle flowHandle;
        private final F flowId;
        private final P payload;
        private final long cost;
        private final ExactRational start;
        private final ExactRational finish;
        private final long sequence;

        private QueuedJob(
                JobHandle handle,
                J jobId,
                FlowHandle flowHandle,
                F flowId,
                P payload,
                long cost,
                ExactRational start,
                ExactRational finish,
                long sequence) {
            this.handle = handle;
            this.jobId = jobId;
            this.flowHandle = flowHandle;
            this.flowId = flowId;
            this.payload = payload;
            this.cost = cost;
            this.start = start;
            this.finish = finish;
            this.sequence = sequence;
        }
    }

    private static final class RunningJob<J> {
        private final JobHandle handle;
        private final J jobId;
        private final FlowHandle flowHandle;
        private final long cost;

        private RunningJob(JobHandle handle, J jobId, FlowHandle flowHandle, long cost) {
            this.handle = handle;
            this.jobId = jobId;
            this.flowHandle = flowHandle;
            this.cost = cost;
        }
    }
}
