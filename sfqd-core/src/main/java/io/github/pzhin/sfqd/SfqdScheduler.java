package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A generic, fully thread-safe Start-time Fair Queueing scheduler with issue depth {@code D}.
 *
 * <p>The scheduler only makes admission and dispatch decisions. It neither executes jobs nor owns an executor,
 * thread pool, resource pool, or completion callback. Fairness is measured against the caller-supplied cost and the
 * registered flow weight, not against unknown actual execution time.
 * Completed-work fairness guarantees do not apply to traces containing cancellation because the supported
 * {@link CancellationAccounting#CHARGE_RESERVED_COST} policy retains cancelled jobs' virtual cost until global idle.
 *
 * <p>All public operations are linearizable and may be invoked concurrently without external synchronization. This
 * baseline uses one private lock: a successful mutating operation linearizes when its complete state transition is
 * committed under that lock; a rejection or read linearizes at its decisive observation under the lock. A non-empty
 * dispatch batch is one atomic transition. In a cancel-versus-dispatch race, {@link CancelResult#CANCELLED} means the
 * handle cannot appear in the batch, while presence in a batch means dispatch won; a later
 * {@link CancelResult#NOT_LIVE} alone intentionally does not reveal the terminal cause. Completion and cancellation
 * successes are exactly once.
 *
 * <p>Queued jobs are linked per flow and only each flow head is globally ordered. Enqueue and cancellation of a flow
 * head are {@code O(log B)} for {@code B} backlogged flows; cancellation of a non-head and completion are expected
 * {@code O(1)}. A batch selecting {@code m} jobs is {@code O(m log B + m)}. Snapshot is {@code O(1)}. The normative
 * idle reset is {@code O(registeredFlows)}. A canonically triggered exact-tag rebase is
 * {@code O(queuedJobs + registeredFlows)} and is computed transactionally before it becomes observable. Internal
 * records are bounded by configured live-job and registration limits; terminal tombstones are not retained.
 *
 * <p>Weights and costs are positive {@code long} values in {@code [1, Long.MAX_VALUE]}. Tags are exact reduced
 * non-negative rationals: each numerator and denominator retained in scheduler state has bit length at most 4096,
 * and each canonical raw or reduced component of an exact primitive has bit length at most 8193. No rounding or
 * floating-point fallback is permitted. If a newly computed start or finish tag first exceeds the persistent budget,
 * admission makes exactly one canonical rebase attempt over virtual time, every queued tag, and every registered
 * flow's finish history on temporary state. Rebase and admission commit together, or
 * {@link EnqueueResult.Rejected#NUMERIC_LIMIT} leaves the complete observable state unchanged and consumes no job
 * sequence. A rebase is never proactive, partial, or retried.
 *
 * <p>Flow and job identifiers must have stable, deterministic, side-effect-free, non-throwing {@code equals} and
 * {@code hashCode} implementations while retained. Reentrant calls from either method violate the caller contract.
 *
 * @param <F> flow identifier type
 * @param <J> job identifier type
 * @param <P> payload type
 */
public final class SfqdScheduler<F, J, P> {
    private final SchedulerConfig config;
    private final OwnerToken ownerToken = new OwnerToken();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<F, FlowHandle> registeredById = new HashMap<>();
    private final Map<FlowHandle, FlowState<F, J, P>> registeredFlows = new HashMap<>();
    private final Map<J, JobHandle> liveById = new HashMap<>();
    private final Map<JobHandle, QueuedJob<F, J, P>> queued = new HashMap<>();
    private final Map<JobHandle, RunningJob<J>> running = new HashMap<>();
    private NavigableSet<QueuedJob<F, J, P>> backlogged = new TreeSet<>(this::compareQueued);
    private ExactTag virtualTime = ExactTag.zero();
    private long lastJobSequence;
    private long lastFlowSequence;
    private long accepted;
    private long dispatched;
    private long cancelled;
    private long completed;
    private int activeFlowCount;

    /**
     * Creates an empty scheduler with immutable limits.
     *
     * @param config depth, cardinality limits, and cancellation accounting policy
     * @throws NullPointerException if config is null
     */
    public SfqdScheduler(SchedulerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Registers a fixed positive weight for a flow identifier.
     *
     * <p>A success linearizes at insertion into both registration indexes and sequence advancement. A rejection
     * linearizes at the first applicable check and does not mutate state or consume a sequence.
     *
     * @param flowId stable non-null flow identifier
     * @param weight fixed registration weight in {@code [1, Long.MAX_VALUE]}
     * @return a fresh opaque capability or a bounded rejection
     * @throws NullPointerException if flowId is null
     * @throws IllegalArgumentException if weight is not positive
     */
    public RegisterFlowResult registerFlow(F flowId, long weight) {
        Objects.requireNonNull(flowId, "flowId");
        requirePositive(weight, "weight");
        lock.lock();
        try {
            if (registeredById.containsKey(flowId)) {
                return RegisterFlowResult.Rejected.DUPLICATE_REGISTERED_ID;
            }
            if (registeredFlows.size() == config.maxFlows()) {
                return RegisterFlowResult.Rejected.FLOW_LIMIT;
            }
            if (lastFlowSequence == Long.MAX_VALUE) {
                return RegisterFlowResult.Rejected.FLOW_SEQUENCE_EXHAUSTED;
            }
            long sequence = lastFlowSequence + 1L;
            FlowHandle handle = new FlowHandle(ownerToken, sequence);
            FlowState<F, J, P> flow = new FlowState<>(handle, flowId, weight);
            registeredById.put(flowId, handle);
            registeredFlows.put(handle, flow);
            lastFlowSequence = sequence;
            return new RegisterFlowResult.Registered(handle);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes an inactive registration once its finish-tag debt is no greater than current virtual time.
     *
     * <p>Success linearizes at removal from both registration indexes. Other results linearize at their decisive
     * observation and do not mutate state. Closing at {@code lastFinish <= virtualTime} is fairness-neutral: retaining
     * the old registration or registering a new identity would give its next job the same start tag, namely current
     * virtual time.
     *
     * @param flowHandle opaque registration capability
     * @return close outcome
     * @throws NullPointerException if flowHandle is null
     */
    public CloseFlowResult closeFlow(FlowHandle flowHandle) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        lock.lock();
        try {
            FlowState<F, J, P> flow = registeredFlows.get(flowHandle);
            if (flow == null) {
                return CloseFlowResult.FLOW_NOT_REGISTERED;
            }
            if (flow.queuedCount != 0 || flow.runningCount != 0) {
                return CloseFlowResult.FLOW_ACTIVE;
            }
            if (compareTags(flow.lastFinish, virtualTime) > 0) {
                return CloseFlowResult.FAIRNESS_DEBT_ACTIVE;
            }
            registeredFlows.remove(flowHandle);
            registeredById.remove(flow.flowId);
            return CloseFlowResult.CLOSED;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enqueues one job using exact {@code cost / registeredWeight} tag arithmetic.
     *
     * <p>Success linearizes at the atomic commit of tag updates, all indexes, counters, and the fresh sequence. A
     * rejection, including {@link EnqueueResult.Rejected#NUMERIC_LIMIT}, is a complete no-op and consumes no sequence.
     * Each canonically reduced persistent tag component is limited to 4096 bits; canonical raw and reduced quantities
     * of an exact primitive are limited to 8193 bits. If the initial new start or finish tag exceeds the 4096-bit
     * budget, exactly one canonical rebase is computed on temporary state for virtual time, every queued start/finish
     * tag, and every registered flow finish tag, then admission is retried. The rebase and accepted job commit as one
     * transition only when all transient and persistent results fit. Otherwise {@code NUMERIC_LIMIT} discards the
     * entire temporary computation: no tag, index, counter, sequence, or other observable state changes.
     *
     * @param flowHandle registered flow capability
     * @param jobId stable non-null identifier, unique among live jobs
     * @param payload non-null caller payload, returned by identity on dispatch
     * @param cost supplied positive cost in {@code [1, Long.MAX_VALUE]}
     * @return admission outcome
     * @throws NullPointerException if any reference argument is null
     * @throws IllegalArgumentException if cost is not positive
     */
    public EnqueueResult enqueue(FlowHandle flowHandle, J jobId, P payload, long cost) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(payload, "payload");
        requirePositive(cost, "cost");
        lock.lock();
        try {
            FlowState<F, J, P> flow = registeredFlows.get(flowHandle);
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
            ExactTag increment;
            try {
                increment = ExactTag.fromCostAndWeight(cost, flow.weight);
            } catch (NumericLimitException impossible) {
                throw new AssertionError("a reduced pair of positive long values must fit", impossible);
            }
            EnqueueComputation<F, J, P> computation = computeEnqueue(flow, increment);
            if (computation == null) {
                return EnqueueResult.Rejected.NUMERIC_LIMIT;
            }
            if (computation.rebase != null) {
                commitRebase(computation.rebase);
            }
            long sequence = lastJobSequence + 1L;
            JobHandle handle = new JobHandle(ownerToken, sequence);
            QueuedJob<F, J, P> job = new QueuedJob<>(
                    handle, jobId, flow, payload, cost, computation.start, computation.finish, sequence);
            appendQueued(flow, job);
            queued.put(handle, job);
            liveById.put(jobId, handle);
            flow.lastFinish = computation.finish;
            flow.acceptedCost = flow.acceptedCost.add(BigInteger.valueOf(cost));
            lastJobSequence = sequence;
            accepted++;
            return new EnqueueResult.Accepted(handle);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancels a queued job without revoking an already dispatched job.
     *
     * <p>Success linearizes at removal from every queued/live structure. A running observation returns
     * {@code TOO_LATE_ALREADY_DISPATCHED}; absence returns {@code NOT_LIVE}, which intentionally does not distinguish
     * stale, foreign, cancelled, completed, or never-existing handles. In a cancel-versus-dispatch race,
     * {@code CANCELLED} and absence from the returned batch means cancellation won; presence in a batch means dispatch
     * won. If an earlier batch selected only other jobs, this queued handle remains cancellable. A late
     * {@code NOT_LIVE} alone does not identify the winner or terminal cause.
     *
     * <p><strong>Fairness accounting warning:</strong> under
     * {@link CancellationAccounting#CHARGE_RESERVED_COST}, cancellation does not reduce the flow's finish history and
     * does not recompute tags of its later jobs. The cancelled supplied cost remains a virtual charge until all live
     * jobs leave the scheduler and the global busy period ends. Completed-work fairness guarantees therefore do not
     * apply to traces containing cancellation. Workloads with frequent deadline or timeout cancellations must account
     * for the resulting dispatch delay.
     *
     * @param handle opaque job capability
     * @return cancellation outcome at the operation's linearization point
     * @throws NullPointerException if handle is null
     */
    public CancelResult cancel(JobHandle handle) {
        Objects.requireNonNull(handle, "handle");
        lock.lock();
        try {
            QueuedJob<F, J, P> job = queued.get(handle);
            if (job != null) {
                removeQueued(job);
                queued.remove(handle);
                liveById.remove(job.jobId);
                job.flow.cancelledCost = job.flow.cancelledCost.add(BigInteger.valueOf(job.cost));
                cancelled++;
                resetIfIdle();
                return CancelResult.CANCELLED;
            }
            return running.containsKey(handle)
                    ? CancelResult.TOO_LATE_ALREADY_DISPATCHED : CancelResult.NOT_LIVE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically dispatches up to the requested capacity, available issue slots, and queued jobs.
     *
     * <p>The whole immutable result batch linearizes at one commit. Jobs are irrevocably running at return; caller
     * failure does not roll back dispatch and each returned handle must eventually be completed. In a race, a handle
     * returned here cannot also have a successful cancellation. A cancellation that linearized first excludes its
     * handle from the batch. An earlier batch that did not select a particular queued handle does not prevent a later
     * successful cancellation of that handle. A later {@link CancelResult#NOT_LIVE} alone does not identify which
     * terminal operation occurred; callers determine the winner from the combined dispatch/cancel/completion history.
     *
     * @param maxJobs maximum jobs to dispatch in this call, in {@code [0, depth]}
     * @return immutable detached list in actual dispatch order
     * @throws IllegalArgumentException if maxJobs is outside {@code [0, depth]}
     */
    public List<Dispatch<F, J, P>> dispatchUpTo(int maxJobs) {
        if (maxJobs < 0 || maxJobs > config.depth()) {
            throw new IllegalArgumentException("maxJobs must be in [0, depth]");
        }
        lock.lock();
        try {
            int count = Math.min(maxJobs, Math.min(config.depth() - running.size(), queued.size()));
            List<Dispatch<F, J, P>> result = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                QueuedJob<F, J, P> job = backlogged.pollFirst();
                if (job == null) {
                    throw new IllegalStateException("queued index invariant violated");
                }
                FlowState<F, J, P> flow = job.flow;
                unlinkHead(flow, job);
                queued.remove(job.handle);
                virtualTime = job.start;
                flow.runningCount++;
                flow.dispatchedCost = flow.dispatchedCost.add(BigInteger.valueOf(job.cost));
                running.put(job.handle, new RunningJob<>(job.jobId, flow.handle, job.cost));
                dispatched++;
                result.add(new Dispatch<>(job.handle, job.jobId, flow.flowId, job.payload, job.cost));
            }
            return List.copyOf(result);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Completes one running job and releases exactly one issue slot.
     *
     * <p>Success linearizes at removal from running/live state. A queued observation returns {@code NOT_DISPATCHED};
     * absence returns bounded-history {@code NOT_LIVE}. At most one concurrent caller can obtain {@code COMPLETED}.
     *
     * @param handle opaque job capability
     * @return completion outcome
     * @throws NullPointerException if handle is null
     */
    public CompletionResult complete(JobHandle handle) {
        Objects.requireNonNull(handle, "handle");
        lock.lock();
        try {
            RunningJob<J> job = running.remove(handle);
            if (job == null) {
                return queued.containsKey(handle) ? CompletionResult.NOT_DISPATCHED : CompletionResult.NOT_LIVE;
            }
            liveById.remove(job.jobId);
            if (job.cost <= 0L) {
                throw new IllegalStateException("running job cost invariant violated");
            }
            FlowState<F, J, P> flow = registeredFlows.get(job.flowHandle);
            if (flow == null) {
                throw new IllegalStateException("running flow registration invariant violated");
            }
            flow.runningCount--;
            if (flow.queuedCount + flow.runningCount == 0) {
                activeFlowCount--;
            }
            completed++;
            resetIfIdle();
            return CompletionResult.COMPLETED;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an exact immutable aggregate snapshot from one linearization point.
     *
     * @return current aggregate state without identifiers, handles, payloads, or tags
     */
    public SchedulerSnapshot snapshot() {
        lock.lock();
        try {
            return new SchedulerSnapshot(
                    config.depth(), config.maxFlows(), config.maxLiveJobs(), registeredFlows.size(), queued.size(),
                    running.size(), config.depth() - running.size(), activeFlowCount, backlogged.size(),
                    accepted, dispatched, cancelled, completed);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an exact immutable snapshot for one currently registered flow.
     *
     * <p>The snapshot is captured at one linearization point under the scheduler's internal synchronization. Cost
     * totals cover the lifetime of this exact registration and use unbounded integers, so repeated positive
     * {@code long} costs cannot overflow. A foreign, stale, or closed capability produces an empty result. No flow
     * identifier, payload, internal scheduling tag, or clock-derived age is exposed.
     *
     * @param flowHandle opaque registration capability
     * @return the current registration snapshot, or empty when the capability is not registered
     * @throws NullPointerException if flowHandle is null
     */
    public Optional<FlowSnapshot> snapshot(FlowHandle flowHandle) {
        Objects.requireNonNull(flowHandle, "flowHandle");
        lock.lock();
        try {
            FlowState<F, J, P> flow = registeredFlows.get(flowHandle);
            if (flow == null) {
                return Optional.empty();
            }
            return Optional.of(new FlowSnapshot(
                    flow.queuedCount, flow.runningCount,
                    flow.acceptedCost, flow.dispatchedCost, flow.cancelledCost));
        } finally {
            lock.unlock();
        }
    }

    private EnqueueComputation<F, J, P> computeEnqueue(FlowState<F, J, P> flow, ExactTag increment) {
        try {
            ExactTag start = virtualTime.max(flow.lastFinish);
            return new EnqueueComputation<>(start, start.add(increment), null);
        } catch (NumericLimitException initialFailure) {
            if (initialFailure.budget() != NumericLimitException.Budget.PERSISTENT) {
                return null;
            }
        }
        try {
            RebasePlan<F, J, P> rebase = prepareRebase();
            ExactTag rebasedLastFinish = rebase.flowTags.get(flow);
            ExactTag start = ExactTag.zero().max(rebasedLastFinish);
            return new EnqueueComputation<>(start, start.add(increment), rebase);
        } catch (NumericLimitException rejected) {
            return null;
        }
    }

    private RebasePlan<F, J, P> prepareRebase() throws NumericLimitException {
        Map<FlowState<F, J, P>, ExactTag> flowTags = new IdentityHashMap<>();
        for (FlowState<F, J, P> flow : registeredFlows.values()) {
            ExactTag transformed = compareTags(flow.lastFinish, virtualTime) < 0
                    ? ExactTag.zero() : flow.lastFinish.subtractNonNegative(virtualTime);
            flowTags.put(flow, transformed);
        }
        Map<QueuedJob<F, J, P>, TagPair> jobTags = new IdentityHashMap<>();
        for (QueuedJob<F, J, P> job : queued.values()) {
            jobTags.put(job, new TagPair(
                    job.start.subtractNonNegative(virtualTime), job.finish.subtractNonNegative(virtualTime)));
        }
        return new RebasePlan<>(flowTags, jobTags);
    }

    private void commitRebase(RebasePlan<F, J, P> rebase) {
        // TreeSet's SortedSet copy path is linear. Every queued start tag receives the same subtraction, so the
        // existing (start, sequence) order remains valid after the prevalidated tag replacements below.
        NavigableSet<QueuedJob<F, J, P>> rebasedHeads = new TreeSet<>(backlogged);
        for (Map.Entry<FlowState<F, J, P>, ExactTag> entry : rebase.flowTags.entrySet()) {
            entry.getKey().lastFinish = entry.getValue();
        }
        for (Map.Entry<QueuedJob<F, J, P>, TagPair> entry : rebase.jobTags.entrySet()) {
            entry.getKey().start = entry.getValue().start;
            entry.getKey().finish = entry.getValue().finish;
        }
        virtualTime = ExactTag.zero();
        backlogged = rebasedHeads;
    }

    private void appendQueued(FlowState<F, J, P> flow, QueuedJob<F, J, P> job) {
        if (flow.tail == null) {
            flow.head = job;
            flow.tail = job;
            requireIndexChange(backlogged.add(job), "new flow head was already indexed");
            if (flow.runningCount == 0) {
                activeFlowCount++;
            }
        } else {
            job.previous = flow.tail;
            flow.tail.next = job;
            flow.tail = job;
        }
        flow.queuedCount++;
    }

    private void removeQueued(QueuedJob<F, J, P> job) {
        FlowState<F, J, P> flow = job.flow;
        boolean removesHead = job.previous == null;
        if (removesHead) {
            requireIndexChange(backlogged.remove(job), "cancelled flow head was not indexed");
        }
        if (job.previous == null) {
            flow.head = job.next;
        } else {
            job.previous.next = job.next;
        }
        if (job.next == null) {
            flow.tail = job.previous;
        } else {
            job.next.previous = job.previous;
        }
        flow.queuedCount--;
        if (removesHead && flow.head != null) {
            requireIndexChange(backlogged.add(flow.head), "promoted flow head was already indexed");
        }
        if (flow.queuedCount + flow.runningCount == 0) {
            activeFlowCount--;
        }
    }

    private void unlinkHead(FlowState<F, J, P> flow, QueuedJob<F, J, P> job) {
        if (flow.head != job) {
            throw new IllegalStateException("selected job is not its flow head");
        }
        flow.head = job.next;
        if (flow.head == null) {
            flow.tail = null;
        } else {
            flow.head.previous = null;
            requireIndexChange(backlogged.add(flow.head), "promoted flow head was already indexed");
        }
        flow.queuedCount--;
    }

    private void resetIfIdle() {
        if (!liveById.isEmpty()) {
            return;
        }
        virtualTime = ExactTag.zero();
        for (FlowState<F, J, P> flow : registeredFlows.values()) {
            flow.lastFinish = ExactTag.zero();
        }
    }

    private int compareQueued(QueuedJob<F, J, P> first, QueuedJob<F, J, P> second) {
        if (first == second) {
            return 0;
        }
        int startOrder = compareTags(first.start, second.start);
        return startOrder != 0 ? startOrder : Long.compare(first.sequence, second.sequence);
    }

    private static int compareTags(ExactTag first, ExactTag second) {
        try {
            return first.compareExact(second);
        } catch (NumericLimitException impossible) {
            throw new AssertionError("persistent tag comparison must fit the transient budget", impossible);
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireIndexChange(boolean changed, String message) {
        if (!changed) {
            throw new IllegalStateException(message);
        }
    }

    private static final class FlowState<F, J, P> {
        private final FlowHandle handle;
        private final F flowId;
        private final long weight;
        private ExactTag lastFinish = ExactTag.zero();
        private QueuedJob<F, J, P> head;
        private QueuedJob<F, J, P> tail;
        private int queuedCount;
        private int runningCount;
        private BigInteger acceptedCost = BigInteger.ZERO;
        private BigInteger dispatchedCost = BigInteger.ZERO;
        private BigInteger cancelledCost = BigInteger.ZERO;

        private FlowState(FlowHandle handle, F flowId, long weight) {
            this.handle = handle;
            this.flowId = flowId;
            this.weight = weight;
        }
    }

    private static final class QueuedJob<F, J, P> {
        private final JobHandle handle;
        private final J jobId;
        private final FlowState<F, J, P> flow;
        private final P payload;
        private final long cost;
        private final long sequence;
        private ExactTag start;
        private ExactTag finish;
        private QueuedJob<F, J, P> previous;
        private QueuedJob<F, J, P> next;

        private QueuedJob(
                JobHandle handle,
                J jobId,
                FlowState<F, J, P> flow,
                P payload,
                long cost,
                ExactTag start,
                ExactTag finish,
                long sequence) {
            this.handle = handle;
            this.jobId = jobId;
            this.flow = flow;
            this.payload = payload;
            this.cost = cost;
            this.start = start;
            this.finish = finish;
            this.sequence = sequence;
        }
    }

    private static final class RunningJob<J> {
        private final J jobId;
        private final FlowHandle flowHandle;
        private final long cost;

        private RunningJob(J jobId, FlowHandle flowHandle, long cost) {
            this.jobId = jobId;
            this.flowHandle = flowHandle;
            this.cost = cost;
        }
    }

    private static final class EnqueueComputation<F, J, P> {
        private final ExactTag start;
        private final ExactTag finish;
        private final RebasePlan<F, J, P> rebase;

        private EnqueueComputation(ExactTag start, ExactTag finish, RebasePlan<F, J, P> rebase) {
            this.start = start;
            this.finish = finish;
            this.rebase = rebase;
        }
    }

    private static final class RebasePlan<F, J, P> {
        private final Map<FlowState<F, J, P>, ExactTag> flowTags;
        private final Map<QueuedJob<F, J, P>, TagPair> jobTags;

        private RebasePlan(
                Map<FlowState<F, J, P>, ExactTag> flowTags,
                Map<QueuedJob<F, J, P>, TagPair> jobTags) {
            this.flowTags = flowTags;
            this.jobTags = jobTags;
        }
    }

    private static final class TagPair {
        private final ExactTag start;
        private final ExactTag finish;

        private TagPair(ExactTag start, ExactTag finish) {
            this.start = start;
            this.finish = finish;
        }
    }
}
