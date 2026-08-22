package io.github.pzhin.sfqd.benchmarks;

import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.PAYLOAD;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireAccepted;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCancelled;
import static io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.requireCompleted;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.FirstBusyPeriodFixture;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.TerminalFixture;
import io.github.pzhin.sfqd.benchmarks.IdleResetBenchmarkSupport.TerminalOperation;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Fixture;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.FlowKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobKey;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.JobRecord;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Payload;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.Scenario;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Isolated public-operation latency benchmarks; invocation lifecycle work is outside the JMH timer. */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class OperationLatencyBenchmark {

    /** Common full-matrix parameters for operation states. */
    @State(Scope.Thread)
    public abstract static class MatrixState {
        /** Configured/registered flow count; ONE_HOT keeps only flow zero active. */
        @Param({"1", "10", "100", "1000", "10000"})
        private int flowCount;

        /** Scheduler issue depth. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        /** Deterministic flow/job distribution. */
        @Param
        private Scenario scenario;

        int flowCount() {
            return flowCount;
        }

        int depth() {
            return depth;
        }

        Scenario scenario() {
            return scenario;
        }

        int expectedActiveFlows() {
            return SchedulerBenchmarkSupport.expectedActiveFlows(flowCount, scenario);
        }
    }

    /** State for enqueue into an already backlogged flow. */
    @State(Scope.Thread)
    public static class EnqueueBackloggedState extends MatrixState {
        private Fixture fixture;
        private JobRecord[] candidates;
        private JobRecord candidate;
        private EnqueueResult result;
        private int nextFlow;
        private boolean observedNonzeroFlow;

        /** Builds the bounded active-flow population outside measurements. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount(), depth(), scenario(), 0);
            candidates = new JobRecord[flowCount()];
            for (int index = 0; index < candidates.length; index++) {
                candidates[index] = fixture.allocateRecord(index);
            }
        }

        /** Selects flows round-robin, except that a one-hot workload can only target flow zero. */
        @Setup(Level.Invocation)
        public void selectTarget() {
            int flow = scenario() == Scenario.ONE_HOT ? 0 : nextFlow++ % flowCount();
            observedNonzeroFlow |= flow != 0;
            candidate = candidates()[flow];
        }

        /** Cancels the measured admission so every invocation starts with the same cardinalities. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            candidate().replaceHandle(requireAccepted(result()));
            requireCancelled(fixture().scheduler().cancel(candidate().handle()));
            result = null;
        }

        /** Checks bounded shape and cumulative counter conservation after an iteration. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(fixture().initialQueued(), expectedActiveFlows());
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.acceptedTotal() != fixture().initialQueued() + snapshot.cancelledTotal()) {
                throw new IllegalStateException("enqueue/cancel counters diverged: " + snapshot);
            }
            requireSkewRotation(observedNonzeroFlow);
        }

        EnqueueResult enqueue() {
            result = fixture().scheduler().enqueue(
                    fixture().flowHandle(candidate().flowIndex()),
                    candidate().jobId(), PAYLOAD, candidate().cost());
            return result;
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private JobRecord candidate() {
            return Objects.requireNonNull(candidate, "trial setup did not run");
        }

        private EnqueueResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }

        private JobRecord[] candidates() {
            return Objects.requireNonNull(candidates, "trial setup did not run");
        }

        private void requireSkewRotation(boolean observed) {
            if (flowCount() > 1 && scenario() != Scenario.ONE_HOT && !observed) {
                throw new IllegalStateException("backlogged enqueue never exercised a nonzero flow index");
            }
        }
    }

    /** State for activation by enqueue into an otherwise inactive registered flow. */
    @State(Scope.Thread)
    public static class EnqueueInactiveState extends MatrixState {
        private Fixture fixture;
        private JobRecord candidate;
        private EnqueueResult result;

        /** Builds active flows plus one inactive registration outside measurements. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount(), depth(), scenario(), 1);
            candidate = fixture.allocateRecord(flowCount());
        }

        /** Cancels the measured admission, returning the extra flow to inactive state. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            candidate().replaceHandle(requireAccepted(result()));
            requireCancelled(fixture().scheduler().cancel(candidate().handle()));
            result = null;
        }

        /** Checks the inactive registration does not change the configured active population. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(fixture().initialQueued(), expectedActiveFlows());
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.acceptedTotal() != fixture().initialQueued() + snapshot.cancelledTotal()) {
                throw new IllegalStateException("inactive enqueue counters diverged: " + snapshot);
            }
        }

        EnqueueResult enqueue() {
            result = fixture().scheduler().enqueue(
                    fixture().flowHandle(flowCount()), candidate().jobId(), PAYLOAD, candidate().cost());
            return result;
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private JobRecord candidate() {
            return Objects.requireNonNull(candidate, "trial setup did not run");
        }

        private EnqueueResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** State for atomic dispatch calls. */
    @State(Scope.Thread)
    public static class DispatchState extends MatrixState {
        private Fixture fixture;
        private List<Dispatch<FlowKey, JobKey, Payload>> dispatched = List.of();
        private int requestedCapacity;

        /** Builds enough queued work to make both one-job and full-depth dispatch deterministic. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount(), depth(), scenario(), 0);
        }

        /** Completes measured dispatches and re-enqueues their preallocated identifiers outside the timer. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            if (dispatched.size() != requestedCapacity) {
                throw new IllegalStateException(
                        "expected full dispatch of " + requestedCapacity + ", got " + dispatched.size());
            }
            fixture.restoreDispatches(dispatched);
            dispatched = List.of();
        }

        /** Checks dispatch/complete balance and bounded cardinalities. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture.assertSteadyShape(fixture.initialQueued(), expectedActiveFlows());
            SchedulerSnapshot snapshot = fixture.scheduler().snapshot();
            if (snapshot.dispatchedTotal() != snapshot.completedTotal()
                    || snapshot.acceptedTotal() != fixture.initialQueued() + snapshot.completedTotal()) {
                throw new IllegalStateException("dispatch restoration counters diverged: " + snapshot);
            }
        }

        List<Dispatch<FlowKey, JobKey, Payload>> dispatch(int capacity) {
            requestedCapacity = capacity;
            dispatched = fixture.scheduler().capacityAvailable(capacity);
            return dispatched;
        }
    }

    /** State for cancellation of a known head or non-head queued job. */
    @State(Scope.Thread)
    public static class CancellationState extends MatrixState {
        /** Cancellation position selected by the JMH parameter. */
        @Param({"HEAD", "NON_HEAD"})
        private CancelPosition position;

        private Fixture fixture;
        private JobRecord target;
        private CancelResult result;
        private int targetFlow;
        private int nextFlow;
        private boolean observedNonzeroFlow;

        /** Builds per-flow queues with at least two entries. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount(), depth(), scenario(), 0, 2);
        }

        /** Selects a stable queued target without charging caller lookup to scheduler latency. */
        @Setup(Level.Invocation)
        public void selectTarget() {
            targetFlow = scenario() == Scenario.ONE_HOT ? 0 : nextFlow++ % flowCount();
            observedNonzeroFlow |= targetFlow != 0;
            ArrayDeque<JobRecord> queue = fixture().queue(targetFlow);
            target = position == CancelPosition.HEAD ? queue.getFirst() : queue.getLast();
        }

        /** Re-enqueues the cancelled preallocated identifier as a tail job. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            requireCancelled(result());
            ArrayDeque<JobRecord> queue = fixture().queue(targetFlow);
            JobRecord removed = position == CancelPosition.HEAD ? queue.removeFirst() : queue.removeLast();
            if (removed != target) {
                throw new IllegalStateException("caller queue target changed");
            }
            fixture().enqueueRecord(target());
            result = null;
        }

        /** Checks cancellation/enqueue conservation and bounded cardinalities. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().assertSteadyShape(fixture().initialQueued(), expectedActiveFlows());
            SchedulerSnapshot snapshot = fixture().scheduler().snapshot();
            if (snapshot.acceptedTotal() != fixture().initialQueued() + snapshot.cancelledTotal()) {
                throw new IllegalStateException("cancel restoration counters diverged: " + snapshot);
            }
            if (flowCount() > 1 && scenario() != Scenario.ONE_HOT && !observedNonzeroFlow) {
                throw new IllegalStateException("cancellation never exercised a nonzero flow index");
            }
        }

        CancelResult cancel() {
            result = fixture().scheduler().cancel(target().handle());
            return result;
        }

        private Fixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private JobRecord target() {
            return Objects.requireNonNull(target, "invocation setup did not run");
        }

        private CancelResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** State for completion while other jobs keep the same busy period alive. */
    @State(Scope.Thread)
    public static class SteadyCompletionState extends MatrixState {
        private Fixture fixture;
        private JobRecord running;
        private CompletionResult result;

        /** Builds the queue and establishes exactly one running job. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new Fixture(flowCount(), depth(), scenario(), 0);
            establishRunning();
        }

        /** Replaces the completed ID, then establishes the next running job outside the timer. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            requireCompleted(result());
            fixture.enqueueRecord(running);
            establishRunning();
            result = null;
        }

        /** Checks exactly one job remains running and all operations balance. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            SchedulerSnapshot snapshot = fixture.scheduler().snapshot();
            if (snapshot.queuedJobs() != fixture.initialQueued() - 1
                    || snapshot.runningJobs() != 1
                    || snapshot.activeFlows() != expectedActiveFlows()
                    || snapshot.backloggedFlows() != fixture.backloggedFlowCount()
                    || snapshot.dispatchedTotal() != snapshot.completedTotal() + 1
                    || snapshot.acceptedTotal() != fixture.initialQueued() + snapshot.completedTotal()) {
                throw new IllegalStateException("steady completion state diverged: " + snapshot);
            }
        }

        CompletionResult complete() {
            result = fixture.scheduler().complete(running.handle());
            return result;
        }

        private CompletionResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }

        private void establishRunning() {
            List<Dispatch<FlowKey, JobKey, Payload>> dispatches = fixture.scheduler().capacityAvailable(1);
            if (dispatches.size() != 1) {
                throw new IllegalStateException("steady completion requires one dispatch");
            }
            Dispatch<FlowKey, JobKey, Payload> dispatch = dispatches.getFirst();
            running = SchedulerBenchmarkSupport.removeByHandle(
                    fixture.queue(dispatch.flowId().index()), dispatch.jobHandle());
        }
    }

    /** State measuring the normative global-idle reset on the last completion. */
    @State(Scope.Thread)
    public static class LastCompletionState {
        /** Number of registered flows whose dormant tags are reset; only flow zero is active. */
        @Param({"1", "10", "100", "1000", "10000"})
        private int flowCount;

        /** Scheduler issue depth, although exactly one slot is occupied in this operation. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        private TerminalFixture fixture;
        private CompletionResult result;

        /** Registers the reset population and establishes the sole live running job. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new TerminalFixture(flowCount, depth, false, TerminalOperation.COMPLETE);
        }

        /** Re-establishes the sole running job after the measured idle reset. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restoreAfterCompletion(result());
            result = null;
        }

        /** Checks the fixture returns to one running job without queued metadata. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyPrepared();
        }

        CompletionResult complete() {
            result = fixture().completeLastRunning();
            return result;
        }

        private CompletionResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }

        private TerminalFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }
    }

    /** State measuring last-queued cancellation when only its flow has a nonzero tag. */
    @State(Scope.Thread)
    public static class OneTaggedCancellationState {
        /** Number of registered flows reset by the terminal cancellation. */
        @Param({"1", "10", "100", "1000", "10000"})
        private int flowCount;

        /** Scheduler issue depth. */
        @Param({"1", "8", "64", "256"})
        private int depth;

        private TerminalFixture fixture;
        private CancelResult result;

        /** Establishes exactly one queued job while every other registered flow remains untagged. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new TerminalFixture(flowCount, depth, false, TerminalOperation.CANCEL);
        }

        /** Validates the idle transition and restores the exact one-tagged boundary. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restoreAfterCancellation(result());
            result = null;
        }

        /** Checks the caller-visible boundary and cumulative conservation. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyPrepared();
        }

        CancelResult cancel() {
            result = fixture().cancelLastQueued();
            return result;
        }

        private TerminalFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private CancelResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** State measuring last completion after every registered flow received a tag in the same busy period. */
    @State(Scope.Thread)
    public static class AllTaggedCompletionState {
        /** Number of registered and tagged flows. */
        @Param({"1", "100", "10000"})
        private int flowCount;

        /** Representative issue depths, including the D=1 boundary. */
        @Param({"1", "256"})
        private int depth;

        private TerminalFixture fixture;
        private CompletionResult result;

        /** Tags every flow without ending the busy period and leaves the final job running. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new TerminalFixture(flowCount, depth, true, TerminalOperation.COMPLETE);
        }

        /** Validates global idle and re-establishes the all-tagged boundary. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restoreAfterCompletion(result());
            result = null;
        }

        /** Checks the public snapshot and cumulative conservation. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyPrepared();
        }

        CompletionResult complete() {
            result = fixture().completeLastRunning();
            return result;
        }

        private TerminalFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private CompletionResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** State measuring last-queued cancellation after all flows were tagged in one busy period. */
    @State(Scope.Thread)
    public static class AllTaggedCancellationState {
        /** Number of registered and tagged flows. */
        @Param({"1", "100", "10000"})
        private int flowCount;

        /** Representative issue depths, including the D=1 boundary. */
        @Param({"1", "256"})
        private int depth;

        private TerminalFixture fixture;
        private CancelResult result;

        /** Tags every flow without ending the busy period and leaves the final job queued. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new TerminalFixture(flowCount, depth, true, TerminalOperation.CANCEL);
        }

        /** Validates global idle and re-establishes the all-tagged boundary. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restoreAfterCancellation(result());
            result = null;
        }

        /** Checks the public snapshot and cumulative conservation. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyPrepared();
        }

        CancelResult cancel() {
            result = fixture().cancelLastQueued();
            return result;
        }

        private TerminalFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private CancelResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** State measuring the first admission of a fresh busy period. */
    @State(Scope.Thread)
    public static class FirstBusyPeriodState {
        /** Registered-flow population while the scheduler is idle. */
        @Param({"1", "10000"})
        private int flowCount;

        /** Representative issue depths. */
        @Param({"1", "256"})
        private int depth;

        private FirstBusyPeriodFixture fixture;
        private EnqueueResult result;

        /** Registers the requested population and leaves the scheduler globally idle. */
        @Setup(Level.Trial)
        public void setupTrial() {
            fixture = new FirstBusyPeriodFixture(flowCount, depth);
        }

        /** Validates the admission, cancels it, and verifies the next global-idle boundary. */
        @TearDown(Level.Invocation)
        public void restoreInvocation() {
            fixture().restoreAfterEnqueue(result());
            result = null;
        }

        /** Checks the scheduler remains bounded and idle between invocations. */
        @TearDown(Level.Iteration)
        public void verifyIteration() {
            fixture().verifyIdle();
        }

        EnqueueResult enqueue() {
            result = fixture().enqueueFirstBusyPeriod();
            return result;
        }

        private FirstBusyPeriodFixture fixture() {
            return Objects.requireNonNull(fixture, "trial setup did not run");
        }

        private EnqueueResult result() {
            return Objects.requireNonNull(result, "benchmark invocation did not run");
        }
    }

    /** Cancellation position for isolated latency. */
    public enum CancelPosition {
        HEAD,
        NON_HEAD
    }

    /**
     * Measures enqueue into a currently backlogged flow.
     *
     * @param state bounded operation state
     * @return accepted admission result
     */
    @Benchmark
    public EnqueueResult enqueueBackloggedTail(EnqueueBackloggedState state) {
        return state.enqueue();
    }

    /**
     * Measures inactive-to-active flow admission.
     *
     * @param state bounded operation state
     * @return accepted admission result
     */
    @Benchmark
    public EnqueueResult enqueueInactiveFlow(EnqueueInactiveState state) {
        return state.enqueue();
    }

    /**
     * Measures an atomic one-job dispatch.
     *
     * @param state bounded operation state
     * @return the one-job dispatch batch
     */
    @Benchmark
    public List<Dispatch<FlowKey, JobKey, Payload>> dispatchOne(DispatchState state) {
        return state.dispatch(1);
    }

    /**
     * Measures an atomic full-depth dispatch batch.
     *
     * @param state bounded operation state
     * @return the full-depth dispatch batch
     */
    @Benchmark
    public List<Dispatch<FlowKey, JobKey, Payload>> dispatchBatch(DispatchState state) {
        return state.dispatch(state.depth());
    }

    /**
     * Measures cancellation of a known head or non-head according to the state parameter.
     *
     * @param state bounded operation state
     * @return successful cancellation result
     */
    @Benchmark
    public CancelResult cancelQueued(CancellationState state) {
        return state.cancel();
    }

    /**
     * Measures completion without ending the current busy period.
     *
     * @param state bounded operation state
     * @return successful completion result
     */
    @Benchmark
    public CompletionResult completeSteady(SteadyCompletionState state) {
        return state.complete();
    }

    /**
     * Measures completion that triggers the registered-flow idle reset.
     *
     * @param state bounded operation state
     * @return successful completion result
     */
    @Benchmark
    public CompletionResult completeLastRunning(LastCompletionState state) {
        return state.complete();
    }

    /**
     * Measures cancellation that ends a busy period with only the terminal flow tagged.
     *
     * @param state bounded one-tagged cancellation state
     * @return successful cancellation result
     */
    @Benchmark
    public CancelResult cancelLastQueuedOneTagged(OneTaggedCancellationState state) {
        return state.cancel();
    }

    /**
     * Measures completion that resets tags previously established on every registered flow.
     *
     * @param state bounded all-tagged completion state
     * @return successful completion result
     */
    @Benchmark
    public CompletionResult completeLastRunningAllTagged(AllTaggedCompletionState state) {
        return state.complete();
    }

    /**
     * Measures cancellation that resets tags previously established on every registered flow.
     *
     * @param state bounded all-tagged cancellation state
     * @return successful cancellation result
     */
    @Benchmark
    public CancelResult cancelLastQueuedAllTagged(AllTaggedCancellationState state) {
        return state.cancel();
    }

    /**
     * Measures the first admission after a completed global-idle reset.
     *
     * @param state bounded globally idle state
     * @return accepted admission result
     */
    @Benchmark
    public EnqueueResult enqueueFirstBusyPeriod(FirstBusyPeriodState state) {
        return state.enqueue();
    }
}
