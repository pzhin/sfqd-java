package io.github.pzhin.sfqd.examples;

import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.util.List;
import java.util.Objects;

/**
 * Pull-style example that connects an SFQ(D) scheduler to a bounded external resource pool.
 *
 * <p>The application remains responsible for computing cost and admitting payloads with
 * {@link SfqdScheduler#enqueue}. This example only demonstrates the dispatch/completion boundary. The configured
 * scheduler depth must equal the number of resources that the pool can issue concurrently. Callers report only
 * resources that are actually free and must not report the same free resource more than once.
 *
 * <p>This class is not a reusable production resource-pool adapter.
 *
 * <p>Each call pulls at most one dispatch at a time before handing it to the pool. This is intentional: a batch is
 * irrevocably running when {@link SfqdScheduler#dispatchUpTo} returns, so pulling a whole batch before submitting
 * its first item could strand later dispatches if that first submission throws.
 *
 * @param <F> flow identifier type
 * @param <J> job identifier type
 * @param <T> application payload type
 */
public final class BoundedResourcePoolIntegration<F, J, T> {
    private final SfqdScheduler<F, J, T> scheduler;
    private final ResourcePool<T> resourcePool;
    private final int resourceParallelism;

    /**
     * Creates an example integration for a scheduler and resource pool with the same concurrency bound.
     *
     * @param scheduler application-owned scheduler
     * @param resourcePool application-owned bounded resource pool
     * @param resourceParallelism maximum resources that the pool can issue concurrently
     * @throws NullPointerException if scheduler or resourcePool is null
     * @throws IllegalArgumentException if resourceParallelism is not positive or does not equal scheduler depth
     */
    public BoundedResourcePoolIntegration(
            SfqdScheduler<F, J, T> scheduler,
            ResourcePool<T> resourcePool,
            int resourceParallelism) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.resourcePool = Objects.requireNonNull(resourcePool, "resourcePool");
        if (resourceParallelism <= 0) {
            throw new IllegalArgumentException("resourceParallelism must be positive");
        }
        if (scheduler.snapshot().depth() != resourceParallelism) {
            throw new IllegalArgumentException("scheduler depth must equal resourceParallelism");
        }
        this.resourceParallelism = resourceParallelism;
    }

    /**
     * Offers only the resources that the caller has observed to be free right now.
     *
     * <p>Every returned dispatch is already running. A synchronous pool rejection therefore completes the handle; it
     * never rolls the dispatch back. Once an accepted task terminates, the pool callback completes the handle and
     * immediately offers the released resource again. If fewer jobs are available than resources, the return value
     * identifies the resources that remain free; this example never acquires or retains them.
     *
     * <p>The example does not define coordination between concurrent capacity signals; correct physical capacity
     * accounting and caller-side coordination remain the application's responsibility. A terminal callback must not
     * be invoked synchronously from {@link ResourcePool#execute}.
     *
     * @param available resources observed to be free, in {@code [0, resourceParallelism]}
     * @return reported resources not assigned to a dispatch
     * @throws IllegalArgumentException if available is outside {@code [0, resourceParallelism]}
     * @throws RuntimeException if the resource pool rejects a submission
     */
    public int onResourcesAvailable(int available) {
        if (available < 0 || available > resourceParallelism) {
            throw new IllegalArgumentException("available must be in [0, resourceParallelism]");
        }
        int remaining = available;
        while (remaining > 0) {
            List<Dispatch<F, J, T>> dispatches = scheduler.dispatchUpTo(1);
            if (dispatches.isEmpty()) {
                break;
            }
            Dispatch<F, J, T> dispatch = dispatches.get(0);
            remaining--;
            submit(dispatch);
        }
        return remaining;
    }

    private void submit(Dispatch<F, J, T> dispatch) {
        try {
            resourcePool.execute(dispatch.payload(), () -> onTerminal(dispatch));
        } catch (RuntimeException | Error submissionFailure) {
            completeRequired(dispatch);
            throw submissionFailure;
        }
    }

    private void onTerminal(Dispatch<F, J, T> dispatch) {
        completeRequired(dispatch);
        onResourcesAvailable(1);
    }

    private void completeRequired(Dispatch<F, J, T> dispatch) {
        CompletionResult result = scheduler.complete(dispatch.jobHandle());
        if (result != CompletionResult.COMPLETED) {
            throw new IllegalStateException("resource pool violated the exactly-once terminal callback contract");
        }
    }

    /**
     * Minimal boundary implemented by an application-specific bounded resource pool.
     *
     * <p>If submission is accepted, this method returns normally and the pool must later invoke {@code onTerminal}
     * exactly once after the task reaches any terminal state, including success, failure, or cancellation. The pool
     * must not invoke the callback synchronously from {@code execute}. If submission is rejected, it must throw
     * synchronously without retaining the task and must never invoke the callback.
     *
     * @param <T> task payload type
     */
    @FunctionalInterface
    public interface ResourcePool<T> {
        /**
         * Submits one task to a resource that the caller has observed to be free.
         *
         * @param task application payload
         * @param onTerminal exactly-once terminal callback for an accepted task, invoked only after this method returns
         * @throws RuntimeException if the task is not accepted
         */
        void execute(T task, Runnable onTerminal);
    }
}
