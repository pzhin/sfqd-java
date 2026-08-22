# Minimal executor lifecycle example

This example demonstrates the scheduling lifecycle and is not a reusable
production executor implementation. It is documentation-only and is not
compiled into or included in `sfqd-core`.

`SfqdScheduler` decides which jobs become running but deliberately does not
own an executor, resource pool, callback, or lease. This caller-side pump
sketch illustrates how the scheduler can be coupled to real capacity.

Call the pump after all three events that can make progress possible:

1. an `enqueue` returns `Accepted`;
2. a dispatched job is completed;
3. an external resource or executor slot becomes available.

The following pattern uses a semaphore as the external-capacity ledger. A
permit must be signalled exactly once when a real slot becomes available and
remains reserved from `dispatchUpTo` until completion. The number of signalled
slots must never exceed the scheduler depth.

```java
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.SfqdScheduler;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

final class ExecutorPump<F, J> {
    private final SfqdScheduler<F, J, Runnable> scheduler;
    private final Executor executor;
    private final Semaphore capacity = new Semaphore(0);
    private final AtomicInteger workInProgress = new AtomicInteger();

    ExecutorPump(SfqdScheduler<F, J, Runnable> scheduler, Executor executor) {
        this.scheduler = scheduler;
        this.executor = executor;
    }

    EnqueueResult enqueue(
            FlowHandle flow, J jobId, Runnable payload, long cost) {
        EnqueueResult result = scheduler.enqueue(flow, jobId, payload, cost);
        if (result instanceof EnqueueResult.Accepted) {
            pump();
        }
        return result;
    }

    // Call once per newly available executor or resource-pool slot.
    void onCapacityAvailable(int newSlots) {
        if (newSlots <= 0) {
            throw new IllegalArgumentException("newSlots must be positive");
        }
        capacity.release(newSlots);
        pump();
    }

    private void pump() {
        if (workInProgress.getAndIncrement() != 0) {
            return;
        }
        int missed = 1;
        do {
            drainOnce();
            missed = workInProgress.addAndGet(-missed);
        } while (missed != 0);
    }

    private void drainOnce() {
        int available = capacity.drainPermits();
        if (available == 0) {
            return;
        }

        List<Dispatch<F, J, Runnable>> batch =
                scheduler.dispatchUpTo(available);
        capacity.release(available - batch.size());
        for (Dispatch<F, J, Runnable> dispatch : batch) {
            submit(dispatch);
        }
    }

    private void submit(Dispatch<F, J, Runnable> dispatch) {
        try {
            executor.execute(() -> {
                try {
                    dispatch.payload().run();
                } finally {
                    complete(dispatch);
                }
            });
        } catch (RejectedExecutionException rejected) {
            // Dispatch is irreversible even though the executor rejected it.
            completeWithoutRestoringCapacity(dispatch);
            recordSubmissionFailure(dispatch, rejected);
        }
    }

    private void complete(Dispatch<F, J, Runnable> dispatch) {
        requireCompleted(dispatch);
        capacity.release();
        pump();
    }

    private void completeWithoutRestoringCapacity(
            Dispatch<F, J, Runnable> dispatch) {
        requireCompleted(dispatch);
        // The rejected executor slot is not considered available again.
        // A later external recovery signal calls onCapacityAvailable(...).
        pump();
    }

    private void requireCompleted(Dispatch<F, J, Runnable> dispatch) {
        if (scheduler.complete(dispatch.jobHandle())
                != CompletionResult.COMPLETED) {
            throw new IllegalStateException("dispatch was not running");
        }
    }

    private void recordSubmissionFailure(
            Dispatch<F, J, Runnable> dispatch,
            RejectedExecutionException rejected) {
        // Report the failed job through application-owned error handling.
        // A retry is a new enqueue with a new job incarnation.
    }
}
```

For a fixed executor with `D` immediately available workers, initialize the
example by calling `onCapacityAvailable(D)` once. For a connection pool, signal
each connection only when ownership transfers to the pump, and return it in
the completion path before pumping again.

Executor rejection is not rollback: the example completes the already-running
scheduler handle but does not restore the rejected external slot. Recovery may
signal capacity later. The core remains independent of this policy, and a
future adapter can package equivalent pump or lease semantics without adding
callbacks or scheduler references to `Dispatch`.
