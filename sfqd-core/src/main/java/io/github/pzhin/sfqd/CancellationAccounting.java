package io.github.pzhin.sfqd;

/**
 * Defines how a successful queued-job cancellation affects virtual fairness accounting.
 *
 * <p>{@link #CHARGE_RESERVED_COST} preserves accepted virtual debt and remains the default.
 * {@link #REFUND_CANCELLED_COST} is opt-in and prospectively recomputes later queued work of the same flow.
 */
public enum CancellationAccounting {
    /**
     * Keeps the cancelled job's reserved virtual cost until the current global busy period ends.
     *
     * <p>Cancellation removes the job and releases its payload, but does not reduce its flow's finish history or
     * recompute tags already assigned to later jobs. The charge disappears when no live job remains and the scheduler
     * performs its global idle reset. Completed-work fairness guarantees do not apply to traces containing a
     * cancellation.
     */
    CHARGE_RESERVED_COST,

    /**
     * Refunds a cancelled queued job's virtual cost from later queued work of the same flow.
     *
     * <p>Admission reserves sufficient exact-arithmetic budget so every later queued cancellation can recompute the
     * affected suffix without a numeric failure. Cancellation does not revise earlier dispatch decisions, and
     * completed-work fairness guarantees do not apply to traces containing cancellation.
     */
    REFUND_CANCELLED_COST
}
