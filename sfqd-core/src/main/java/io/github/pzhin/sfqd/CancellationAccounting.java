package io.github.pzhin.sfqd;

/**
 * Defines how a successful queued-job cancellation affects virtual fairness accounting.
 *
 * <p>The current implementation supports only {@link #CHARGE_RESERVED_COST}. The explicit policy prevents a queued
 * cancellation from being mistaken for a free rollback of the job's scheduling tags.
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
    CHARGE_RESERVED_COST
}
