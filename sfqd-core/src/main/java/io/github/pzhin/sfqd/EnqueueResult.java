package io.github.pzhin.sfqd;

import java.util.Objects;

/** Result of attempting to enqueue a job. */
public sealed interface EnqueueResult {
    /** Successful admission with its fresh job capability. */
    final class Accepted implements EnqueueResult {
        private final JobHandle jobHandle;

        Accepted(JobHandle jobHandle) {
            this.jobHandle = Objects.requireNonNull(jobHandle, "jobHandle");
        }

        /**
         * Returns the fresh job capability.
         *
         * @return job capability
         */
        public JobHandle jobHandle() {
            return jobHandle;
        }
    }

    /**
     * Admission rejection with no state mutation and no consumed job sequence.
     *
     * <p>Every rejected enqueue is an atomic no-op: exact tags, virtual time,
     * flow history, indexes, counters, and configured limits remain unchanged.
     */
    enum Rejected implements EnqueueResult {
        /** The flow capability is not registered by this scheduler. */
        FLOW_NOT_REGISTERED,
        /** The job identifier already denotes a live job. */
        DUPLICATE_LIVE_ID,
        /** The configured live-job limit is full. */
        LIVE_LIMIT,
        /** The lifetime job sequence has been exhausted. */
        SEQUENCE_EXHAUSTED,
        /**
         * Exact tag arithmetic exceeded its fail-closed representation budget.
         *
         * <p>Every canonically reduced numerator and denominator retained in
         * scheduler state is limited to 4096 bits. Each canonical raw or
         * reduced component of an exact primitive is limited to 8193 bits. If
         * the initial new start or finish tag exceeds the persistent budget,
         * enqueue makes exactly one canonical rebase attempt on temporary
         * state, covering virtual time, every queued tag, and every registered
         * flow's finish history. The rebase and admission either commit
         * together or this result discards the whole computation. No rounding,
         * partial/proactive rebase, state mutation, or sequence consumption is
         * permitted.
         */
        NUMERIC_LIMIT
    }
}
