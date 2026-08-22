package io.github.pzhin.sfqd;

import java.util.Objects;

/**
 * Immutable detached identity carrier for one irrevocably dispatched job.
 *
 * @param <F> flow identifier type
 * @param <J> job identifier type
 * @param <P> payload type
 */
public final class Dispatch<F, J, P> {
    private final JobHandle jobHandle;
    private final J jobId;
    private final F flowId;
    private final P payload;
    private final long cost;

    Dispatch(JobHandle jobHandle, J jobId, F flowId, P payload, long cost) {
        this.jobHandle = Objects.requireNonNull(jobHandle, "jobHandle");
        this.jobId = Objects.requireNonNull(jobId, "jobId");
        this.flowId = Objects.requireNonNull(flowId, "flowId");
        this.payload = Objects.requireNonNull(payload, "payload");
        if (cost <= 0L) {
            throw new IllegalArgumentException("cost must be positive");
        }
        this.cost = cost;
    }

    /**
     * Returns the capability required for completion.
     *
     * @return job capability
     */
    public JobHandle jobHandle() {
        return jobHandle;
    }

    /**
     * Returns the caller's job identifier.
     *
     * @return job identifier
     */
    public J jobId() {
        return jobId;
    }

    /**
     * Returns the caller's flow identifier.
     *
     * @return flow identifier
     */
    public F flowId() {
        return flowId;
    }

    /**
     * Returns the caller payload by identity.
     *
     * @return payload
     */
    public P payload() {
        return payload;
    }

    /**
     * Returns the supplied positive cost.
     *
     * @return cost
     */
    public long cost() {
        return cost;
    }
}
