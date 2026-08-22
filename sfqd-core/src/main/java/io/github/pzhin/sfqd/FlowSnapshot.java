package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.Objects;

/**
 * Immutable observation of one registered flow created only by a scheduler implementation.
 *
 * <p>Every cost value is a caller-supplied scheduling cost, not elapsed time or actual resource service.
 */
public final class FlowSnapshot {
    private final int queuedJobs;
    private final int runningJobs;
    private final BigInteger acceptedSuppliedCost;
    private final BigInteger dispatchedSuppliedCost;
    private final BigInteger cancelledSuppliedCost;
    private final BigInteger runningSuppliedCost;

    FlowSnapshot(
            int queuedJobs,
            int runningJobs,
            BigInteger acceptedSuppliedCost,
            BigInteger dispatchedSuppliedCost,
            BigInteger cancelledSuppliedCost,
            BigInteger runningSuppliedCost) {
        this.queuedJobs = queuedJobs;
        this.runningJobs = runningJobs;
        this.acceptedSuppliedCost = Objects.requireNonNull(acceptedSuppliedCost, "acceptedSuppliedCost");
        this.dispatchedSuppliedCost = Objects.requireNonNull(dispatchedSuppliedCost, "dispatchedSuppliedCost");
        this.cancelledSuppliedCost = Objects.requireNonNull(cancelledSuppliedCost, "cancelledSuppliedCost");
        this.runningSuppliedCost = Objects.requireNonNull(runningSuppliedCost, "runningSuppliedCost");
    }

    /**
     * Returns the current queued-job count for the flow.
     *
     * @return current queued jobs
     */
    public int queuedJobs() {
        return queuedJobs;
    }

    /**
     * Returns the current running-job count for the flow.
     *
     * @return current running jobs
     */
    public int runningJobs() {
        return runningJobs;
    }

    /**
     * Returns the exact cumulative supplied cost of successfully accepted jobs for the flow registration.
     *
     * @return cumulative accepted supplied-cost units
     */
    public BigInteger acceptedSuppliedCost() {
        return acceptedSuppliedCost;
    }

    /**
     * Returns the exact cumulative supplied cost of jobs dispatched for the flow registration.
     *
     * @return cumulative dispatched supplied-cost units
     */
    public BigInteger dispatchedSuppliedCost() {
        return dispatchedSuppliedCost;
    }

    /**
     * Returns the exact cumulative supplied cost of successfully cancelled jobs for the flow registration.
     *
     * @return cumulative cancelled supplied-cost units
     */
    public BigInteger cancelledSuppliedCost() {
        return cancelledSuppliedCost;
    }

    /**
     * Returns the exact supplied cost of jobs currently running for the flow.
     *
     * <p>This is the caller-supplied service estimate from enqueue, not elapsed or actual execution time.
     *
     * @return current in-flight supplied cost units
     */
    public BigInteger runningSuppliedCost() {
        return runningSuppliedCost;
    }

    /**
     * Returns the exact cumulative supplied cost of completed jobs for the flow.
     *
     * <p>This is derived as dispatched supplied cost minus current running supplied cost.
     *
     * @return cumulative completed supplied cost units
     */
    public BigInteger completedSuppliedCost() {
        return dispatchedSuppliedCost.subtract(runningSuppliedCost);
    }

    /**
     * Returns the exact supplied cost of jobs currently queued for the flow.
     *
     * <p>This is derived from cumulative lifecycle costs as accepted minus dispatched minus cancelled. Completion
     * does not affect it because dispatched cost includes both running and completed jobs.
     *
     * @return current queued supplied-cost units
     */
    public BigInteger queuedSuppliedCost() {
        return acceptedSuppliedCost.subtract(dispatchedSuppliedCost).subtract(cancelledSuppliedCost);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FlowSnapshot snapshot)) {
            return false;
        }
        return queuedJobs == snapshot.queuedJobs
                && runningJobs == snapshot.runningJobs
                && acceptedSuppliedCost.equals(snapshot.acceptedSuppliedCost)
                && dispatchedSuppliedCost.equals(snapshot.dispatchedSuppliedCost)
                && cancelledSuppliedCost.equals(snapshot.cancelledSuppliedCost)
                && runningSuppliedCost.equals(snapshot.runningSuppliedCost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                queuedJobs, runningJobs, acceptedSuppliedCost, dispatchedSuppliedCost,
                cancelledSuppliedCost, runningSuppliedCost);
    }

    @Override
    public String toString() {
        return "FlowSnapshot[queuedJobs=" + queuedJobs
                + ", runningJobs=" + runningJobs
                + ", acceptedSuppliedCost=" + acceptedSuppliedCost
                + ", dispatchedSuppliedCost=" + dispatchedSuppliedCost
                + ", cancelledSuppliedCost=" + cancelledSuppliedCost
                + ", runningSuppliedCost=" + runningSuppliedCost + ']';
    }
}
