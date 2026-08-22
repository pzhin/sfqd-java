package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.Objects;

/** Immutable observation of one registered flow created only by a scheduler implementation. */
public final class FlowSnapshot {
    private final int queuedJobs;
    private final int runningJobs;
    private final BigInteger acceptedCost;
    private final BigInteger dispatchedCost;
    private final BigInteger cancelledCost;
    private final BigInteger runningSuppliedCost;

    FlowSnapshot(
            int queuedJobs,
            int runningJobs,
            BigInteger acceptedCost,
            BigInteger dispatchedCost,
            BigInteger cancelledCost,
            BigInteger runningSuppliedCost) {
        this.queuedJobs = queuedJobs;
        this.runningJobs = runningJobs;
        this.acceptedCost = Objects.requireNonNull(acceptedCost, "acceptedCost");
        this.dispatchedCost = Objects.requireNonNull(dispatchedCost, "dispatchedCost");
        this.cancelledCost = Objects.requireNonNull(cancelledCost, "cancelledCost");
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
     * @return cumulative accepted cost units
     */
    public BigInteger acceptedCost() {
        return acceptedCost;
    }

    /**
     * Returns the exact cumulative supplied cost of jobs dispatched for the flow registration.
     *
     * @return cumulative dispatched cost units
     */
    public BigInteger dispatchedCost() {
        return dispatchedCost;
    }

    /**
     * Returns the exact cumulative supplied cost of successfully cancelled jobs for the flow registration.
     *
     * @return cumulative cancelled cost units
     */
    public BigInteger cancelledCost() {
        return cancelledCost;
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
        return dispatchedCost.subtract(runningSuppliedCost);
    }

    /**
     * Returns the exact total cost of jobs currently queued for the flow.
     *
     * <p>This is derived from cumulative lifecycle costs as accepted minus dispatched minus cancelled. Completion
     * does not affect it because dispatched cost includes both running and completed jobs.
     *
     * @return current queued cost units
     */
    public BigInteger queuedCost() {
        return acceptedCost.subtract(dispatchedCost).subtract(cancelledCost);
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
                && acceptedCost.equals(snapshot.acceptedCost)
                && dispatchedCost.equals(snapshot.dispatchedCost)
                && cancelledCost.equals(snapshot.cancelledCost)
                && runningSuppliedCost.equals(snapshot.runningSuppliedCost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                queuedJobs, runningJobs, acceptedCost, dispatchedCost, cancelledCost, runningSuppliedCost);
    }

    @Override
    public String toString() {
        return "FlowSnapshot[queuedJobs=" + queuedJobs
                + ", runningJobs=" + runningJobs
                + ", acceptedCost=" + acceptedCost
                + ", dispatchedCost=" + dispatchedCost
                + ", cancelledCost=" + cancelledCost
                + ", runningSuppliedCost=" + runningSuppliedCost + ']';
    }
}
