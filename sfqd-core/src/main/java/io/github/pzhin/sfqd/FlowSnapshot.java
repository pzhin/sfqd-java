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

    FlowSnapshot(
            int queuedJobs,
            int runningJobs,
            BigInteger acceptedCost,
            BigInteger dispatchedCost,
            BigInteger cancelledCost) {
        this.queuedJobs = queuedJobs;
        this.runningJobs = runningJobs;
        this.acceptedCost = Objects.requireNonNull(acceptedCost, "acceptedCost");
        this.dispatchedCost = Objects.requireNonNull(dispatchedCost, "dispatchedCost");
        this.cancelledCost = Objects.requireNonNull(cancelledCost, "cancelledCost");
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
     * Returns the current dispatched-job count for the flow.
     *
     * @return current dispatched jobs
     */
    public int runningJobs() {
        return runningJobs;
    }

    /**
     * Returns the exact cumulative cost of successfully accepted jobs for the flow registration.
     *
     * @return cumulative accepted cost units
     */
    public BigInteger acceptedCost() {
        return acceptedCost;
    }

    /**
     * Returns the exact cumulative cost of jobs dispatched for the flow registration.
     *
     * @return cumulative dispatched cost units
     */
    public BigInteger dispatchedCost() {
        return dispatchedCost;
    }

    /**
     * Returns the exact cumulative cost of successfully cancelled jobs for the flow registration.
     *
     * @return cumulative cancelled cost units
     */
    public BigInteger cancelledCost() {
        return cancelledCost;
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
                && cancelledCost.equals(snapshot.cancelledCost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queuedJobs, runningJobs, acceptedCost, dispatchedCost, cancelledCost);
    }

    @Override
    public String toString() {
        return "FlowSnapshot[queuedJobs=" + queuedJobs
                + ", runningJobs=" + runningJobs
                + ", acceptedCost=" + acceptedCost
                + ", dispatchedCost=" + dispatchedCost
                + ", cancelledCost=" + cancelledCost + ']';
    }
}
