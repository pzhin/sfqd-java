package io.github.pzhin.sfqd;

import java.util.Objects;

/** Immutable aggregate observation created only by a scheduler implementation. */
public final class SchedulerSnapshot {
    private final int depth;
    private final int maxFlows;
    private final int maxLiveJobs;
    private final int registeredFlows;
    private final int queuedJobs;
    private final int runningJobs;
    private final int freeSlots;
    private final int activeFlows;
    private final int backloggedFlows;
    private final long acceptedTotal;
    private final long dispatchedTotal;
    private final long cancelledTotal;
    private final long completedTotal;

    SchedulerSnapshot(
            int depth,
            int maxFlows,
            int maxLiveJobs,
            int registeredFlows,
            int queuedJobs,
            int runningJobs,
            int freeSlots,
            int activeFlows,
            int backloggedFlows,
            long acceptedTotal,
            long dispatchedTotal,
            long cancelledTotal,
            long completedTotal) {
        this.depth = depth;
        this.maxFlows = maxFlows;
        this.maxLiveJobs = maxLiveJobs;
        this.registeredFlows = registeredFlows;
        this.queuedJobs = queuedJobs;
        this.runningJobs = runningJobs;
        this.freeSlots = freeSlots;
        this.activeFlows = activeFlows;
        this.backloggedFlows = backloggedFlows;
        this.acceptedTotal = acceptedTotal;
        this.dispatchedTotal = dispatchedTotal;
        this.cancelledTotal = cancelledTotal;
        this.completedTotal = completedTotal;
    }

    /**
     * Returns the configured issue depth.
     *
     * @return configured issue depth
     */
    public int depth() {
        return depth;
    }

    /**
     * Returns the configured flow limit.
     *
     * @return configured flow limit
     */
    public int maxFlows() {
        return maxFlows;
    }

    /**
     * Returns the configured live-job limit.
     *
     * @return configured live-job limit
     */
    public int maxLiveJobs() {
        return maxLiveJobs;
    }

    /**
     * Returns the current registration count.
     *
     * @return current registrations
     */
    public int registeredFlows() {
        return registeredFlows;
    }

    /**
     * Returns the current queued-job count.
     *
     * @return current queued jobs
     */
    public int queuedJobs() {
        return queuedJobs;
    }

    /**
     * Returns the current dispatched-job count.
     *
     * @return current dispatched jobs
     */
    public int runningJobs() {
        return runningJobs;
    }

    /**
     * Returns issue slots that are currently free.
     *
     * @return current free issue slots
     */
    public int freeSlots() {
        return freeSlots;
    }

    /**
     * Returns the number of flows with queued or running jobs.
     *
     * @return active flows
     */
    public int activeFlows() {
        return activeFlows;
    }

    /**
     * Returns the number of flows with queued jobs.
     *
     * @return backlogged flows
     */
    public int backloggedFlows() {
        return backloggedFlows;
    }

    /**
     * Returns the cumulative accepted-job count.
     *
     * @return cumulative accepted jobs
     */
    public long acceptedTotal() {
        return acceptedTotal;
    }

    /**
     * Returns the cumulative dispatched-job count.
     *
     * @return cumulative dispatched jobs
     */
    public long dispatchedTotal() {
        return dispatchedTotal;
    }

    /**
     * Returns the cumulative cancelled-job count.
     *
     * @return cumulative cancelled jobs
     */
    public long cancelledTotal() {
        return cancelledTotal;
    }

    /**
     * Returns the cumulative completed-job count.
     *
     * @return cumulative completed jobs
     */
    public long completedTotal() {
        return completedTotal;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SchedulerSnapshot snapshot)) {
            return false;
        }
        return depth == snapshot.depth
                && maxFlows == snapshot.maxFlows
                && maxLiveJobs == snapshot.maxLiveJobs
                && registeredFlows == snapshot.registeredFlows
                && queuedJobs == snapshot.queuedJobs
                && runningJobs == snapshot.runningJobs
                && freeSlots == snapshot.freeSlots
                && activeFlows == snapshot.activeFlows
                && backloggedFlows == snapshot.backloggedFlows
                && acceptedTotal == snapshot.acceptedTotal
                && dispatchedTotal == snapshot.dispatchedTotal
                && cancelledTotal == snapshot.cancelledTotal
                && completedTotal == snapshot.completedTotal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                depth, maxFlows, maxLiveJobs, registeredFlows, queuedJobs, runningJobs, freeSlots,
                activeFlows, backloggedFlows, acceptedTotal, dispatchedTotal, cancelledTotal, completedTotal);
    }

    @Override
    public String toString() {
        return "SchedulerSnapshot[depth=" + depth
                + ", maxFlows=" + maxFlows
                + ", maxLiveJobs=" + maxLiveJobs
                + ", registeredFlows=" + registeredFlows
                + ", queuedJobs=" + queuedJobs
                + ", runningJobs=" + runningJobs
                + ", freeSlots=" + freeSlots
                + ", activeFlows=" + activeFlows
                + ", backloggedFlows=" + backloggedFlows
                + ", acceptedTotal=" + acceptedTotal
                + ", dispatchedTotal=" + dispatchedTotal
                + ", cancelledTotal=" + cancelledTotal
                + ", completedTotal=" + completedTotal + ']';
    }
}
