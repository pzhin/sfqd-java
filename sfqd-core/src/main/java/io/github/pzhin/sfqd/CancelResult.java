package io.github.pzhin.sfqd;

/** Result of attempting to cancel a queued job. */
public enum CancelResult {
    /** The queued job was removed. */
    CANCELLED,
    /** The job was already dispatched and cannot be revoked. */
    TOO_LATE_ALREADY_DISPATCHED,
    /**
     * The capability does not denote a live job at the observation point.
     * This intentionally conflates never-existing, stale, foreign, cancelled,
     * and completed handles. Alone it does not identify the winner of a
     * cancel-versus-dispatch race; callers must use the combined operation history.
     */
    NOT_LIVE
}
