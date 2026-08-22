package io.github.pzhin.sfqd;

/** Result of attempting to complete a job. */
public enum CompletionResult {
    /** The running job was completed and its issue slot released. */
    COMPLETED,
    /** The job is live but has not been dispatched. */
    NOT_DISPATCHED,
    /** The capability does not denote a live job at the observation point. */
    NOT_LIVE
}
