package io.github.pzhin.sfqd;

/** Result of attempting to close a flow registration. */
public enum CloseFlowResult {
    /** The registration was removed. */
    CLOSED,
    /** The capability is foreign, stale, or already closed. */
    FLOW_NOT_REGISTERED,
    /** The flow still owns a queued or running job. */
    FLOW_ACTIVE,
    /** Another flow keeps the current busy period active. */
    BUSY_PERIOD_ACTIVE
}
