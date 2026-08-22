package io.github.pzhin.sfqd;

/** Result of attempting to close a flow registration. */
public enum CloseFlowResult {
    /** The registration was removed. */
    CLOSED,
    /** The capability is foreign, stale, or already closed. */
    FLOW_NOT_REGISTERED,
    /** The flow still owns a queued or running job. */
    FLOW_ACTIVE,
    /** The inactive flow still has finish-tag debt beyond current virtual time. */
    FAIRNESS_DEBT_ACTIVE
}
