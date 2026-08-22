package io.github.pzhin.sfqd;

import java.util.Objects;

/** Result of attempting to register a flow. */
public sealed interface RegisterFlowResult {
    /** Successful registration with its fresh capability. */
    final class Registered implements RegisterFlowResult {
        private final FlowHandle flowHandle;

        Registered(FlowHandle flowHandle) {
            this.flowHandle = Objects.requireNonNull(flowHandle, "flowHandle");
        }

        /**
         * Returns the fresh registration capability.
         *
         * @return flow capability
         */
        public FlowHandle flowHandle() {
            return flowHandle;
        }
    }

    /** Registration rejection with no state mutation. */
    enum Rejected implements RegisterFlowResult {
        /** The flow identifier is already registered. */
        DUPLICATE_REGISTERED_ID,
        /** The configured registration limit is full. */
        FLOW_LIMIT,
        /** The lifetime flow sequence has been exhausted. */
        FLOW_SEQUENCE_EXHAUSTED
    }
}
