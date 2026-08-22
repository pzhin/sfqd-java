package io.github.pzhin.sfqd;

import java.util.Objects;

/** Opaque inert capability for one accepted job incarnation. */
public final class JobHandle {
    private final OwnerToken ownerToken;
    private final long sequence;

    JobHandle(OwnerToken ownerToken, long sequence) {
        this.ownerToken = Objects.requireNonNull(ownerToken, "ownerToken");
        if (sequence <= 0L) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        this.sequence = sequence;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof JobHandle handle
                && ownerToken == handle.ownerToken
                && sequence == handle.sequence;
    }

    @Override
    public int hashCode() {
        return 31 * System.identityHashCode(ownerToken) + Long.hashCode(sequence);
    }

    @Override
    public String toString() {
        return "JobHandle[opaque]";
    }
}
