package io.github.pzhin.sfqd;

/**
 * Internal checked signal that an exact numeric operation exceeded a normative
 * budget before a value could be stored.
 */
final class NumericLimitException extends Exception {
    private static final long serialVersionUID = 1L;

    /** Numeric budget that rejected the operation. */
    enum Budget {
        /** Canonically reduced stored component exceeded 4096 bits. */
        PERSISTENT,
        /** Canonical mathematical raw quantity exceeded 8193 bits. */
        TRANSIENT
    }

    private final Budget budget;
    private final String quantity;

    NumericLimitException(Budget budget, String quantity, int actualBits, int maximumBits) {
        super(quantity + " requires " + actualBits + " bits; " + budget
                + " budget permits at most " + maximumBits);
        this.budget = budget;
        this.quantity = quantity;
    }

    Budget budget() {
        return budget;
    }

    String quantity() {
        return quantity;
    }
}
