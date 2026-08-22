package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.Objects;

/**
 * Internal immutable exact non-negative rational used for SFQ(D) tags.
 *
 * <p>Values are always stored in reduced form with a positive denominator;
 * zero is represented only as {@code 0/1}. Neither floating point nor decimal
 * rounding participates in construction, arithmetic, or ordering.
 *
 * <p>For operands with at most {@code b} bits per component, comparison and
 * {@link #max(ExactTag)} require two integer multiplications. Addition and
 * subtraction require a constant number of multiplications plus a greatest
 * common divisor reduction. Storage is {@code O(b)}; no historical metadata is
 * retained. The normative budgets cap persistent components at 4096 bits and
 * canonical mathematical raw quantities at 8193 bits.
 */
final class ExactTag {
    static final int MAX_PERSISTENT_BITS = 4096;
    static final int MAX_TRANSIENT_BITS = 8193;

    private static final ExactTag ZERO = new ExactTag(BigInteger.ZERO, BigInteger.ONE);

    private final BigInteger numerator;
    private final BigInteger denominator;

    private ExactTag(BigInteger numerator, BigInteger denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }

    /**
     * Returns canonical exact zero.
     *
     * @return the shared immutable {@code 0/1} value
     */
    static ExactTag zero() {
        return ZERO;
    }

    /**
     * Creates the exact normalized job increment {@code cost / weight}.
     *
     * @param cost supplied job cost in {@code [1, Long.MAX_VALUE]}
     * @param weight flow weight in {@code [1, Long.MAX_VALUE]}
     * @return a reduced exact increment
     * @throws IllegalArgumentException if either argument is not positive
     * @throws NumericLimitException if the reduced value exceeds the persistent budget
     */
    static ExactTag fromCostAndWeight(long cost, long weight) throws NumericLimitException {
        if (cost <= 0L) {
            throw new IllegalArgumentException("cost must be in [1, Long.MAX_VALUE]");
        }
        if (weight <= 0L) {
            throw new IllegalArgumentException("weight must be in [1, Long.MAX_VALUE]");
        }
        return reduceAndCheckPersistent(BigInteger.valueOf(cost), BigInteger.valueOf(weight));
    }

    /**
     * Creates a canonical persistent value from exact components.
     *
     * <p>The persistent limit is applied after reduction. This internal factory
     * exists for exact state transformations and boundary verification; caller
     * inputs are not interpreted as raw quantities of an arithmetic primitive.
     *
     * @param numerator non-negative exact numerator
     * @param denominator positive exact denominator
     * @return the reduced persistent value
     * @throws NullPointerException if either component is null
     * @throws IllegalArgumentException if either component has an invalid sign
     * @throws NumericLimitException if a reduced component exceeds 4096 bits
     */
    static ExactTag fromComponents(BigInteger numerator, BigInteger denominator)
            throws NumericLimitException {
        Objects.requireNonNull(numerator, "numerator");
        Objects.requireNonNull(denominator, "denominator");
        validateSigns(numerator, denominator);
        return reduceAndCheckPersistent(numerator, denominator);
    }

    BigInteger numerator() {
        return numerator;
    }

    BigInteger denominator() {
        return denominator;
    }

    /**
     * Adds another persistent exact tag.
     *
     * @param other value to add
     * @return exact reduced sum
     * @throws NullPointerException if other is null
     * @throws NumericLimitException if a canonical raw quantity exceeds 8193
     *         bits or a reduced result component exceeds 4096 bits
     */
    ExactTag add(ExactTag other) throws NumericLimitException {
        Objects.requireNonNull(other, "other");
        BigInteger rawNumerator = numerator.multiply(other.denominator)
                .add(other.numerator.multiply(denominator));
        BigInteger rawDenominator = denominator.multiply(other.denominator);
        checkTransient(rawNumerator, "add raw numerator");
        checkTransient(rawDenominator, "add raw denominator");
        return reduceAndCheckPersistent(rawNumerator, rawDenominator);
    }

    /**
     * Subtracts another value when the mathematical result is non-negative.
     *
     * @param other value to subtract
     * @return exact reduced difference
     * @throws NullPointerException if other is null
     * @throws IllegalArgumentException if the exact result would be negative
     * @throws NumericLimitException if a comparison/raw quantity exceeds 8193
     *         bits or a reduced result component exceeds 4096 bits
     */
    ExactTag subtractNonNegative(ExactTag other) throws NumericLimitException {
        Objects.requireNonNull(other, "other");
        BigInteger leftProduct = numerator.multiply(other.denominator);
        BigInteger rightProduct = other.numerator.multiply(denominator);
        checkTransient(leftProduct, "subtract left cross-product");
        checkTransient(rightProduct, "subtract right cross-product");
        BigInteger rawNumerator = leftProduct.subtract(rightProduct);
        if (rawNumerator.signum() < 0) {
            throw new IllegalArgumentException("subtraction result must be non-negative");
        }
        BigInteger rawDenominator = denominator.multiply(other.denominator);
        checkTransient(rawNumerator, "subtract raw numerator");
        checkTransient(rawDenominator, "subtract raw denominator");
        return reduceAndCheckPersistent(rawNumerator, rawDenominator);
    }

    /**
     * Returns the mathematically greater operand, retaining operand identity.
     *
     * @param other value to compare
     * @return this value on equality or when greater, otherwise other
     * @throws NullPointerException if other is null
     * @throws NumericLimitException if a comparison cross-product exceeds 8193 bits
     */
    ExactTag max(ExactTag other) throws NumericLimitException {
        return compareExact(other) >= 0 ? this : other;
    }

    /**
     * Compares two values by exact cross multiplication.
     *
     * @param other value to compare
     * @return a negative integer, zero, or a positive integer as this value is
     *         less than, equal to, or greater than other
     * @throws NullPointerException if other is null
     * @throws NumericLimitException if either cross-product exceeds 8193 bits
     */
    int compareExact(ExactTag other) throws NumericLimitException {
        Objects.requireNonNull(other, "other");
        BigInteger leftProduct = numerator.multiply(other.denominator);
        BigInteger rightProduct = other.numerator.multiply(denominator);
        checkTransient(leftProduct, "comparison left cross-product");
        checkTransient(rightProduct, "comparison right cross-product");
        return leftProduct.compareTo(rightProduct);
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof ExactTag tag
                && numerator.equals(tag.numerator)
                && denominator.equals(tag.denominator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numerator, denominator);
    }

    @Override
    public String toString() {
        return numerator + "/" + denominator;
    }

    private static ExactTag reduceAndCheckPersistent(BigInteger numerator, BigInteger denominator)
            throws NumericLimitException {
        if (numerator.signum() == 0) {
            return ZERO;
        }
        BigInteger divisor = numerator.gcd(denominator);
        BigInteger reducedNumerator = numerator.divide(divisor);
        BigInteger reducedDenominator = denominator.divide(divisor);
        checkPersistent(reducedNumerator, "numerator");
        checkPersistent(reducedDenominator, "denominator");
        return new ExactTag(reducedNumerator, reducedDenominator);
    }

    private static void validateSigns(BigInteger numerator, BigInteger denominator) {
        if (numerator.signum() < 0) {
            throw new IllegalArgumentException("numerator must be non-negative");
        }
        if (denominator.signum() <= 0) {
            throw new IllegalArgumentException("denominator must be positive");
        }
    }

    private static void checkPersistent(BigInteger value, String quantity) throws NumericLimitException {
        checkBits(value, quantity, MAX_PERSISTENT_BITS, NumericLimitException.Budget.PERSISTENT);
    }

    private static void checkTransient(BigInteger value, String quantity) throws NumericLimitException {
        checkBits(value, quantity, MAX_TRANSIENT_BITS, NumericLimitException.Budget.TRANSIENT);
    }

    private static void checkBits(
            BigInteger value,
            String quantity,
            int maximumBits,
            NumericLimitException.Budget budget) throws NumericLimitException {
        int actualBits = value.bitLength();
        if (actualBits > maximumBits) {
            throw new NumericLimitException(budget, quantity, actualBits, maximumBits);
        }
    }
}
