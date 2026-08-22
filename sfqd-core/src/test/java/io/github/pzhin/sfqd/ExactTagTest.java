package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

final class ExactTagTest {
    private static final BigInteger TWO = BigInteger.TWO;

    @Test
    void canonicalizesZeroAndReducedComponents() throws NumericLimitException {
        ExactTag zero = ExactTag.fromComponents(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE));
        ExactTag half = ExactTag.fromComponents(TWO, BigInteger.valueOf(4L));

        assertSame(ExactTag.zero(), zero);
        assertEquals(BigInteger.ZERO, zero.numerator());
        assertEquals(BigInteger.ONE, zero.denominator());
        assertEquals(ExactTag.fromCostAndWeight(1L, 2L), half);
        assertEquals(ExactTag.fromCostAndWeight(1L, 2L).hashCode(), half.hashCode());
        assertEquals("1/2", half.toString());
        assertNotEquals(half, ExactTag.zero());
    }

    @Test
    void validatesInputBeforeCreatingAValue() {
        assertThrows(IllegalArgumentException.class, () -> ExactTag.fromCostAndWeight(0L, 1L));
        assertThrows(IllegalArgumentException.class, () -> ExactTag.fromCostAndWeight(1L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> ExactTag.fromComponents(BigInteger.valueOf(-1L), BigInteger.ONE));
        assertThrows(IllegalArgumentException.class,
                () -> ExactTag.fromComponents(BigInteger.ONE, BigInteger.ZERO));
    }

    @Test
    void acceptsEntirePositiveLongCostAndWeightDomain() throws NumericLimitException {
        ExactTag maximum = ExactTag.fromCostAndWeight(Long.MAX_VALUE, Long.MAX_VALUE);
        ExactTag minimumWeight = ExactTag.fromCostAndWeight(Long.MAX_VALUE, 1L);
        ExactTag minimumIncrement = ExactTag.fromCostAndWeight(1L, Long.MAX_VALUE);

        assertEquals(ExactTag.fromCostAndWeight(1L, 1L), maximum);
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), minimumWeight.numerator());
        assertEquals(BigInteger.ONE, minimumWeight.denominator());
        assertEquals(BigInteger.ONE, minimumIncrement.numerator());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), minimumIncrement.denominator());
    }

    @Test
    void preservesIntegerOrderingBeyondDoublePrecision() throws NumericLimitException {
        long twoToFiftyThree = 1L << 53;
        ExactTag first = ExactTag.fromCostAndWeight(twoToFiftyThree, 1L);
        ExactTag next = ExactTag.fromCostAndWeight(twoToFiftyThree + 1L, 1L);

        assertTrue(first.compareExact(next) < 0);
        assertTrue(next.compareExact(first) > 0);
    }

    @Test
    void appliesPersistentBudgetAfterCanonicalReduction() throws NumericLimitException {
        BigInteger largestPower = BigInteger.ONE.shiftLeft(ExactTag.MAX_PERSISTENT_BITS - 1);
        ExactTag directBoundary = ExactTag.fromComponents(largestPower, BigInteger.ONE);
        ExactTag reducedBoundary = ExactTag.fromComponents(largestPower.shiftLeft(1), TWO);

        assertEquals(directBoundary, reducedBoundary);
        NumericLimitException error = assertThrows(NumericLimitException.class,
                () -> ExactTag.fromComponents(largestPower.shiftLeft(2), TWO));
        assertEquals(NumericLimitException.Budget.PERSISTENT, error.budget());
        assertEquals("numerator", error.quantity());

        ExactTag reducedDenominatorBoundary = ExactTag.fromComponents(
                TWO, largestPower.shiftLeft(1));
        assertEquals(BigInteger.ONE, reducedDenominatorBoundary.numerator());
        assertEquals(largestPower, reducedDenominatorBoundary.denominator());
        NumericLimitException denominatorError = assertThrows(NumericLimitException.class,
                () -> ExactTag.fromComponents(TWO, largestPower.shiftLeft(2)));
        assertEquals(NumericLimitException.Budget.PERSISTENT, denominatorError.budget());
        assertEquals("denominator", denominatorError.quantity());
    }

    @Test
    void permitsCanonicalRawAdditionAtThe8193BitBoundary() throws NumericLimitException {
        BigInteger nearLimitNumerator = BigInteger.ONE.shiftLeft(ExactTag.MAX_PERSISTENT_BITS)
                .subtract(BigInteger.ONE);
        BigInteger nearLimitDenominator = nearLimitNumerator.subtract(BigInteger.ONE);
        ExactTag operand = ExactTag.fromComponents(nearLimitNumerator, nearLimitDenominator);
        ExactTag result = operand.add(operand);
        ExactRational expected = ExactRational.of(nearLimitNumerator, nearLimitDenominator)
                .add(ExactRational.of(nearLimitNumerator, nearLimitDenominator));

        assertEquals(ExactTag.MAX_TRANSIENT_BITS,
                nearLimitNumerator.multiply(nearLimitDenominator).shiftLeft(1).bitLength());
        assertEquals(expected.numerator(), result.numerator());
        assertEquals(expected.denominator(), result.denominator());
    }

    @Test
    void rejectsAnOperationWhoseReducedResultExceedsPersistentBudget() throws NumericLimitException {
        ExactTag largestPower = ExactTag.fromComponents(
                BigInteger.ONE.shiftLeft(ExactTag.MAX_PERSISTENT_BITS - 1), BigInteger.ONE);

        NumericLimitException error = assertThrows(NumericLimitException.class,
                () -> largestPower.add(largestPower));

        assertEquals(NumericLimitException.Budget.PERSISTENT, error.budget());
        assertEquals("numerator", error.quantity());
    }

    @Test
    void subtractionIsExactAndRejectsNegativeResults() throws NumericLimitException {
        ExactTag oneThird = ExactTag.fromCostAndWeight(1L, 3L);
        ExactTag oneHalf = ExactTag.fromCostAndWeight(1L, 2L);

        assertEquals(ExactTag.fromCostAndWeight(1L, 6L), oneHalf.subtractNonNegative(oneThird));
        assertSame(ExactTag.zero(), oneHalf.subtractNonNegative(oneHalf));
        assertThrows(IllegalArgumentException.class, () -> oneThird.subtractNonNegative(oneHalf));
    }

    @Test
    void maxReturnsOneOfTheImmutableOperands() throws NumericLimitException {
        ExactTag oneThird = ExactTag.fromCostAndWeight(1L, 3L);
        ExactTag oneHalf = ExactTag.fromCostAndWeight(1L, 2L);

        assertSame(oneHalf, oneThird.max(oneHalf));
        assertSame(oneHalf, oneHalf.max(oneThird));
        assertSame(oneHalf, oneHalf.max(oneHalf));
    }
}
