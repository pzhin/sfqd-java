package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

final class ExactRationalProperties {
    @Property(tries = 500)
    void canonicalFormAndOrderingAgreeWithCrossMultiplication(
            @ForAll @IntRange(min = 0, max = 1_000_000) int firstNumerator,
            @ForAll @IntRange(min = 1, max = 10_000) int firstDenominator,
            @ForAll @IntRange(min = 0, max = 1_000_000) int secondNumerator,
            @ForAll @IntRange(min = 1, max = 10_000) int secondDenominator) {
        ExactRational first = ExactRational.of(firstNumerator, firstDenominator);
        ExactRational second = ExactRational.of(secondNumerator, secondDenominator);
        BigInteger expectedSign = BigInteger.valueOf(firstNumerator)
                .multiply(BigInteger.valueOf(secondDenominator))
                .subtract(BigInteger.valueOf(secondNumerator).multiply(BigInteger.valueOf(firstDenominator)));

        assertEquals(Integer.signum(expectedSign.signum()), Integer.signum(first.compareTo(second)));
        assertEquals(BigInteger.ONE, first.numerator().gcd(first.denominator()));
        assertTrue(first.denominator().signum() > 0);
    }

    @Property(tries = 500)
    void additionAndExactSubtractionPreserveMathematicalValue(
            @ForAll @IntRange(min = 0, max = 1_000_000) int firstNumerator,
            @ForAll @IntRange(min = 1, max = 10_000) int firstDenominator,
            @ForAll @IntRange(min = 0, max = 1_000_000) int secondNumerator,
            @ForAll @IntRange(min = 1, max = 10_000) int secondDenominator) {
        ExactRational first = ExactRational.of(firstNumerator, firstDenominator);
        ExactRational second = ExactRational.of(secondNumerator, secondDenominator);

        ExactRational sum = first.add(second);
        assertEquals(first, sum.subtract(second));
        assertEquals(sum, second.add(first));
        assertEquals(first.max(second), second.max(first));
    }
}
