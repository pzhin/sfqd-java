package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

final class ExactRationalTest {
    @Test
    void reducesSignsAndZeroCanonicallyWithoutFixedWidthLimits() {
        assertEquals(ExactRational.ZERO, ExactRational.of(0L, Long.MAX_VALUE));
        assertEquals(ExactRational.of(1L, 2L), ExactRational.of(2L, 4L));
        BigInteger huge = BigInteger.ONE.shiftLeft(10_000);
        ExactRational rational = ExactRational.of(huge, BigInteger.valueOf(3L));
        assertEquals(10_001, rational.numerator().bitLength());
        assertEquals("1/2", ExactRational.of(1L, 2L).toString());
    }

    @Test
    void rejectsInvalidDomainAndNegativeSubtraction() {
        assertThrows(IllegalArgumentException.class, () -> ExactRational.of(-1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> ExactRational.of(1L, 0L));
        assertThrows(ArithmeticException.class,
                () -> ExactRational.of(1L, 3L).subtract(ExactRational.of(1L, 2L)));
    }
}
